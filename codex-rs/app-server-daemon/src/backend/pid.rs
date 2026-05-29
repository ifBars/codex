use std::io::SeekFrom;
use std::path::Path;
use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

use anyhow::Context;
use anyhow::Result;
use anyhow::bail;
use serde::Deserialize;
use serde::Serialize;
use tokio::fs;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncSeekExt;
use tokio::process::Command;
use tokio::time::sleep;

const STOP_POLL_INTERVAL: Duration = Duration::from_millis(50);
const STOP_GRACE_PERIOD: Duration = Duration::from_secs(60);
const STOP_TIMEOUT: Duration = Duration::from_secs(70);
const START_TIMEOUT: Duration = Duration::from_secs(10);
const STDERR_LOG_TAIL_BYTES: u64 = 4096;

#[derive(Debug)]
pub(crate) struct PidBackend {
    codex_bin: PathBuf,
    pid_file: PathBuf,
    lock_file: PathBuf,
    command_kind: PidCommandKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PidRecord {
    pid: u32,
    process_start_time: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PidLogTail {
    pub(crate) path: PathBuf,
    pub(crate) contents: String,
}

impl PidLogTail {
    pub(crate) fn append_to_context(&self, context: &mut String) {
        context.push_str(&format!(
            "\n\nManaged app-server stderr ({}):",
            self.path.display()
        ));
        for line in self.contents.lines() {
            context.push_str("\n  ");
            context.push_str(line);
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum PidFileState {
    Missing,
    Starting,
    Running(PidRecord),
}

#[derive(Debug, Clone, Copy)]
enum PidCommandKind {
    AppServer { remote_control_enabled: bool },
    UpdateLoop,
}

impl PidBackend {
    pub(crate) fn new(codex_bin: PathBuf, pid_file: PathBuf, remote_control_enabled: bool) -> Self {
        let lock_file = pid_file.with_extension("pid.lock");
        Self {
            codex_bin,
            pid_file,
            lock_file,
            command_kind: PidCommandKind::AppServer {
                remote_control_enabled,
            },
        }
    }

    pub(crate) fn new_update_loop(codex_bin: PathBuf, pid_file: PathBuf) -> Self {
        let lock_file = pid_file.with_extension("pid.lock");
        Self {
            codex_bin,
            pid_file,
            lock_file,
            command_kind: PidCommandKind::UpdateLoop,
        }
    }

    pub(crate) async fn is_starting_or_running(&self) -> Result<bool> {
        loop {
            match self.read_pid_file_state().await? {
                PidFileState::Missing => return Ok(false),
                PidFileState::Starting => return Ok(true),
                PidFileState::Running(record) => {
                    if self.record_is_active(&record).await? {
                        return Ok(true);
                    }
                    match self.refresh_after_stale_record(&record).await? {
                        PidFileState::Missing => return Ok(false),
                        PidFileState::Starting | PidFileState::Running(_) => continue,
                    }
                }
            }
        }
    }

    pub(crate) async fn start(&self) -> Result<Option<u32>> {
        if let Some(parent) = self.pid_file.parent() {
            fs::create_dir_all(parent)
                .await
                .with_context(|| format!("failed to create pid directory {}", parent.display()))?;
        }
        let reservation_lock = self.acquire_reservation_lock().await?;
        let _pid_file = loop {
            match fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&self.pid_file)
                .await
            {
                Ok(pid_file) => break pid_file,
                Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
                    match self.read_pid_file_state_with_lock_held().await? {
                        PidFileState::Missing => continue,
                        PidFileState::Running(record) => {
                            if self.record_is_active(&record).await? {
                                return Ok(None);
                            }
                            let _ = fs::remove_file(&self.pid_file).await;
                            continue;
                        }
                        PidFileState::Starting => {
                            unreachable!("lock holder cannot observe starting")
                        }
                    }
                }
                Err(err) => {
                    return Err(err).with_context(|| {
                        format!("failed to reserve pid file {}", self.pid_file.display())
                    });
                }
            }
        };
        let mut command = Command::new(&self.codex_bin);
        let stderr_log = match self.open_stderr_log().await {
            Ok(stderr_log) => stderr_log,
            Err(err) => {
                let _ = fs::remove_file(&self.pid_file).await;
                return Err(err);
            }
        };
        command
            .args(self.command_args())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::from(stderr_log.into_std().await));

        configure_detached_command(&mut command);

        let child = match command.spawn() {
            Ok(child) => child,
            Err(err) => {
                let _ = fs::remove_file(&self.pid_file).await;
                return Err(err).with_context(|| {
                    format!(
                        "failed to spawn detached app-server process using {}",
                        self.codex_bin.display()
                    )
                });
            }
        };
        let pid = child
            .id()
            .context("spawned app-server process has no pid")?;
        let record = match read_process_start_time(pid).await {
            Ok(process_start_time) => PidRecord {
                pid,
                process_start_time,
            },
            Err(err) => {
                let _ = self.terminate_process(pid);
                let mut context =
                    format!("failed to record pid-managed app-server process {pid} startup");
                super::append_stderr_log_tail_context(&self.pid_file, &mut context).await;
                let _ = fs::remove_file(&self.pid_file).await;
                return Err(err).context(context);
            }
        };
        let contents = serde_json::to_vec(&record).context("failed to serialize pid record")?;
        let temp_pid_file = self.pid_file.with_extension("pid.tmp");
        if let Err(err) = fs::write(&temp_pid_file, &contents).await {
            let _ = self.terminate_process(pid);
            let _ = fs::remove_file(&self.pid_file).await;
            return Err(err).with_context(|| {
                format!("failed to write pid temp file {}", temp_pid_file.display())
            });
        }
        if let Err(err) = fs::rename(&temp_pid_file, &self.pid_file).await {
            let _ = self.terminate_process(pid);
            let _ = fs::remove_file(&temp_pid_file).await;
            let _ = fs::remove_file(&self.pid_file).await;
            return Err(err).with_context(|| {
                format!("failed to publish pid file {}", self.pid_file.display())
            });
        }
        drop(reservation_lock);
        Ok(Some(pid))
    }

    pub(crate) async fn stop(&self) -> Result<()> {
        loop {
            let Some(record) = self.wait_for_pid_start().await? else {
                return Ok(());
            };
            if !self.record_is_active(&record).await? {
                match self.refresh_after_stale_record(&record).await? {
                    PidFileState::Missing => return Ok(()),
                    PidFileState::Starting | PidFileState::Running(_) => continue,
                }
            }

            let pid = record.pid;
            self.terminate_process(pid)?;
            let started_at = tokio::time::Instant::now();
            let deadline = tokio::time::Instant::now() + STOP_TIMEOUT;
            let mut forced = false;
            while tokio::time::Instant::now() < deadline {
                if !self.record_is_active(&record).await? {
                    match self.refresh_after_stale_record(&record).await? {
                        PidFileState::Missing => return Ok(()),
                        PidFileState::Starting | PidFileState::Running(_) => break,
                    }
                }
                if !forced && started_at.elapsed() >= STOP_GRACE_PERIOD {
                    self.force_terminate_process(pid)?;
                    forced = true;
                }
                sleep(STOP_POLL_INTERVAL).await;
            }

            if self.record_is_active(&record).await? {
                bail!("timed out waiting for pid-managed app server {pid} to stop");
            }
        }
    }

    async fn wait_for_pid_start(&self) -> Result<Option<PidRecord>> {
        let deadline = tokio::time::Instant::now() + START_TIMEOUT;
        loop {
            match self.read_pid_file_state().await? {
                PidFileState::Missing => return Ok(None),
                PidFileState::Running(record) => return Ok(Some(record)),
                PidFileState::Starting if tokio::time::Instant::now() < deadline => {
                    sleep(STOP_POLL_INTERVAL).await;
                }
                PidFileState::Starting => {
                    bail!(
                        "timed out waiting for pid reservation in {} to finish initializing",
                        self.pid_file.display()
                    );
                }
            }
        }
    }

    async fn read_pid_file_state(&self) -> Result<PidFileState> {
        let contents = match fs::read_to_string(&self.pid_file).await {
            Ok(contents) => contents,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                return if reservation_lock_is_active(&self.lock_file).await? {
                    Ok(PidFileState::Starting)
                } else {
                    Ok(PidFileState::Missing)
                };
            }
            Err(err) => {
                return Err(err).with_context(|| {
                    format!("failed to read pid file {}", self.pid_file.display())
                });
            }
        };
        if contents.trim().is_empty() {
            match inspect_empty_pid_reservation(&self.pid_file, &self.lock_file).await? {
                EmptyPidReservation::Active => {
                    return Ok(PidFileState::Starting);
                }
                EmptyPidReservation::Stale => {
                    return Ok(PidFileState::Missing);
                }
                EmptyPidReservation::Record(record) => return Ok(PidFileState::Running(record)),
            }
        }
        let record = serde_json::from_str(&contents)
            .with_context(|| format!("invalid pid file contents in {}", self.pid_file.display()))?;
        Ok(PidFileState::Running(record))
    }

    async fn read_pid_file_state_with_lock_held(&self) -> Result<PidFileState> {
        let contents = match fs::read_to_string(&self.pid_file).await {
            Ok(contents) => contents,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                return Ok(PidFileState::Missing);
            }
            Err(err) => {
                return Err(err).with_context(|| {
                    format!("failed to read pid file {}", self.pid_file.display())
                });
            }
        };
        if contents.trim().is_empty() {
            let _ = fs::remove_file(&self.pid_file).await;
            return Ok(PidFileState::Missing);
        }
        let record = serde_json::from_str(&contents)
            .with_context(|| format!("invalid pid file contents in {}", self.pid_file.display()))?;
        Ok(PidFileState::Running(record))
    }

    async fn refresh_after_stale_record(&self, expected: &PidRecord) -> Result<PidFileState> {
        let reservation_lock = self.acquire_reservation_lock().await?;
        let state = match self.read_pid_file_state_with_lock_held().await? {
            PidFileState::Running(record) if record == *expected => {
                let _ = fs::remove_file(&self.pid_file).await;
                PidFileState::Missing
            }
            state => state,
        };
        drop(reservation_lock);
        Ok(state)
    }

    async fn acquire_reservation_lock(&self) -> Result<fs::File> {
        let reservation_lock = fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&self.lock_file)
            .await
            .with_context(|| {
                format!("failed to open pid lock file {}", self.lock_file.display())
            })?;
        let lock_deadline = tokio::time::Instant::now() + START_TIMEOUT;
        while !try_lock_file(&reservation_lock)? {
            if tokio::time::Instant::now() >= lock_deadline {
                bail!(
                    "timed out waiting for pid lock {}",
                    self.lock_file.display()
                );
            }
            sleep(STOP_POLL_INTERVAL).await;
        }
        Ok(reservation_lock)
    }

    async fn open_stderr_log(&self) -> Result<fs::File> {
        let stderr_log_file = stderr_log_file_for_pid_file(&self.pid_file);
        fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&stderr_log_file)
            .await
            .with_context(|| {
                format!(
                    "failed to open stderr log for pid-managed app server {}",
                    stderr_log_file.display()
                )
            })
    }

    fn command_args(&self) -> Vec<&'static str> {
        match self.command_kind {
            PidCommandKind::AppServer {
                remote_control_enabled: true,
            } => vec!["app-server", "--remote-control", "--listen", "unix://"],
            PidCommandKind::AppServer {
                remote_control_enabled: false,
            } => vec!["app-server", "--listen", "unix://"],
            PidCommandKind::UpdateLoop => vec!["app-server", "daemon", "pid-update-loop"],
        }
    }

    fn terminate_process(&self, pid: u32) -> Result<()> {
        match self.command_kind {
            PidCommandKind::AppServer { .. } => terminate_process(pid),
            PidCommandKind::UpdateLoop => terminate_process(pid),
        }
    }

    fn force_terminate_process(&self, pid: u32) -> Result<()> {
        match self.command_kind {
            PidCommandKind::AppServer { .. } => force_terminate_process(pid),
            PidCommandKind::UpdateLoop => force_terminate_process_group(pid),
        }
    }

    async fn record_is_active(&self, record: &PidRecord) -> Result<bool> {
        process_matches_record(record).await
    }
}

#[cfg(unix)]
fn configure_detached_command(command: &mut Command) {
    use std::os::unix::process::CommandExt;

    unsafe {
        command.pre_exec(|| {
            if libc::setsid() == -1 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
}

#[cfg(windows)]
fn configure_detached_command(command: &mut Command) {
    const CREATE_NO_WINDOW: u32 = 0x0800_0000;
    const DETACHED_PROCESS: u32 = 0x0000_0008;

    command.creation_flags(CREATE_NO_WINDOW | DETACHED_PROCESS);
}

pub(crate) async fn read_stderr_log_tail(pid_file: &Path) -> Result<Option<PidLogTail>> {
    let path = stderr_log_file_for_pid_file(pid_file);
    let Some(contents) = read_log_tail(&path, STDERR_LOG_TAIL_BYTES).await? else {
        return Ok(None);
    };
    Ok(Some(PidLogTail { path, contents }))
}

fn stderr_log_file_for_pid_file(pid_file: &Path) -> PathBuf {
    pid_file.with_extension("stderr.log")
}

async fn read_log_tail(path: &Path, byte_limit: u64) -> Result<Option<String>> {
    let mut file = match fs::File::open(path).await {
        Ok(file) => file,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => {
            return Err(err)
                .with_context(|| format!("failed to open stderr log {}", path.display()));
        }
    };
    let len = file
        .metadata()
        .await
        .with_context(|| format!("failed to inspect stderr log {}", path.display()))?
        .len();
    if len == 0 {
        return Ok(None);
    }

    let start = len.saturating_sub(byte_limit);
    file.seek(SeekFrom::Start(start))
        .await
        .with_context(|| format!("failed to seek stderr log {}", path.display()))?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)
        .await
        .with_context(|| format!("failed to read stderr log {}", path.display()))?;
    if start > 0
        && let Some(newline_index) = bytes.iter().position(|byte| *byte == b'\n')
    {
        bytes.drain(..=newline_index);
    }
    let contents = String::from_utf8_lossy(&bytes).trim_end().to_string();
    if contents.is_empty() {
        return Ok(None);
    }
    Ok(Some(contents))
}

#[cfg(unix)]
fn process_exists(pid: u32) -> bool {
    let Ok(pid) = libc::pid_t::try_from(pid) else {
        return false;
    };
    let result = unsafe { libc::kill(pid, 0) };
    result == 0 || std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}

#[cfg(unix)]
fn terminate_process(pid: u32) -> Result<()> {
    let raw_pid = libc::pid_t::try_from(pid)
        .with_context(|| format!("pid-managed app server pid {pid} is out of range"))?;
    let result = unsafe { libc::kill(raw_pid, libc::SIGTERM) };
    if result == 0 {
        return Ok(());
    }
    let err = std::io::Error::last_os_error();
    if err.raw_os_error() == Some(libc::ESRCH) {
        return Ok(());
    }
    Err(err).with_context(|| format!("failed to terminate pid-managed app server {pid}"))
}

#[cfg(unix)]
fn force_terminate_process(pid: u32) -> Result<()> {
    let raw_pid = libc::pid_t::try_from(pid)
        .with_context(|| format!("pid-managed app server pid {pid} is out of range"))?;
    let result = unsafe { libc::kill(raw_pid, libc::SIGKILL) };
    if result == 0 {
        return Ok(());
    }
    let err = std::io::Error::last_os_error();
    if err.raw_os_error() == Some(libc::ESRCH) {
        return Ok(());
    }
    Err(err).with_context(|| format!("failed to force terminate pid-managed app server {pid}"))
}

#[cfg(unix)]
fn force_terminate_process_group(pid: u32) -> Result<()> {
    let raw_pid = libc::pid_t::try_from(pid)
        .with_context(|| format!("pid-managed updater pid {pid} is out of range"))?;
    let result = unsafe { libc::kill(-raw_pid, libc::SIGKILL) };
    if result == 0 {
        return Ok(());
    }
    let err = std::io::Error::last_os_error();
    if err.raw_os_error() == Some(libc::ESRCH) {
        return Ok(());
    }
    Err(err).with_context(|| format!("failed to force terminate pid-managed updater group {pid}"))
}

#[cfg(unix)]
async fn process_matches_record(record: &PidRecord) -> Result<bool> {
    if !process_exists(record.pid) {
        return Ok(false);
    }

    match read_process_start_time(record.pid).await {
        Ok(start_time) => Ok(start_time == record.process_start_time),
        Err(_err) if !process_exists(record.pid) => Ok(false),
        Err(err) => Err(err),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum EmptyPidReservation {
    Active,
    Stale,
    Record(PidRecord),
}

#[cfg(unix)]
fn try_lock_file(file: &fs::File) -> Result<bool> {
    use std::os::fd::AsRawFd;

    let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if result == 0 {
        return Ok(true);
    }

    let err = std::io::Error::last_os_error();
    if err.raw_os_error() == Some(libc::EWOULDBLOCK) {
        return Ok(false);
    }
    Err(err).context("failed to lock pid reservation")
}

#[cfg(unix)]
async fn reservation_lock_is_active(path: &Path) -> Result<bool> {
    let file = match fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)
        .await
    {
        Ok(file) => file,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return Ok(false);
        }
        Err(err) => {
            return Err(err)
                .with_context(|| format!("failed to inspect pid lock file {}", path.display()));
        }
    };
    Ok(!try_lock_file(&file)?)
}

#[cfg(unix)]
async fn inspect_empty_pid_reservation(
    pid_path: &Path,
    lock_path: &Path,
) -> Result<EmptyPidReservation> {
    let file = match fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(false)
        .open(lock_path)
        .await
    {
        Ok(file) => file,
        Err(err) => {
            return Err(err).with_context(|| {
                format!("failed to inspect pid lock file {}", lock_path.display())
            });
        }
    };
    if !try_lock_file(&file)? {
        return Ok(EmptyPidReservation::Active);
    }

    let contents = match fs::read_to_string(pid_path).await {
        Ok(contents) => contents,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return Ok(EmptyPidReservation::Stale);
        }
        Err(err) => {
            return Err(err)
                .with_context(|| format!("failed to reread pid file {}", pid_path.display()));
        }
    };
    if contents.trim().is_empty() {
        let _ = fs::remove_file(pid_path).await;
        return Ok(EmptyPidReservation::Stale);
    }

    let record = serde_json::from_str(&contents)
        .with_context(|| format!("invalid pid file contents in {}", pid_path.display()))?;
    Ok(EmptyPidReservation::Record(record))
}

#[cfg(unix)]
async fn read_process_start_time(pid: u32) -> Result<String> {
    let output = Command::new("ps")
        .args(["-p", &pid.to_string(), "-o", "lstart="])
        .output()
        .await
        .context("failed to invoke ps for pid-managed app server")?;
    if !output.status.success() {
        bail!("failed to read start time for pid-managed app server {pid}");
    }

    let start_time = String::from_utf8(output.stdout)
        .context("pid-managed app server start time was not utf-8")?;
    let start_time = start_time.trim();
    if start_time.is_empty() {
        bail!("pid-managed app server {pid} has no recorded start time");
    }
    Ok(start_time.to_string())
}

#[cfg(windows)]
mod windows_process {
    use anyhow::Context;
    use anyhow::Result;
    use anyhow::bail;
    use windows_sys::Win32::Foundation::CloseHandle;
    use windows_sys::Win32::Foundation::FALSE;
    use windows_sys::Win32::Foundation::FILETIME;
    use windows_sys::Win32::Foundation::GetLastError;
    use windows_sys::Win32::Foundation::HANDLE;
    use windows_sys::Win32::Foundation::WAIT_OBJECT_0;
    use windows_sys::Win32::Foundation::WAIT_TIMEOUT;
    use windows_sys::Win32::System::Threading::GetProcessTimes;
    use windows_sys::Win32::System::Threading::OpenProcess;
    use windows_sys::Win32::System::Threading::PROCESS_QUERY_LIMITED_INFORMATION;
    use windows_sys::Win32::System::Threading::PROCESS_TERMINATE;
    use windows_sys::Win32::System::Threading::TerminateProcess;
    use windows_sys::Win32::System::Threading::WaitForSingleObject;

    use super::PidRecord;

    const WAIT_FAILED: u32 = 0xffff_ffff;

    pub(super) async fn process_matches_record(record: &PidRecord) -> Result<bool> {
        let Some(handle) = ProcessHandle::open(record.pid, PROCESS_QUERY_LIMITED_INFORMATION)
        else {
            return Ok(false);
        };
        if !handle.is_running()? {
            return Ok(false);
        }
        Ok(read_process_start_time_from_handle(handle.raw())? == record.process_start_time)
    }

    pub(super) fn terminate_process(pid: u32) -> Result<()> {
        let Some(handle) = ProcessHandle::open(pid, PROCESS_TERMINATE) else {
            return Ok(());
        };
        let result = unsafe { TerminateProcess(handle.raw(), 0) };
        if result == 0 {
            bail!(
                "failed to terminate pid-managed app server {pid}: {}",
                unsafe { GetLastError() }
            );
        }
        Ok(())
    }

    pub(super) fn force_terminate_process(pid: u32) -> Result<()> {
        terminate_process(pid)
    }

    pub(super) async fn read_process_start_time(pid: u32) -> Result<String> {
        let Some(handle) = ProcessHandle::open(pid, PROCESS_QUERY_LIMITED_INFORMATION) else {
            bail!("pid-managed app server {pid} is not running");
        };
        read_process_start_time_from_handle(handle.raw())
    }

    fn read_process_start_time_from_handle(handle: HANDLE) -> Result<String> {
        let mut creation_time = zero_filetime();
        let mut exit_time = zero_filetime();
        let mut kernel_time = zero_filetime();
        let mut user_time = zero_filetime();
        let result = unsafe {
            GetProcessTimes(
                handle,
                &mut creation_time,
                &mut exit_time,
                &mut kernel_time,
                &mut user_time,
            )
        };
        if result == 0 {
            return Err(std::io::Error::last_os_error())
                .context("failed to read start time for pid-managed app server");
        }
        Ok(format!(
            "{:08x}{:08x}",
            creation_time.dwHighDateTime, creation_time.dwLowDateTime
        ))
    }

    struct ProcessHandle(HANDLE);

    impl ProcessHandle {
        fn open(pid: u32, access: u32) -> Option<Self> {
            let handle = unsafe { OpenProcess(access, FALSE, pid) };
            if handle == 0 {
                None
            } else {
                Some(Self(handle))
            }
        }

        fn raw(&self) -> HANDLE {
            self.0
        }

        fn is_running(&self) -> Result<bool> {
            match unsafe { WaitForSingleObject(self.0, 0) } {
                WAIT_TIMEOUT => Ok(true),
                WAIT_OBJECT_0 => Ok(false),
                WAIT_FAILED => Err(std::io::Error::last_os_error())
                    .context("failed to query pid-managed app server liveness"),
                status => bail!("unexpected process wait status {status}"),
            }
        }
    }

    impl Drop for ProcessHandle {
        fn drop(&mut self) {
            unsafe {
                let _ = CloseHandle(self.0);
            }
        }
    }

    fn zero_filetime() -> FILETIME {
        FILETIME {
            dwLowDateTime: 0,
            dwHighDateTime: 0,
        }
    }
}

#[cfg(windows)]
fn terminate_process(pid: u32) -> Result<()> {
    windows_process::terminate_process(pid)
}

#[cfg(windows)]
fn force_terminate_process(pid: u32) -> Result<()> {
    windows_process::force_terminate_process(pid)
}

#[cfg(windows)]
fn force_terminate_process_group(pid: u32) -> Result<()> {
    windows_process::force_terminate_process(pid)
}

#[cfg(windows)]
async fn process_matches_record(record: &PidRecord) -> Result<bool> {
    windows_process::process_matches_record(record).await
}

#[cfg(windows)]
fn try_lock_file(file: &fs::File) -> Result<bool> {
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Foundation::ERROR_LOCK_VIOLATION;
    use windows_sys::Win32::Foundation::GetLastError;
    use windows_sys::Win32::Storage::FileSystem::LOCKFILE_EXCLUSIVE_LOCK;
    use windows_sys::Win32::Storage::FileSystem::LOCKFILE_FAIL_IMMEDIATELY;
    use windows_sys::Win32::Storage::FileSystem::LockFileEx;
    use windows_sys::Win32::System::IO::OVERLAPPED;

    let mut overlapped = unsafe { std::mem::zeroed::<OVERLAPPED>() };
    let result = unsafe {
        LockFileEx(
            file.as_raw_handle() as isize,
            LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY,
            0,
            1,
            0,
            &mut overlapped,
        )
    };
    if result != 0 {
        return Ok(true);
    }

    match unsafe { GetLastError() } {
        ERROR_LOCK_VIOLATION => Ok(false),
        _ => Err(std::io::Error::last_os_error()).context("failed to lock pid reservation"),
    }
}

#[cfg(windows)]
async fn reservation_lock_is_active(path: &Path) -> Result<bool> {
    let file = match fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)
        .await
    {
        Ok(file) => file,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return Ok(false);
        }
        Err(err) => {
            return Err(err)
                .with_context(|| format!("failed to inspect pid lock file {}", path.display()));
        }
    };
    Ok(!try_lock_file(&file)?)
}

#[cfg(windows)]
async fn inspect_empty_pid_reservation(
    pid_path: &Path,
    lock_path: &Path,
) -> Result<EmptyPidReservation> {
    let file = fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(false)
        .open(lock_path)
        .await
        .with_context(|| format!("failed to inspect pid lock file {}", lock_path.display()))?;
    if !try_lock_file(&file)? {
        return Ok(EmptyPidReservation::Active);
    }

    let contents = match fs::read_to_string(pid_path).await {
        Ok(contents) => contents,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return Ok(EmptyPidReservation::Stale);
        }
        Err(err) => {
            return Err(err)
                .with_context(|| format!("failed to reread pid file {}", pid_path.display()));
        }
    };
    if contents.trim().is_empty() {
        let _ = fs::remove_file(pid_path).await;
        return Ok(EmptyPidReservation::Stale);
    }

    let record = serde_json::from_str(&contents)
        .with_context(|| format!("invalid pid file contents in {}", pid_path.display()))?;
    Ok(EmptyPidReservation::Record(record))
}

#[cfg(windows)]
async fn read_process_start_time(pid: u32) -> Result<String> {
    windows_process::read_process_start_time(pid).await
}

#[cfg(all(test, any(unix, windows)))]
#[path = "pid_tests.rs"]
mod tests;
