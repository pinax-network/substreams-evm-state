//! Bounded subprocess capture and explicit ownership of supervised process groups.
use anyhow::{ensure, Context, Result};
use std::{
    io::Read,
    os::unix::process::{CommandExt, ExitStatusExt},
    process::{Child, Command, ExitStatus, Stdio},
    thread,
    time::Duration,
};
use wait_timeout::ChildExt;

pub struct Output {
    pub status: ExitStatus,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
}

/// A native child must be reaped on every error path before its wrapper returns.
pub struct ChildGuard(pub Child);
impl std::ops::Deref for ChildGuard {
    type Target = Child;
    fn deref(&self) -> &Child {
        &self.0
    }
}
impl std::ops::DerefMut for ChildGuard {
    fn deref_mut(&mut self) -> &mut Child {
        &mut self.0
    }
}
impl Drop for ChildGuard {
    fn drop(&mut self) {
        if !matches!(self.0.try_wait(), Ok(Some(_))) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
}

pub fn capture(command: &mut Command, timeout: Duration) -> Result<Option<Output>> {
    const LIMIT: u64 = 16 * 1024 * 1024;
    let mut child = ChildGuard(
        command
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?,
    );
    let read = |pipe: Box<dyn Read + Send>| {
        thread::spawn(move || -> Result<Vec<u8>> {
            let mut bytes = Vec::new();
            pipe.take(LIMIT + 1).read_to_end(&mut bytes)?;
            ensure!(
                bytes.len() <= LIMIT as usize,
                "subprocess output exceeds capture limit"
            );
            Ok(bytes)
        })
    };
    let stdout = read(Box::new(child.stdout.take().unwrap()));
    let stderr = read(Box::new(child.stderr.take().unwrap()));
    let status = child.wait_timeout(timeout)?;
    if status.is_none() {
        let _ = child.kill();
        child.wait()?;
    }
    let stdout = stdout
        .join()
        .map_err(|_| anyhow::anyhow!("subprocess stdout reader failed"))??;
    let stderr = stderr
        .join()
        .map_err(|_| anyhow::anyhow!("subprocess stderr reader failed"))??;
    Ok(status.map(|status| Output {
        status,
        stdout,
        stderr,
    }))
}

pub fn exit_code(status: ExitStatus) -> i32 {
    status
        .code()
        .unwrap_or_else(|| -status.signal().unwrap_or(1))
}

pub struct OwnedGroup {
    pub child: Child,
    group: i32,
    armed: bool,
}
impl OwnedGroup {
    pub fn spawn(command: &mut Command) -> Result<Self> {
        let child = command.process_group(0).spawn()?;
        let group = child
            .id()
            .try_into()
            .context("invalid child process group")?;
        Ok(Self {
            child,
            group,
            armed: true,
        })
    }
    fn signal(&self, signal: i32) -> Result<()> {
        // SAFETY: group is a positive PID created by this instance; a negative
        // argument targets that owned group, never the caller's group.
        if unsafe { libc::kill(-self.group, signal) } == 0 {
            return Ok(());
        }
        let error = std::io::Error::last_os_error();
        if error.raw_os_error() == Some(libc::ESRCH) {
            return Ok(());
        }
        Err(error).context("could_not_confirm_process_group_termination")
    }
    pub fn terminate(&mut self) -> Result<ExitStatus> {
        let term = self.signal(libc::SIGTERM);
        let _ = self.child.wait_timeout(Duration::from_secs(15));
        // Reaping the wrapper does not show that its children have stopped.
        let kill = self.signal(libc::SIGKILL);
        let status = self
            .child
            .wait_timeout(Duration::from_secs(10))?
            .context("child did not exit after SIGKILL")?;
        self.armed = false;
        term?;
        kill?;
        Ok(status)
    }
    pub fn complete(&mut self) {
        self.armed = false;
    }
}
impl Drop for OwnedGroup {
    fn drop(&mut self) {
        if self.armed {
            let _ = self.signal(libc::SIGKILL);
            let _ = self.child.wait();
        }
    }
}
