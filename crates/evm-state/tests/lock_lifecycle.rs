use anyhow::Result;
use evm_state::{files, process};
use std::{
    fs,
    process::Command,
    time::{Duration, Instant},
};

#[test]
fn normal_return_releases_lock_despite_an_unrelated_fork_in_progress() -> Result<()> {
    let root = tempfile::tempdir()?;
    let path = root.path().join("run.lock");
    let lock = files::file_lock(&path, true, false)?;
    let mut pipe = [-1; 2];
    // SAFETY: the forked child only invokes async-signal-safe read, close and
    // _exit. It models another thread between fork and exec, where CLOEXEC has
    // not closed the accidentally copied lock descriptor yet.
    unsafe {
        assert_eq!(libc::pipe(pipe.as_mut_ptr()), 0);
        let pid = libc::fork();
        assert!(pid >= 0);
        if pid == 0 {
            libc::close(pipe[1]);
            let mut byte = 0_u8;
            libc::read(pipe[0], (&mut byte as *mut u8).cast(), 1);
            libc::_exit(0);
        }
        libc::close(pipe[0]);
        drop(lock);
        let reacquired = files::file_lock(&path, true, false);
        libc::close(pipe[1]);
        let mut status = 0;
        assert_eq!(libc::waitpid(pid, &mut status, 0), pid);
        reacquired?;
    }
    Ok(())
}

fn child_command(root: &std::path::Path, mode: &str) -> Result<Command> {
    let mut command = Command::new(std::env::current_exe()?);
    command
        .args(["--exact", "lock_child_fixture", "--ignored", "--nocapture"])
        .env("EVM_LOCK_TEST_ROOT", root)
        .env("EVM_LOCK_TEST_MODE", mode);
    Ok(command)
}
#[test]
#[ignore = "subprocess fixture only"]
fn lock_child_fixture() -> Result<()> {
    let root = std::path::PathBuf::from(std::env::var("EVM_LOCK_TEST_ROOT")?);
    if std::env::var("EVM_LOCK_TEST_MODE")? == "wrapper" {
        let lock = files::file_lock(&root.join("run.lock"), true, false)?;
        let mut command = child_command(&root, "native")?;
        lock.inherit_in(&mut command);
        let mut child = process::ChildGuard(command.spawn()?);
        child.wait()?;
    } else {
        fs::write(root.join("native-ready"), b"ready")?;
        let deadline = Instant::now() + Duration::from_secs(10);
        while !root.join("native-stop").exists() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
    }
    Ok(())
}

#[test]
fn killed_wrapper_keeps_native_child_lock_until_the_child_exits() -> Result<()> {
    let root = tempfile::tempdir()?;
    let mut wrapper = process::ChildGuard(child_command(root.path(), "wrapper")?.spawn()?);
    let deadline = Instant::now() + Duration::from_secs(10);
    while !root.path().join("native-ready").exists() {
        anyhow::ensure!(Instant::now() < deadline, "native child did not start");
        anyhow::ensure!(wrapper.try_wait()?.is_none(), "wrapper exited early");
        std::thread::sleep(Duration::from_millis(10));
    }
    wrapper.kill()?;
    wrapper.wait()?;
    assert!(files::file_lock(&root.path().join("run.lock"), true, false).is_err());
    fs::write(root.path().join("native-stop"), b"stop")?;
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match files::file_lock(&root.path().join("run.lock"), true, false) {
            Ok(_) => break,
            Err(e) if Instant::now() >= deadline => return Err(e),
            Err(_) => std::thread::sleep(Duration::from_millis(10)),
        }
    }
    Ok(())
}
