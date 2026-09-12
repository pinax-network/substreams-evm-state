//! Durable local metadata and locks, compatible with existing controller files.
use anyhow::{Context, Result};
use fs2::FileExt;
use serde_json::Value;
use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Component, Path, PathBuf},
};

fn ascii_json(text: String) -> String {
    let mut out = String::with_capacity(text.len());
    for c in text.chars() {
        if c.is_ascii() {
            out.push(c);
        } else {
            for unit in c.encode_utf16(&mut [0; 2]).iter() {
                out.push_str(&format!("\\u{unit:04x}"));
            }
        }
    }
    out
}

/// Python's historical sorted, compact, ensure_ascii JSON for identity hashes.
/// serde_json's default map is sorted recursively; do not enable preserve_order.
pub fn canonical_json(value: &Value) -> Result<String> {
    Ok(ascii_json(serde_json::to_string(value)?))
}

/// Historical sorted JSON with spaces after separators, used by capacity hashes.
pub fn spaced_json(value: &Value) -> Result<String> {
    let compact = canonical_json(value)?;
    let mut out = String::new();
    let mut quoted = false;
    let mut escaped = false;
    for c in compact.chars() {
        out.push(c);
        if quoted {
            if escaped {
                escaped = false;
            } else if c == '\\' {
                escaped = true;
            } else if c == '"' {
                quoted = false;
            }
        } else if c == '"' {
            quoted = true;
        } else if c == ':' || c == ',' {
            out.push(' ');
        }
    }
    Ok(out)
}

pub fn atomic_write(path: &Path, bytes: &[u8], overwrite: bool) -> Result<()> {
    let parent = path.parent().context("metadata path has no parent")?;
    fs::create_dir_all(parent)?;
    let mut file = tempfile::NamedTempFile::new_in(parent)?;
    file.write_all(bytes)?;
    file.flush()?;
    file.as_file().sync_all()?;
    if overwrite {
        fs::rename(file.path(), path)?;
    } else {
        fs::hard_link(file.path(), path)?;
    }
    File::open(parent)?.sync_all()?;
    Ok(())
}

pub fn atomic_json(path: &Path, value: &Value, overwrite: bool) -> Result<()> {
    let text = ascii_json(serde_json::to_string_pretty(value)?) + "\n";
    atomic_write(path, text.as_bytes(), overwrite)
}

pub struct Lock {
    _file: File,
}

impl Drop for Lock {
    fn drop(&mut self) {
        // A concurrent fork briefly copies every open descriptor before exec
        // closes CLOEXEC files. Closing ours alone would leave that unrelated
        // child's copy holding the flock and make immediate reacquisition fail.
        // On normal return explicitly release the shared open-file-description
        // lock. Ingestion keeps this guard until its child has been reaped.
        // SIGKILL skips Drop, so an intentionally inherited native descriptor
        // still protects the source after its wrapper is killed.
        let _ = FileExt::unlock(&self._file);
    }
}

impl Lock {
    /// Keep the same flock open in a native child even if its wrapper is killed.
    pub fn inherit_in(&self, command: &mut std::process::Command) {
        use std::os::{fd::AsRawFd, unix::process::CommandExt};
        let fd = self._file.as_raw_fd();
        // SAFETY: only async-signal-safe fcntl calls run between fork and exec.
        // The caller retains this Lock while spawning and waiting for the child.
        unsafe {
            command.pre_exec(move || {
                let flags = libc::fcntl(fd, libc::F_GETFD);
                if flags == -1 || libc::fcntl(fd, libc::F_SETFD, flags & !libc::FD_CLOEXEC) == -1 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
    }
}

pub fn file_lock(path: &Path, exclusive: bool, blocking: bool) -> Result<Lock> {
    fs::create_dir_all(path.parent().context("lock path has no parent")?)?;
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)?;
    match (exclusive, blocking) {
        (true, true) => FileExt::lock_exclusive(&file),
        (false, true) => FileExt::lock_shared(&file),
        (true, false) => FileExt::try_lock_exclusive(&file),
        (false, false) => FileExt::try_lock_shared(&file),
    }
    .context("another process owns this state directory or lock acquisition failed")?;
    Ok(Lock { _file: file })
}

/// Resolve existing symlink components and normalize a not-yet-created suffix.
pub fn resolve(path: &Path) -> Result<PathBuf> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    };
    resolve_inner(&absolute, &mut 0)
}

fn resolve_inner(absolute: &Path, links: &mut usize) -> Result<PathBuf> {
    let mut resolved = PathBuf::new();
    for component in absolute.components() {
        match component {
            Component::ParentDir => {
                resolved.pop();
            }
            Component::CurDir => {}
            other => {
                resolved.push(other);
                match resolved.symlink_metadata() {
                    Ok(metadata) if metadata.is_symlink() => {
                        *links += 1;
                        anyhow::ensure!(*links <= 40, "too many symbolic links in state directory");
                        let target = fs::read_link(&resolved)?;
                        let target = if target.is_absolute() {
                            target
                        } else {
                            resolved
                                .parent()
                                .context("symlink has no parent")?
                                .join(target)
                        };
                        resolved = resolve_inner(&target, links)?;
                    }
                    Ok(_) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => return Err(error.into()),
                }
            }
        }
    }
    Ok(resolved)
}
