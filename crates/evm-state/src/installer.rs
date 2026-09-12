//! Install the pinned upstream native CLI only after archive checksum validation.
use anyhow::{ensure, Context, Result};
use flate2::read::MultiGzDecoder;
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File},
    io::{Read, Write},
    os::unix::fs::PermissionsExt,
    path::Path,
    time::Duration,
};

pub const VERSION: &str = "1.22.0";
pub fn checksum(target: &str) -> Result<&'static str> {
    Ok(match target {
        "darwin_arm64" => "80ec00a9a89d18402420f8ae9f79575ab4696107c38169e2e81408d51e346d17",
        "darwin_x86_64" => "2b91ff37978c7f0179ade469d0f4cad5cc2454580ed480255793f27bbad2fdcf",
        "linux_arm64" => "385ac239bf792e09936f19e08ac419c4bab64d79415c06a423f29ed8fd4c278b",
        "linux_x86_64" => "6eef6182d8d9e3c0147a3d03f38fc2b9c0f96900d040132294a0b540e691ae67",
        _ => anyhow::bail!("supported platforms are Linux/WSL or macOS on ARM64 or x86-64"),
    })
}
fn platform() -> Result<String> {
    let system = match std::env::consts::OS {
        "macos" => "darwin",
        "linux" => "linux",
        _ => anyhow::bail!("unsupported operating system"),
    };
    let arch = match std::env::consts::ARCH {
        "aarch64" => "arm64",
        "x86_64" => "x86_64",
        _ => anyhow::bail!("unsupported CPU architecture"),
    };
    Ok(format!("{system}_{arch}"))
}

struct Limited<R> {
    inner: R,
    remaining: u64,
}
impl<R: Read> Read for Limited<R> {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        if buffer.is_empty() {
            return Ok(0);
        }
        if self.remaining == 0 {
            let mut extra = [0];
            return match self.inner.read(&mut extra)? {
                0 => Ok(0),
                _ => Err(std::io::Error::other("release archive exceeds size limit")),
            };
        }
        let limit = buffer
            .len()
            .min(self.remaining.min(usize::MAX as u64) as usize);
        let n = self.inner.read(&mut buffer[..limit])?;
        self.remaining -= n as u64;
        Ok(n)
    }
}

/// Fully verify a local archive and atomically replace the destination binary.
/// No paths or links from the tar archive are extracted to the filesystem.
pub fn install_archive(
    archive: &Path,
    expected: &str,
    destination: &Path,
) -> Result<std::path::PathBuf> {
    ensure!(
        expected.len() == 64 && expected.bytes().all(|c| c.is_ascii_hexdigit()),
        "invalid pinned archive checksum"
    );
    let mut file = File::open(archive)?;
    let mut digest = Sha256::new();
    let mut buffer = [0; 65536];
    let mut bytes = 0_u64;
    loop {
        let n = file.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        bytes += n as u64;
        ensure!(
            bytes <= 256 * 1024 * 1024,
            "release archive exceeds size limit"
        );
        digest.update(&buffer[..n]);
    }
    ensure!(
        hex::encode(digest.finalize()) == expected,
        "Substreams archive checksum mismatch"
    );
    use std::io::{Seek, SeekFrom};
    file.seek(SeekFrom::Start(0))?;
    let mut tar = tar::Archive::new(Limited {
        inner: MultiGzDecoder::new(file),
        remaining: 512 * 1024 * 1024,
    });
    let mut binary = None;
    for entry in tar.entries()? {
        let mut entry = entry?;
        if entry.path_bytes().as_ref() != b"substreams" {
            continue;
        }
        ensure!(
            entry.header().entry_type().is_file() && binary.is_none(),
            "unexpected release archive layout"
        );
        let size = entry.size();
        ensure!(
            size > 0 && size <= 256 * 1024 * 1024,
            "invalid release binary size"
        );
        let mut content = Vec::new();
        entry.read_to_end(&mut content)?;
        ensure!(content.len() as u64 == size, "truncated release binary");
        binary = Some(content);
    }
    // Tar stops at its zero records; consume the remainder to check gzip CRCs
    // and the size cap even if a valid tar prefix was already seen.
    std::io::copy(&mut tar.into_inner(), &mut std::io::sink())?;
    let binary = binary.context("unexpected release archive layout")?;
    fs::create_dir_all(destination)?;
    let mut output = tempfile::NamedTempFile::new_in(destination)?;
    output.write_all(&binary)?;
    output
        .as_file()
        .set_permissions(fs::Permissions::from_mode(0o755))?;
    output.as_file().sync_all()?;
    let path = destination.join("substreams");
    output.persist(&path)?;
    File::open(destination)?.sync_all()?;
    crate::files::resolve(&path)
}

pub fn install(destination: &Path) -> Result<std::path::PathBuf> {
    let target = platform()?;
    let expected = checksum(&target)?;
    let url=format!("https://github.com/streamingfast/substreams/releases/download/v{VERSION}/substreams_{target}.tar.gz");
    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(120))
        .build()?;
    let mut response = client
        .get(url)
        .send()
        .map_err(|_| anyhow::anyhow!("Substreams archive download failed"))?;
    ensure!(
        response.status().is_success(),
        "Substreams archive download failed (HTTP {})",
        response.status().as_u16()
    );
    let mut archive = tempfile::NamedTempFile::new()?;
    let bytes = std::io::copy(
        &mut response.by_ref().take(256 * 1024 * 1024 + 1),
        &mut archive,
    )?;
    ensure!(
        bytes <= 256 * 1024 * 1024,
        "release archive exceeds size limit"
    );
    archive.flush()?;
    install_archive(archive.path(), expected, destination)
}
