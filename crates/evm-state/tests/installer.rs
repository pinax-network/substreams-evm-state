use anyhow::Result;
use evm_state::installer;
use flate2::{write::GzEncoder, Compression};
use sha2::{Digest, Sha256};
use std::{fs, os::unix::fs::PermissionsExt, path::Path};

fn archive(path: &Path, entries: &[(&str, tar::EntryType, &[u8])]) -> Result<Vec<u8>> {
    let mut tar = tar::Builder::new(Vec::new());
    for (name, kind, bytes) in entries {
        let mut header = tar::Header::new_gnu();
        header.set_entry_type(*kind);
        header.set_mode(0o755);
        header.set_size(bytes.len() as u64);
        if kind.is_symlink() {
            header.set_link_name("/outside")?;
        }
        header.set_cksum();
        tar.append_data(&mut header, name, *bytes)?;
    }
    let mut gzip = GzEncoder::new(Vec::new(), Compression::default());
    std::io::Write::write_all(&mut gzip, &tar.into_inner()?)?;
    let bytes = gzip.finish()?;
    fs::write(path, &bytes)?;
    Ok(bytes)
}
fn sha(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

#[test]
fn pinned_archive_installs_only_the_executable_and_atomically_replaces_old_binary() -> Result<()> {
    let root = tempfile::tempdir()?;
    let path = root.path().join("archive.gz");
    let destination = root.path().join("bin");
    fs::create_dir(&destination)?;
    fs::write(destination.join("substreams"), b"old")?;
    let bytes = archive(
        &path,
        &[
            (
                "substreams",
                tar::EntryType::Regular,
                b"qualified executable",
            ),
            (
                "nested/ignored",
                tar::EntryType::Regular,
                b"never extracted",
            ),
        ],
    )?;
    let installed = installer::install_archive(&path, &sha(&bytes), &destination)?;
    assert_eq!(fs::read(&installed)?, b"qualified executable");
    assert_eq!(fs::metadata(installed)?.permissions().mode() & 0o777, 0o755);
    assert_eq!(fs::read_dir(destination)?.count(), 1);
    assert!(!root.path().join("nested").exists());
    Ok(())
}

#[test]
fn checksum_mismatch_truncated_gzip_and_bad_crc_preserve_existing_binary() -> Result<()> {
    for defect in ["checksum", "truncated", "crc"] {
        let root = tempfile::tempdir()?;
        let path = root.path().join("archive.gz");
        let destination = root.path().join("bin");
        fs::create_dir(&destination)?;
        fs::write(destination.join("substreams"), b"old")?;
        let mut bytes = archive(&path, &[("substreams", tar::EntryType::Regular, b"binary")])?;
        let expected = match defect {
            "checksum" => "0".repeat(64),
            "truncated" => {
                bytes.truncate(bytes.len() - 3);
                sha(&bytes)
            }
            _ => {
                let i = bytes.len() - 8;
                bytes[i] ^= 0xff;
                sha(&bytes)
            }
        };
        fs::write(&path, bytes)?;
        assert!(
            installer::install_archive(&path, &expected, &destination).is_err(),
            "{defect}"
        );
        assert_eq!(fs::read(destination.join("substreams"))?, b"old");
    }
    Ok(())
}

#[test]
fn missing_duplicate_link_and_empty_binaries_are_rejected_before_destination_creation() -> Result<()>
{
    let variants = vec![
        vec![(
            "nested/substreams",
            tar::EntryType::Regular,
            b"binary".as_slice(),
        )],
        vec![
            ("substreams", tar::EntryType::Regular, b"one".as_slice()),
            ("substreams", tar::EntryType::Regular, b"two".as_slice()),
        ],
        vec![("substreams", tar::EntryType::Symlink, b"".as_slice())],
        vec![("substreams", tar::EntryType::Regular, b"".as_slice())],
    ];
    for entries in variants {
        let root = tempfile::tempdir()?;
        let path = root.path().join("archive.gz");
        let destination = root.path().join("bin");
        let bytes = archive(&path, &entries)?;
        assert!(installer::install_archive(&path, &sha(&bytes), &destination).is_err());
        assert!(!destination.exists());
    }
    Ok(())
}

#[test]
fn supported_platforms_have_distinct_pinned_checksums() -> Result<()> {
    let mut sums = std::collections::BTreeSet::new();
    for target in [
        "linux_arm64",
        "linux_x86_64",
        "darwin_arm64",
        "darwin_x86_64",
    ] {
        let sum = installer::checksum(target)?;
        assert_eq!(sum.len(), 64);
        assert!(sums.insert(sum));
    }
    assert!(installer::checksum("windows_x86_64").is_err());
    Ok(())
}
