//! Explicit host-bind measurement, with fresh checks against the actual container.
use crate::{capacity, control::new_id, proof::string};
use anyhow::{ensure, Context, Result};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, OpenOptions},
    io::Write,
    os::unix::fs::MetadataExt,
    path::{Component, Path, PathBuf},
};

#[derive(Debug, PartialEq)]
struct Binding {
    container: PathBuf,
    host: PathBuf,
}

fn absolute(value: &str) -> Result<PathBuf> {
    let path = PathBuf::from(value);
    ensure!(
        path.is_absolute() && !path.components().any(|c| c == Component::ParentDir),
        "invalid host-bind path"
    );
    Ok(path)
}

fn bindings(info: &Value, disks: &[PathBuf]) -> Result<Vec<Binding>> {
    let mounts = info["Mounts"]
        .as_array()
        .context("invalid container mounts")?;
    let mut destinations = BTreeSet::new();
    for mount in mounts {
        ensure!(
            destinations.insert(absolute(string(mount, "Destination")?)?),
            "ambiguous container mount destination"
        );
    }
    let mut mapped = BTreeMap::new();
    let mut add = |path: &Path, mount: &Value| -> Result<()> {
        ensure!(mount["Type"] == "bind", "host_bind scanning requires directory bind mounts for every data path and nested mount");
        let destination = absolute(string(mount, "Destination")?)?;
        let source = absolute(string(mount, "Source")?)?;
        let host = fs::canonicalize(source.join(path.strip_prefix(destination)?))?;
        ensure!(
            host.is_dir(),
            "host_bind scanning requires readable host directories"
        );
        if let Some(previous) = mapped.insert(path.to_path_buf(), host.clone()) {
            ensure!(previous == host, "ambiguous host-bind mapping");
        }
        Ok(())
    };
    ensure!(!disks.is_empty(), "host_bind scanning requires data disks");
    for disk in disks {
        let mut covering = Vec::new();
        for mount in mounts {
            let destination = absolute(string(mount, "Destination")?)?;
            if disk.starts_with(&destination) {
                covering.push((destination.components().count(), mount));
            } else if destination.starts_with(disk) {
                // A nested volume/tmpfs cannot disappear behind its host parent.
                add(&destination, mount)?;
            }
        }
        let (_, mount) = covering
            .into_iter()
            .max_by_key(|(depth, _)| *depth)
            .context("data disk has no host bind mount")?;
        add(disk, mount)?;
    }
    Ok(mapped
        .into_iter()
        .map(|(container, host)| Binding { container, host })
        .collect())
}

struct Probe {
    path: PathBuf,
    container: PathBuf,
    value: Vec<u8>,
    root: PathBuf,
    device: u64,
    inode: u64,
    file_device: u64,
    file_inode: u64,
}
impl Drop for Probe {
    fn drop(&mut self) {
        // Only the randomly named file created by this measurement is removed.
        if fs::symlink_metadata(&self.path)
            .is_ok_and(|m| m.dev() == self.file_device && m.ino() == self.file_inode)
        {
            let _ = fs::remove_file(&self.path);
        }
    }
}
impl Probe {
    fn create(binding: &Binding) -> Result<Self> {
        let name = format!(".evm-capacity-probe-{}", new_id());
        let metadata = fs::metadata(&binding.host)?;
        let path = binding.host.join(&name);
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)?;
        let file_metadata = file.metadata()?;
        let probe = Self {
            path,
            container: binding.container.join(name),
            value: new_id().into_bytes(),
            root: binding.host.clone(),
            device: metadata.dev(),
            inode: metadata.ino(),
            file_device: file_metadata.dev(),
            file_inode: file_metadata.ino(),
        };
        file.write_all(&probe.value)?;
        file.sync_all()?;
        Ok(probe)
    }
    fn verify(&self, read: &mut impl FnMut(&Path) -> Result<Vec<u8>>) -> Result<()> {
        let metadata = fs::symlink_metadata(&self.root)?;
        ensure!(
            metadata.is_dir() && metadata.dev() == self.device && metadata.ino() == self.inode,
            "host data directory was replaced during measurement"
        );
        ensure!(
            read(&self.container)? == self.value,
            "host directory is not the container's current data mount"
        );
        Ok(())
    }
}

pub(crate) fn sample(
    info: &Value,
    disks: &[PathBuf],
    mut read: impl FnMut(&Path) -> Result<Vec<u8>>,
) -> Result<Value> {
    let bindings = bindings(info, disks)?;
    let probes = bindings
        .iter()
        .map(Probe::create)
        .collect::<Result<Vec<_>>>()?;
    for probe in &probes {
        probe.verify(&mut read)?;
    }
    let all_roots = bindings
        .iter()
        .map(|b| b.host.clone())
        .collect::<BTreeSet<_>>();
    let roots = capacity::roots(all_roots.iter().cloned().collect());
    // A concurrent sampler can remove its own probe; discard and retry the walk.
    let usage = match capacity::data_usage(&roots) {
        Err(error)
            if error
                .downcast_ref::<std::io::Error>()
                .is_some_and(|e| e.kind() == std::io::ErrorKind::NotFound) =>
        {
            capacity::data_usage(&roots)?
        }
        result => result?,
    };
    let mut available = u64::MAX;
    let mut filesystems = Vec::new();
    for root in &all_roots {
        let free = fs2::available_space(root)?;
        available = available.min(free);
        filesystems.push(json!({"path":root,"available_bytes":free}));
    }
    for probe in &probes {
        probe.verify(&mut read)?;
    }
    Ok(
        json!({"allocated_bytes":usage["allocated_bytes"],"logical_bytes":usage["logical_bytes"],
        "files":usage["files"],"available_bytes":available,"roots":roots,"filesystems":filesystems,
        "bindings":bindings.iter().map(|b|json!({"container":b.container,"host":b.host})).collect::<Vec<_>>(),
        "fresh_probes_verified_before_and_after":true,
        "scope":"Allocated host inode blocks for the complete data roots; symlinks are counted without following and hard links deduplicated. Includes temporary identity-probe files."}),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    fn info(root: &Path) -> Value {
        json!({"Mounts":[{"Type":"bind","Source":root,"Destination":"/data"}]})
    }
    #[test]
    fn mapped_subdirectories_and_nested_mounts_are_all_required() -> Result<()> {
        let first = tempfile::tempdir()?;
        let second = tempfile::tempdir()?;
        fs::create_dir(first.path().join("db"))?;
        let mut value = info(first.path());
        assert_eq!(
            bindings(&value, &["/data/db".into()])?[0].host,
            fs::canonicalize(first.path().join("db"))?
        );
        value["Mounts"]
            .as_array_mut()
            .unwrap()
            .push(json!({"Type":"bind","Source":second.path(),"Destination":"/data/db/nested"}));
        assert_eq!(bindings(&value, &["/data/db".into()])?.len(), 2);
        value["Mounts"][1]["Type"] = json!("volume");
        assert!(bindings(&value, &["/data/db".into()]).is_err());
        value["Mounts"][1]["Type"] = json!("tmpfs");
        assert!(bindings(&value, &["/data/db".into()]).is_err());
        assert!(bindings(&value, &["/elsewhere".into()]).is_err());
        let mut duplicate = info(first.path());
        let duplicate_mount = duplicate["Mounts"][0].clone();
        duplicate["Mounts"]
            .as_array_mut()
            .unwrap()
            .push(duplicate_mount);
        assert!(bindings(&duplicate, &["/data".into()]).is_err());
        Ok(())
    }
    #[test]
    fn fresh_probes_reject_copied_or_unreadable_mounts_and_clean_up() -> Result<()> {
        let root = tempfile::tempdir()?;
        let other = tempfile::tempdir()?;
        fs::write(root.path().join("state"), vec![1; 8192])?;
        let read = |path: &Path| Ok(fs::read(root.path().join(path.strip_prefix("/data")?))?);
        let measured = sample(&info(root.path()), &["/data".into()], read)?;
        assert_eq!(measured["fresh_probes_verified_before_and_after"], true);
        assert!(measured["allocated_bytes"].as_u64().unwrap() >= 8192);
        assert!(sample(&info(root.path()), &["/data".into()], |_| Ok(
            b"stale copied nonce".to_vec()
        ))
        .is_err());
        assert!(
            sample(&info(root.path()), &["/data".into()], |path| Ok(fs::read(
                other.path().join(path.strip_prefix("/data")?)
            )?))
            .is_err()
        );
        assert_eq!(fs::read_dir(root.path())?.count(), 1);
        Ok(())
    }
    #[test]
    fn replacing_the_host_directory_during_a_probe_is_rejected() -> Result<()> {
        let base = tempfile::tempdir()?;
        let root = base.path().join("data");
        fs::create_dir(&root)?;
        let mut calls = 0;
        let mut replacement = None;
        let result = sample(&info(&root), &["/data".into()], |path| {
            let value = fs::read(root.join(path.strip_prefix("/data")?))?;
            calls += 1;
            if calls == 1 {
                fs::rename(&root, base.path().join("old"))?;
                fs::create_dir(&root)?;
                let path = root.join(path.file_name().unwrap());
                fs::write(&path, b"replacement file")?;
                replacement = Some(path);
            }
            Ok(value)
        });
        assert!(result.is_err());
        assert_eq!(fs::read(replacement.unwrap())?, b"replacement file");
        Ok(())
    }
    #[test]
    fn data_walk_counts_alias_inodes_without_following_or_double_counting() -> Result<()> {
        let root = tempfile::tempdir()?;
        let outside = tempfile::tempdir()?;
        fs::write(root.path().join("state"), vec![1; 8192])?;
        fs::hard_link(root.path().join("state"), root.path().join("alias"))?;
        fs::write(outside.path().join("large"), vec![1; 1048576])?;
        std::os::unix::fs::symlink(outside.path(), root.path().join("link"))?;
        let usage = capacity::data_usage(&[root.path().into()])?;
        assert!(usage["logical_bytes"].as_u64().unwrap() < 16384);
        assert!(capacity::local_usage(&[root.path().into()]).is_err());
        Ok(())
    }
}
