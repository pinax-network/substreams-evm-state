//! Whole-directory sampled operating guards. These are not filesystem quotas.
use crate::{
    ch::{identifier, uint, ClickHouse},
    control::new_id,
    files::{atomic_json, canonical_json, resolve, spaced_json},
    process,
    proof::string,
    reader::now_ns,
};
use anyhow::{ensure, Context, Result};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    env, fmt,
    fs::{self, File},
    io::Write,
    os::unix::fs::MetadataExt,
    path::{Component, Path, PathBuf},
    process::Command,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use wait_timeout::ChildExt;

pub fn roots(mut paths: Vec<PathBuf>) -> Vec<PathBuf> {
    paths.sort_by_key(|p| (p.components().count(), p.clone()));
    paths.dedup();
    let mut result = Vec::<PathBuf>::new();
    for path in paths {
        if !result.iter().any(|parent| path.starts_with(parent)) {
            result.push(path);
        }
    }
    result
}

pub fn local_usage(paths: &[PathBuf]) -> Result<Value> {
    usage(paths, false)
}

pub(crate) fn data_usage(paths: &[PathBuf]) -> Result<Value> {
    usage(paths, true)
}

fn usage(paths: &[PathBuf], data_directory: bool) -> Result<Value> {
    let mut seen = BTreeSet::new();
    let mut allocated = 0_u64;
    let mut logical = 0_u64;
    let mut files = 0_u64;
    for root in roots(
        paths
            .iter()
            .map(fs::canonicalize)
            .collect::<std::io::Result<_>>()?,
    ) {
        let mut pending = vec![root];
        while let Some(path) = pending.pop() {
            let metadata = fs::symlink_metadata(&path)?;
            ensure!(
                data_directory || !metadata.is_symlink(),
                "capacity roots contain a symlink; declare its target as a separate root"
            );
            if !seen.insert((metadata.dev(), metadata.ino())) {
                continue;
            }
            allocated = allocated
                .checked_add(
                    metadata
                        .blocks()
                        .checked_mul(512)
                        .context("allocated byte count overflow")?,
                )
                .context("allocated byte count overflow")?;
            if metadata.is_dir() {
                for entry in fs::read_dir(&path)? {
                    pending.push(entry?.path());
                }
            } else if metadata.is_file() || data_directory {
                logical = logical
                    .checked_add(metadata.len())
                    .context("logical byte count overflow")?;
                files += 1;
            } else {
                anyhow::bail!("capacity roots contain an unsupported special file");
            }
        }
    }
    Ok(json!({"allocated_bytes":allocated,"logical_bytes":logical,"files":files}))
}

#[derive(Debug)]
pub struct DockerCapacityError {
    pub operation: &'static str,
    pub timed_out: bool,
    pub directory_changed: bool,
}

#[derive(Debug)]
pub(crate) struct SampleStage(pub &'static str);
impl fmt::Display for SampleStage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "capacity measurement failed at {}", self.0)
    }
}
impl std::error::Error for SampleStage {}
impl fmt::Display for DockerCapacityError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Docker capacity {} {}; the sample is incomplete",
            self.operation,
            if self.timed_out {
                "timed out"
            } else {
                "failed"
            }
        )
    }
}
impl std::error::Error for DockerCapacityError {}

fn docker(arguments: &[String]) -> Result<Vec<u8>> {
    let operation = if arguments[0] == "inspect" {
        "container inspection"
    } else {
        "data-directory scan"
    };
    decode_docker_output(
        operation,
        process::capture(
            Command::new("docker").args(arguments),
            Duration::from_secs(30),
        )?,
    )
}

fn decode_docker_output(
    operation: &'static str,
    output: Option<process::Output>,
) -> Result<Vec<u8>> {
    let Some(output) = output else {
        return Err(DockerCapacityError {
            operation,
            timed_out: true,
            directory_changed: false,
        }
        .into());
    };
    if !output.status.success() {
        let error = String::from_utf8_lossy(&output.stderr);
        let changed = operation == "data-directory scan"
            && output.status.code() == Some(1)
            && error.contains("No such file or directory")
            && !error.contains("Permission denied");
        return Err(DockerCapacityError {
            operation,
            timed_out: matches!(operation, "data-directory scan" | "host-bind verification")
                && output.status.code() == Some(124),
            directory_changed: changed,
        }
        .into());
    }
    Ok(output.stdout)
}

fn integer(config: &Value, key: &str, minimum: u64) -> Result<u64> {
    let value = config[key]
        .as_u64()
        .with_context(|| format!("{key} must be an integer >= {minimum}"))?;
    ensure!(value >= minimum, "{key} must be an integer >= {minimum}");
    Ok(value)
}

pub struct Meter {
    client: ClickHouse,
    pub config: Value,
    pub paths: Vec<PathBuf>,
    budget: u64,
    headroom: u64,
    min_free: u64,
    container: String,
    container_id: Option<String>,
    databases: BTreeSet<String>,
    components: BTreeMap<String, Vec<PathBuf>>,
}

pub trait CapacitySampler {
    fn config(&self) -> &Value;
    fn contains(&self, path: &Path) -> Result<bool>;
    fn sample(&mut self) -> Result<Value>;
}
impl CapacitySampler for Meter {
    fn config(&self) -> &Value {
        &self.config
    }
    fn contains(&self, path: &Path) -> Result<bool> {
        Meter::contains(self, path)
    }
    fn sample(&mut self) -> Result<Value> {
        Meter::sample(self)
    }
}

impl Meter {
    pub fn new(client: &ClickHouse, config: Value) -> Result<Self> {
        ensure!(
            config.is_object() && config["format_version"] == 1,
            "unsupported capacity configuration"
        );
        let allowed = [
            "format_version",
            "clickhouse_container",
            "local_paths",
            "databases",
            "components",
            "budget_bytes",
            "headroom_bytes",
            "min_free_bytes",
            "data_scan_mode",
        ];
        ensure!(
            config
                .as_object()
                .unwrap()
                .keys()
                .all(|key| allowed.contains(&key.as_str())),
            "unknown capacity configuration option"
        );
        let budget = integer(&config, "budget_bytes", 1)?;
        let headroom = integer(&config, "headroom_bytes", 1)?;
        let min_free = integer(&config, "min_free_bytes", 0)?;
        ensure!(
            matches!(
                config.get("data_scan_mode").and_then(Value::as_str),
                None | Some("container" | "host_bind")
            ) && config.get("data_scan_mode").is_none_or(Value::is_string),
            "data_scan_mode must be container or host_bind"
        );
        ensure!(
            headroom < budget,
            "headroom must be smaller than the capacity budget"
        );
        let container = string(&config, "clickhouse_container")?.to_owned();
        ensure!(
            !container.is_empty()
                && container
                    .bytes()
                    .enumerate()
                    .all(|(i, b)| b.is_ascii_alphanumeric()
                        || i > 0 && matches!(b, b'_' | b'.' | b'-')),
            "invalid ClickHouse container name or ID"
        );
        let declared = config["local_paths"]
            .as_array()
            .context("declare the local run, control, verification and export directories")?;
        ensure!(
            !declared.is_empty(),
            "declare the local run, control, verification and export directories"
        );
        let mut paths = Vec::new();
        for value in declared {
            let path = Path::new(value.as_str().context("invalid capacity path")?);
            ensure!(path.is_absolute(), "capacity local paths must be absolute");
            let path = fs::canonicalize(path)?;
            ensure!(
                path.is_dir(),
                "capacity local paths must be existing directories"
            );
            paths.push(path);
        }
        let paths = roots(paths);
        let databases = config
            .get("databases")
            .cloned()
            .unwrap_or_else(|| json!([client.database]));
        let databases = databases
            .as_array()
            .context("capacity databases must be a nonempty list")?;
        ensure!(
            !databases.is_empty(),
            "capacity databases must be a nonempty list"
        );
        let databases = databases
            .iter()
            .map(|v| Ok(identifier(v.as_str().context("invalid capacity database")?)?.to_owned()))
            .collect::<Result<_>>()?;
        let mut components = BTreeMap::new();
        if let Some(declared) = config.get("components") {
            for (name, values) in declared
                .as_object()
                .context("capacity components must be an object")?
            {
                ensure!(
                    !name.is_empty()
                        && name.len() <= 31
                        && name.bytes().enumerate().all(|(i, v)| v.is_ascii_lowercase()
                            || i > 0 && (v.is_ascii_digit() || v == b'_' || v == b'-')),
                    "invalid capacity component declaration"
                );
                let mut entries = Vec::new();
                for value in values
                    .as_array()
                    .context("invalid capacity component declaration")?
                {
                    let path = Path::new(value.as_str().context("invalid component path")?);
                    ensure!(
                        path.is_absolute(),
                        "capacity component paths must be absolute"
                    );
                    let path = resolve(path)?;
                    ensure!(
                        paths.iter().any(|root| path.starts_with(root)),
                        "capacity component is outside declared local roots"
                    );
                    entries.push(path);
                }
                components.insert(name.clone(), entries);
            }
        }
        let mut meter = Self {
            client: client.with_database("default")?,
            config,
            paths,
            budget,
            headroom,
            min_free,
            container,
            container_id: None,
            databases,
            components,
        };
        meter.inspect()?;
        Ok(meter)
    }

    pub fn inspect(&mut self) -> Result<Value> {
        let info: Value =
            serde_json::from_slice(&docker(&["inspect".into(), self.container.clone()])?)?;
        let info = info
            .as_array()
            .and_then(|a| a.first())
            .context("invalid container inspection")?
            .clone();
        self.container_id = Some(verify_container(
            &info,
            &self.client.url,
            self.container_id.as_deref(),
        )?);
        Ok(info)
    }

    pub fn contains(&self, path: &Path) -> Result<bool> {
        let path = resolve(path)?;
        Ok(self.paths.iter().any(|root| path.starts_with(root)))
    }

    pub fn sample(&mut self) -> Result<Value> {
        let started = now_ns()?;
        let info = self
            .inspect()
            .context(SampleStage("container inspection"))?;
        let disks = (|| self.client.rows("SELECT name,path,type,is_remote,total_space,free_space,unreserved_space FROM system.disks ORDER BY name",&Default::default())?.collect::<Result<Vec<_>>>())().context(SampleStage("ClickHouse disk query"))?;
        let (mut available, disk_paths) =
            data_disks(&info, &disks).context(SampleStage("data disk mapping and counters"))?;
        let host_scan = if self.config["data_scan_mode"] == "host_bind" {
            Some(crate::capacity_host::sample(&info, &disk_paths, |path| {
                decode_docker_output(
                    "host-bind verification",
                    process::capture(
                        Command::new("docker").args([
                            "exec",
                            self.container_id.as_deref().unwrap(),
                            "timeout",
                            "--signal=TERM",
                            "--kill-after=2s",
                            "25s",
                            "cat",
                            "--",
                            path.to_str().context("invalid container probe path")?,
                        ]),
                        Duration::from_secs(30),
                    )?,
                )
            })?)
        } else {
            None
        };
        let mut arguments = vec![
            "exec".into(),
            self.container_id.clone().unwrap(),
            // Killing the local docker client does not terminate its remote
            // exec process. Give the scanner its own shorter container deadline.
            "timeout".into(),
            "--signal=TERM".into(),
            "--kill-after=2s".into(),
            "25s".into(),
            "du".into(),
            "-s".into(),
            "-c".into(),
            "-B1".into(),
            "--null".into(),
            "--".into(),
        ];
        arguments.extend(
            disk_paths
                .iter()
                .map(|p| {
                    p.to_str()
                        .context("invalid data disk path")
                        .map(str::to_owned)
                })
                .collect::<Result<Vec<_>>>()?,
        );
        let server_bytes = if let Some(scan) = &host_scan {
            available = available.min(uint(&scan["available_bytes"])?);
            uint(&scan["allocated_bytes"])?
        } else {
            (|| du_total(&retry_scan(|| docker(&arguments))?))()
                .context(SampleStage("container data-directory scan"))?
        };
        let local = retry_local(|| local_usage(&self.paths))
            .context(SampleStage("runtime directory scan"))?;
        let used = server_bytes
            .checked_add(uint(&local["allocated_bytes"])?)
            .context("accounted byte count overflow")?;
        let mut components = BTreeMap::new();
        for (name, paths) in &self.components {
            components.insert(
                name,
                retry_local(|| {
                    let mut existing = Vec::new();
                    for path in paths {
                        if path.try_exists()? {
                            existing.push(path.clone());
                        }
                    }
                    local_usage(&existing)
                })
                .context(SampleStage("component directory scan"))?,
            );
        }
        let selected = |sql: &str| -> Result<Vec<Value>> {
            let rows = self
                .client
                .rows(sql, &Default::default())?
                .collect::<Result<Vec<_>>>()?;
            Ok(rows
                .into_iter()
                .filter(|row| {
                    row["database"]
                        .as_str()
                        .is_some_and(|name| self.databases.contains(name))
                })
                .collect())
        };
        let parts = selected("SELECT database,active,sum(bytes_on_disk) AS bytes FROM system.parts GROUP BY database,active ORDER BY database,active").context(SampleStage("ClickHouse part accounting"))?;
        let detached = selected("SELECT database,sum(bytes_on_disk) AS bytes FROM system.detached_parts GROUP BY database ORDER BY database").context(SampleStage("ClickHouse detached-part accounting"))?;
        let merges = selected(
            "SELECT database,total_size_bytes_compressed AS input_bytes FROM system.merges",
        )
        .context(SampleStage("ClickHouse merge accounting"))?;
        let mut local_disks = Vec::new();
        for path in &self.paths {
            let free =
                fs2::available_space(path).context(SampleStage("runtime filesystem free space"))?;
            available = available.min(free);
            local_disks.push(json!({"path":path,"available_bytes":free}));
        }
        let reasons = admission_reasons(used, self.headroom, self.budget, available, self.min_free);
        Ok(
            json!({"format_version":1,"sample_started_ns":started,"sample_finished_ns":now_ns()?,"container_id":self.container_id,
            "data_scan_mode":if host_scan.is_some(){"host_bind"}else{"container"},"host_bind_scan":host_scan,
            "server_data_roots":disk_paths,"server_data_allocated_bytes":server_bytes,"local_roots":self.paths,
            "local":local,"local_components":components,"accounted_allocated_bytes":used,"budget_bytes":self.budget,"headroom_bytes":self.headroom,
            "available_above_headroom_bytes":i128::from(self.budget)-i128::from(self.headroom)-i128::from(used),"min_free_bytes":self.min_free,
            "server_disks":disks,"local_filesystems":local_disks,"selected_database_parts":parts,"selected_database_detached_parts":detached,
            "selected_database_merges":merges,"admitted":reasons.is_empty(),"reasons":reasons,
            "coverage":"entire local ClickHouse data disks plus declared local roots; shared data may overcount","limit_kind":"sampled operating guard, not a filesystem quota"}),
        )
    }
}

fn data_disks(info: &Value, disks: &[Value]) -> Result<(u64, Vec<PathBuf>)> {
    ensure!(
        !disks.is_empty(),
        "complete capacity measurement requires local ClickHouse data disks"
    );
    let mut available = u64::MAX;
    let mut disk_paths = Vec::new();
    for disk in disks {
        ensure!(
            disk["type"] == "Local" && uint(&disk["is_remote"])? == 0,
            "complete capacity measurement requires local ClickHouse data disks"
        );
        let total = uint(&disk["total_space"])?;
        let free = uint(&disk["free_space"])?;
        let unreserved = uint(&disk["unreserved_space"])?;
        ensure!(
            total > 0 && free <= total && unreserved <= total,
            "invalid ClickHouse disk capacity counters"
        );
        available = available.min(free).min(unreserved);
        let path = PathBuf::from(string(disk, "path")?);
        ensure!(
            path.is_absolute() && !path.components().any(|c| c == Component::ParentDir),
            "ClickHouse data disks must be on declared persistent container mounts"
        );
        disk_paths.push(path);
    }
    let disk_paths = roots(disk_paths);
    let mounts: Vec<_> = info["Mounts"]
        .as_array()
        .context("invalid container mounts")?
        .iter()
        .filter(|m| matches!(m["Type"].as_str(), Some("bind" | "volume")))
        .map(|m| string(m, "Destination").map(PathBuf::from))
        .collect::<Result<_>>()?;
    ensure!(
        disk_paths
            .iter()
            .all(|p| mounts.iter().any(|m| p.starts_with(m))),
        "ClickHouse data disks must be on declared persistent container mounts"
    );
    Ok((available, disk_paths))
}

fn admission_reasons(
    used: u64,
    headroom: u64,
    budget: u64,
    available: u64,
    min_free: u64,
) -> Vec<&'static str> {
    let mut reasons = Vec::new();
    if u128::from(used) + u128::from(headroom) >= u128::from(budget) {
        reasons.push("budget_headroom_exhausted");
    }
    if available < min_free {
        reasons.push("filesystem_free_space_below_floor");
    }
    reasons
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::process::ExitStatusExt;
    #[test]
    fn local_walk_retries_only_transient_disappearance_and_discards_partial_work() {
        use std::io::{Error, ErrorKind};
        for (kind, failures, expected_calls) in [
            (ErrorKind::NotFound, 3, 4),
            (ErrorKind::NotFound, 5, 5),
            (ErrorKind::PermissionDenied, 1, 1),
            (ErrorKind::Other, 1, 1),
        ] {
            let mut calls = 0;
            let result = retry_local(|| -> Result<u64> {
                calls += 1;
                // Each call represents a fresh whole walk, never an accumulated subtotal.
                if calls <= failures {
                    return Err(Error::new(kind, "sensitive filesystem path").into());
                }
                Ok(600)
            });
            assert_eq!(calls, expected_calls);
            if kind == ErrorKind::NotFound && failures < 5 {
                assert_eq!(result.unwrap(), 600);
            } else {
                assert_eq!(
                    result.unwrap_err().downcast_ref::<Error>().unwrap().kind(),
                    kind
                );
            }
        }
    }

    #[test]
    fn failed_samples_expose_only_static_stage_and_safe_io_metadata() -> Result<()> {
        let error = anyhow::Error::from(std::io::Error::from_raw_os_error(libc::ENOENT))
            .context("sensitive filesystem path and credentials")
            .context(SampleStage("runtime directory scan"));
        let measured = failure(&error, now_ns()?)?;
        assert_eq!(measured["measurement_stage"], "runtime directory scan");
        assert_eq!(measured["io_error_kind"], "NotFound");
        assert_eq!(measured["io_error_code"], libc::ENOENT);
        assert_eq!(measured["admitted"], false);
        assert_eq!(measured["reasons"], json!(["incomplete_capacity_sample"]));
        assert!(measured.get("accounted_allocated_bytes").is_none());
        assert!(!measured.to_string().contains("sensitive"));
        assert!(!measured.to_string().contains("credentials"));
        let plain = failure(
            &anyhow::anyhow!("untrusted endpoint or SQL error"),
            now_ns()?,
        )?;
        assert!(plain.get("measurement_stage").is_none());
        assert!(plain.get("io_error_kind").is_none());
        assert!(!plain.to_string().contains("untrusted"));
        Ok(())
    }

    #[test]
    fn local_disk_validation_and_skewed_counters_fail_closed() -> Result<()> {
        let info = json!({"Mounts":[{"Type":"volume","Destination":"/var/lib/clickhouse"}]});
        let disk = json!({"name":"default","path":"/var/lib/clickhouse/","type":"Local","is_remote":0,"total_space":1_000_000_000,"free_space":799_995_904,"unreserved_space":800_000_000});
        let (available, paths) = data_disks(&info, &[disk.clone()])?;
        assert_eq!(available, 799_995_904);
        assert_eq!(paths, vec![PathBuf::from("/var/lib/clickhouse/")]);
        assert_eq!(
            admission_reasons(600, 100, 500, available, 800_000_000),
            vec![
                "budget_headroom_exhausted",
                "filesystem_free_space_below_floor"
            ]
        );
        assert_eq!(
            admission_reasons(600, 100, 1_000_000_000, available, 800_000_000),
            vec!["filesystem_free_space_below_floor"]
        );
        assert!(admission_reasons(600, 100, 1_000_000_000, available, available).is_empty());
        assert_eq!(
            admission_reasons(u64::MAX, 1, u64::MAX, 0, 0),
            vec!["budget_headroom_exhausted"]
        );
        for (field, value) in [
            ("total_space", json!(0)),
            ("free_space", json!(1_000_000_001)),
            ("unreserved_space", json!(-1)),
            ("unreserved_space", json!(1_000_000_001)),
            ("is_remote", json!(1)),
            ("type", json!("S3")),
            ("path", json!("/var/lib/clickhouse/../outside")),
            ("path", json!("relative")),
            ("path", json!("/unmounted")),
        ] {
            let mut bad = disk.clone();
            bad[field] = value;
            assert!(data_disks(&info, &[bad]).is_err(), "{field}");
        }
        assert!(data_disks(&json!({"Mounts":[]}), &[disk]).is_err());
        assert!(data_disks(&info, &[]).is_err());
        Ok(())
    }
    #[test]
    fn docker_failures_discard_partial_output_and_only_retry_disappearing_files() {
        for operation in [
            "container inspection",
            "data-directory scan",
            "host-bind verification",
        ] {
            for (stderr, expected) in [
                ("du: No such file or directory", true),
                ("du: Permission denied", false),
                ("No such file or directory\nPermission denied", false),
                ("dummy-secret", false),
            ] {
                for status in [1, 2, 124] {
                    let result = decode_docker_output(
                        operation,
                        Some(process::Output {
                            status: std::process::ExitStatus::from_raw(status << 8),
                            stdout: b"untrustworthy partial total".to_vec(),
                            stderr: stderr.as_bytes().to_vec(),
                        }),
                    )
                    .unwrap_err();
                    let error = result.downcast_ref::<DockerCapacityError>().unwrap();
                    assert_eq!(error.operation, operation);
                    assert_eq!(
                        error.timed_out,
                        operation != "container inspection" && status == 124
                    );
                    assert_eq!(
                        error.directory_changed,
                        expected && operation == "data-directory scan" && status == 1
                    );
                    assert!(!error.to_string().contains("dummy-secret"));
                    assert!(!error.to_string().contains("partial total"));
                }
            }
            assert!(
                decode_docker_output(operation, None)
                    .unwrap_err()
                    .downcast_ref::<DockerCapacityError>()
                    .unwrap()
                    .timed_out
            );
        }
    }
}

pub fn retry_scan(mut scan: impl FnMut() -> Result<Vec<u8>>) -> Result<Vec<u8>> {
    for attempt in 0..5 {
        match scan() {
            Ok(output) => return Ok(output),
            Err(error)
                if attempt < 4
                    && error
                        .downcast_ref::<DockerCapacityError>()
                        .is_some_and(|e| e.directory_changed) =>
            {
                std::thread::sleep(Duration::from_millis(100 << attempt))
            }
            Err(error) => return Err(error),
        }
    }
    unreachable!()
}

pub(crate) fn retry_local<T>(mut sample: impl FnMut() -> Result<T>) -> Result<T> {
    for attempt in 0..5 {
        match sample() {
            Err(error)
                if attempt < 4
                    && error
                        .downcast_ref::<std::io::Error>()
                        .is_some_and(|e| e.kind() == std::io::ErrorKind::NotFound) =>
            {
                std::thread::sleep(Duration::from_millis(100 << attempt));
            }
            result => return result,
        }
    }
    unreachable!()
}

pub fn du_total(output: &[u8]) -> Result<u64> {
    let end = output
        .iter()
        .rposition(|b| *b != 0)
        .map(|i| i + 1)
        .unwrap_or(0);
    let last = output[..end]
        .rsplit(|b| *b == 0)
        .next()
        .context("invalid ClickHouse data-directory measurement")?;
    let text = std::str::from_utf8(last)?;
    let (bytes, label) = text
        .split_once('\t')
        .context("invalid ClickHouse data-directory measurement")?;
    ensure!(
        label == "total" && !bytes.is_empty() && bytes.bytes().all(|b| b.is_ascii_digit()),
        "invalid ClickHouse data-directory measurement"
    );
    bytes
        .parse()
        .context("invalid ClickHouse data-directory measurement")
}

fn failure(error: &anyhow::Error, started: u64) -> Result<Value> {
    let mut value = json!({"format_version":1,"sample_started_ns":started,"sample_finished_ns":now_ns()?,"admitted":false,
        "reasons":["incomplete_capacity_sample"],"error_type":"CapacityError"});
    if let Some(stage) = error.downcast_ref::<SampleStage>() {
        value["measurement_stage"] = json!(stage.0);
    }
    if let Some(error) = error.downcast_ref::<std::io::Error>() {
        value["io_error_kind"] = json!(format!("{:?}", error.kind()));
        value["io_error_code"] = json!(error.raw_os_error());
    }
    if let Some(error) = error.downcast_ref::<DockerCapacityError>() {
        value["error_type"] = json!("DockerCapacityError");
        value["inspection_operation"] = json!(error.operation);
        value["directory_changed"] = json!(error.directory_changed);
        value["timed_out"] = json!(error.timed_out);
    }
    Ok(value)
}

pub fn check(
    client: &ClickHouse,
    required_paths: &[PathBuf],
    stage: &str,
) -> Result<Option<Value>> {
    if let Some(guard) = &client.additional_capacity_guard {
        guard(stage)?;
    }
    let Some(policy) = env::var_os("EVM_STATE_CAPACITY_CONFIG") else {
        return Ok(None);
    };
    let mut meter = Meter::new(client, serde_json::from_slice(&fs::read(policy)?)?)?;
    let destination = env::var_os("EVM_STATE_CAPACITY_EVENTS")
        .map(|path| resolve(Path::new(&path)))
        .transpose()?;
    guard(&mut meter, required_paths, stage, destination.as_deref()).map(Some)
}

/// Apply a complete sample at an operation boundary. Used by the configured
/// Docker guard and embedders with another capacity sampler.
pub fn guard(
    meter: &mut impl CapacitySampler,
    required_paths: &[PathBuf],
    stage: &str,
    destination: Option<&Path>,
) -> Result<Value> {
    for path in required_paths {
        ensure!(
            meter.contains(path)?,
            "operation uses a directory outside the declared capacity roots"
        );
    }
    if let Some(destination) = &destination {
        ensure!(
            meter.contains(destination)?,
            "capacity events directory is outside declared roots"
        );
    }
    let started = now_ns()?;
    let measured = match meter.sample() {
        Ok(value) => value,
        Err(error) => {
            if let Some(destination) = &destination {
                let mut value = failure(&error, started)?;
                value["stage"] = json!(stage);
                atomic_json(
                    &destination.join(format!("{}.json", new_id())),
                    &value,
                    false,
                )?;
            }
            return Err(error);
        }
    };
    if let Some(destination) = &destination {
        let mut value = measured.clone();
        value["stage"] = json!(stage);
        atomic_json(
            &destination.join(format!("{}.json", new_id())),
            &value,
            false,
        )?;
    }
    ensure!(
        measured["admitted"] == true,
        "capacity guard rejected operation: {}",
        measured["reasons"]
    );
    Ok(measured)
}

#[derive(Default)]
struct Stats {
    samples: u64,
    failures: u64,
    peak: u64,
    max_gap: u64,
    last: Option<u64>,
}
impl Stats {
    fn record(&mut self, meter: &mut impl CapacitySampler, log: &mut File) -> Result<Value> {
        let started = now_ns()?;
        let value = match meter.sample() {
            Ok(value) => {
                self.samples += 1;
                self.peak = self.peak.max(uint(&value["accounted_allocated_bytes"])?);
                let now = uint(&value["sample_finished_ns"])?;
                if let Some(last) = self.last {
                    self.max_gap = self.max_gap.max(now.saturating_sub(last));
                }
                self.last = Some(now);
                value
            }
            Err(error) => {
                self.failures += 1;
                failure(&error, started)?
            }
        };
        writeln!(log, "{}", canonical_json(&value)?)?;
        log.flush()?;
        log.sync_all()?;
        Ok(value)
    }
}

pub fn supervise(
    meter: &mut impl CapacitySampler,
    command: &[String],
    output: &Path,
    interval: f64,
) -> Result<Value> {
    ensure!(
        !command.is_empty(),
        "capacity-run requires a command after --"
    );
    ensure!(
        interval.is_finite() && (0.1..=60.0).contains(&interval),
        "sample interval must be between 0.1 and 60 seconds"
    );
    let output = resolve(output)?;
    ensure!(
        meter.contains(&output)?,
        "capacity report directory must be inside a declared local root"
    );
    fs::create_dir_all(output.parent().context("capacity output has no parent")?)?;
    fs::create_dir(&output)?;
    atomic_json(&output.join("config.json"), meter.config(), false)?;
    fs::create_dir(output.join("guards"))?;
    let started = now_ns()?;
    let mut stats = Stats::default();
    let mut result = None;
    let mut reasons = BTreeSet::<String>::new();
    let mut termination_error = None;
    let config_hash = hex::encode(Sha256::digest(spaced_json(meter.config())?));
    let mut log = File::create_new(output.join("samples.jsonl"))?;
    let stopped = Arc::new(AtomicBool::new(false));
    let signals = [signal_hook::consts::SIGINT, signal_hook::consts::SIGTERM]
        .into_iter()
        .map(|signal| signal_hook::flag::register(signal, stopped.clone()))
        .collect::<std::io::Result<Vec<_>>>()?;
    let execute = (|| -> Result<()> {
        let first = stats.record(meter, &mut log)?;
        if first["admitted"] != true {
            for reason in first["reasons"]
                .as_array()
                .context("invalid sample reasons")?
            {
                reasons.insert(reason.as_str().context("invalid sample reason")?.into());
            }
            return Ok(());
        }
        let mut process = process::OwnedGroup::spawn(
            Command::new(&command[0])
                .args(&command[1..])
                .env("EVM_STATE_CAPACITY_CONFIG", output.join("config.json"))
                .env("EVM_STATE_CAPACITY_EVENTS", output.join("guards")),
        )?;
        let run = (|| -> Result<()> {
            loop {
                result = process
                    .child
                    .wait_timeout(Duration::from_secs_f64(interval))?
                    .map(process::exit_code);
                let current = stats.record(meter, &mut log)?;
                if current["admitted"] != true {
                    for reason in current["reasons"]
                        .as_array()
                        .context("invalid sample reasons")?
                    {
                        reasons.insert(reason.as_str().context("invalid sample reason")?.into());
                    }
                }
                if stopped.load(Ordering::Relaxed) {
                    reasons.insert("supervisor_interrupted".into());
                }
                if result.is_some() || !reasons.is_empty() {
                    break;
                }
            }
            Ok(())
        })();
        if result.is_none() || !reasons.is_empty() || run.is_err() {
            match process.terminate() {
                Ok(status) => result = Some(process::exit_code(status)),
                Err(_) => termination_error = Some("could_not_confirm_process_group_termination"),
            }
        } else {
            process.complete();
        }
        run
    })();
    for signal in signals {
        signal_hook::low_level::unregister(signal);
    }
    if execute.is_err() {
        reasons.insert("supervisor_error".into());
    }
    let mut guards = Vec::new();
    for entry in fs::read_dir(output.join("guards"))? {
        let path = entry?.path();
        if path.extension().is_some_and(|v| v == "json") {
            guards.push(serde_json::from_slice::<Value>(&fs::read(path)?)?);
        }
    }
    let mut rejected = Vec::new();
    let mut guard_failures = 0;
    for guard in &guards {
        if let Some(bytes) = guard.get("accounted_allocated_bytes") {
            stats.peak = stats.peak.max(uint(bytes)?);
        }
        let causes = guard["reasons"]
            .as_array()
            .context("invalid capacity guard reasons")?;
        if causes.contains(&json!("incomplete_capacity_sample")) {
            guard_failures += 1;
        }
        if guard["admitted"] != true {
            rejected.push(string(guard, "stage")?.to_owned());
            for cause in causes {
                reasons.insert(
                    cause
                        .as_str()
                        .context("invalid capacity guard reason")?
                        .into(),
                );
            }
        }
    }
    let report = json!({"format_version":1,"started_ns":started,"finished_ns":now_ns()?,"config_sha256":config_hash,"config":meter.config(),
        "samples":stats.samples,"failed_samples":stats.failures,"peak_observed_allocated_bytes":stats.peak,"maximum_sample_gap_ns":stats.max_gap,
        "guard_samples":guards.len(),"failed_guard_samples":guard_failures,"rejected_guard_stages":rejected,"command_exit_code":result,
        "stop_reasons":reasons,"termination_error":termination_error,
        "status":if result==Some(0) && reasons.is_empty() && termination_error.is_none() {"completed"} else {"stopped"},
        "limit_kind":"sampled operating guard; excursions between samples are not bounded by this report"});
    atomic_json(&output.join("summary.json"), &report, false)?;
    Ok(report)
}

pub fn verify_container(info: &Value, endpoint: &str, previous: Option<&str>) -> Result<String> {
    let id = string(info, "Id")?;
    ensure!(
        previous.is_none_or(|previous| previous == id),
        "ClickHouse capacity container was replaced"
    );
    let url = reqwest::Url::parse(endpoint)
        .map_err(|_| anyhow::anyhow!("invalid local capacity endpoint"))?;
    ensure!(
        url.scheme() == "http"
            && matches!(url.host_str(), Some("localhost" | "127.0.0.1"))
            && url.username().is_empty()
            && url.password().is_none(),
        "Docker capacity measurement requires a local published HTTP endpoint"
    );
    let port = url.port().unwrap_or(80).to_string();
    let bindings = info["NetworkSettings"]["Ports"]["8123/tcp"].as_array();
    ensure!(
        info["State"]["Running"] == true
            && bindings.is_some_and(|entries| entries.iter().any(|binding| binding["HostPort"]
                == port
                && matches!(
                    binding["HostIp"].as_str(),
                    Some("127.0.0.1" | "0.0.0.0" | "")
                ))),
        "capacity container does not own the configured ClickHouse HTTP endpoint"
    );
    Ok(id.into())
}
