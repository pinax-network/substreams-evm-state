use anyhow::Result;
use evm_state::{
    capacity::{self, CapacitySampler, DockerCapacityError},
    files,
    reader::now_ns,
};
use serde_json::{json, Value};
use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
    sync::{atomic::AtomicBool, Arc},
    time::{Duration, Instant},
};

#[test]
fn nested_roots_and_hard_links_are_counted_once() -> Result<()> {
    let root = tempfile::tempdir()?;
    let nested = root.path().join("nested");
    fs::create_dir(&nested)?;
    fs::write(nested.join("data"), b"hello")?;
    fs::hard_link(nested.join("data"), root.path().join("hardlink"))?;
    let result = capacity::local_usage(&[root.path().into(), nested])?;
    assert_eq!(result["logical_bytes"], 5);
    assert_eq!(result["files"], 1);
    assert!(result["allocated_bytes"].as_u64().unwrap() > 0);
    Ok(())
}

#[test]
fn symlinks_special_files_and_missing_roots_cannot_hide_unmeasured_bytes() -> Result<()> {
    let root = tempfile::tempdir()?;
    assert!(capacity::local_usage(&[root.path().join("missing")]).is_err());
    std::os::unix::fs::symlink("/not/a/declared/root", root.path().join("outside"))?;
    assert!(capacity::local_usage(&[root.path().into()]).is_err());
    fs::remove_file(root.path().join("outside"))?;
    let _socket = std::os::unix::net::UnixListener::bind(root.path().join("socket"))?;
    assert!(capacity::local_usage(&[root.path().into()]).is_err());
    Ok(())
}

#[test]
fn only_disappearing_files_allow_bounded_fresh_retries() {
    for (changed, failures, expected_calls) in [(true, 3, 4), (true, 5, 5), (false, 1, 1)] {
        let mut calls = 0;
        let result = capacity::retry_scan(|| {
            calls += 1;
            if calls <= failures {
                Err(DockerCapacityError {
                    operation: "data-directory scan",
                    timed_out: false,
                    directory_changed: changed,
                }
                .into())
            } else {
                Ok(b"600\ttotal\0".to_vec())
            }
        });
        assert_eq!(calls, expected_calls);
        assert_eq!(result.is_ok(), changed && failures < 5);
        if let Ok(value) = result {
            assert_eq!(capacity::du_total(&value).unwrap(), 600);
        }
    }
}

#[test]
fn partial_directory_totals_are_never_accepted() {
    assert_eq!(
        capacity::du_total(b"600\t/path\0".as_slice()).is_err(),
        true
    );
    for value in [
        b"".as_slice(),
        b"600\t/path\0broken\0",
        b"-1\ttotal\0",
        b"1.5\ttotal\0",
        b"5\ttotal\n",
    ] {
        assert!(capacity::du_total(value).is_err());
    }
    assert_eq!(
        capacity::du_total(b"42\t/path\0\x36\x30\x30\ttotal\0").unwrap(),
        600
    );
}

#[test]
fn inspection_rejects_wrong_endpoint_stopped_or_replaced_container() -> Result<()> {
    let info = json!({"Id":"owned","State":{"Running":true},"NetworkSettings":{"Ports":{"8123/tcp":[{"HostIp":"127.0.0.1","HostPort":"18123"}]}}});
    assert_eq!(
        capacity::verify_container(&info, "http://127.0.0.1:18123", Some("owned"))?,
        "owned"
    );
    for endpoint in [
        "http://127.0.0.1:19999",
        "http://remote.example.invalid:18123",
        "http://user:password@127.0.0.1:18123",
        "https://127.0.0.1:18123",
    ] {
        assert!(capacity::verify_container(&info, endpoint, None).is_err());
    }
    assert!(
        capacity::verify_container(&info, "http://127.0.0.1:18123", Some("replacement")).is_err()
    );
    let mut stopped = info;
    stopped["State"]["Running"] = json!(false);
    assert!(capacity::verify_container(&stopped, "http://127.0.0.1:18123", None).is_err());
    Ok(())
}

struct FakeMeter {
    root: PathBuf,
    config: Value,
    calls: usize,
    mode: &'static str,
}
impl FakeMeter {
    fn new(root: &Path, mode: &'static str) -> Result<Self> {
        Ok(Self {
            root: files::resolve(root)?,
            config: json!({"format_version":1,"test":true}),
            calls: 0,
            mode,
        })
    }
}
impl CapacitySampler for FakeMeter {
    fn config(&self) -> &Value {
        &self.config
    }
    fn contains(&self, path: &Path) -> Result<bool> {
        Ok(files::resolve(path)?.starts_with(&self.root))
    }
    fn sample(&mut self) -> Result<Value> {
        self.calls += 1;
        if self.mode == "docker-failure" {
            return Err(DockerCapacityError {
                operation: "data-directory scan",
                timed_out: false,
                directory_changed: true,
            }
            .into());
        }
        if self.mode == "incomplete" && self.calls > 1 {
            anyhow::bail!("unreadable capacity directory");
        }
        let allowed = self.mode != "reject"
            && (self.mode != "marker" || !self.root.join("child-ready").exists());
        Ok(
            json!({"sample_finished_ns":now_ns()?,"accounted_allocated_bytes":123,"admitted":allowed,"reasons":if allowed {vec![]} else {vec!["budget_headroom_exhausted"]}}),
        )
    }
}

fn child_command(mode: &str, root: &Path) -> Vec<String> {
    vec![
        "/usr/bin/env".into(),
        format!("EVM_TEST_CAPACITY_CHILD={mode}"),
        format!("EVM_TEST_CAPACITY_ROOT={}", root.display()),
        std::env::current_exe().unwrap().to_str().unwrap().into(),
        "--exact".into(),
        "supervisor_child_fixture".into(),
        "--ignored".into(),
        "--nocapture".into(),
    ]
}

#[test]
#[ignore = "subprocess-only fixture; invoked explicitly by supervisor tests"]
fn supervisor_child_fixture() -> Result<()> {
    let mode = std::env::var("EVM_TEST_CAPACITY_CHILD")?;
    let root = PathBuf::from(std::env::var_os("EVM_TEST_CAPACITY_ROOT").unwrap());
    match mode.as_str() {
        "policy" => fs::write(
            root.join("seen-policy"),
            std::env::var("EVM_STATE_CAPACITY_CONFIG")?,
        )?,
        "guard" => files::atomic_json(
            &PathBuf::from(std::env::var_os("EVM_STATE_CAPACITY_EVENTS").unwrap())
                .join("failed.json"),
            &json!({"sample_finished_ns":1,"admitted":false,"stage":"bootstrap-compact","reasons":["incomplete_capacity_sample"],"error_type":"CapacityError"}),
            false,
        )?,
        "stubborn-parent" => {
            let mut child = Command::new(std::env::current_exe()?)
                .args([
                    "--exact",
                    "supervisor_child_fixture",
                    "--ignored",
                    "--nocapture",
                ])
                .env("EVM_TEST_CAPACITY_CHILD", "stubborn-child")
                .spawn()?;
            child.wait()?;
        }
        "stubborn-child" => {
            signal_hook::flag::register(
                signal_hook::consts::SIGTERM,
                Arc::new(AtomicBool::new(false)),
            )?;
            let _lock = files::file_lock(&root.join("child.lock"), true, false)?;
            fs::write(root.join("child-ready"), b"ready")?;
            std::thread::sleep(Duration::from_secs(60));
        }
        "sleep" => std::thread::sleep(Duration::from_secs(60)),
        _ => anyhow::bail!("unknown child fixture"),
    }
    Ok(())
}

#[test]
fn supervisor_records_exit_and_passes_frozen_policy() -> Result<()> {
    let root = tempfile::tempdir()?;
    let mut meter = FakeMeter::new(root.path(), "accept")?;
    let output = root.path().join("run");
    let result = capacity::supervise(
        &mut meter,
        &child_command("policy", root.path()),
        &output,
        0.1,
    )?;
    assert_eq!(result["status"], "completed");
    assert_eq!(result["command_exit_code"], 0);
    assert!(result["samples"].as_u64().unwrap() >= 2);
    assert_eq!(
        fs::read_to_string(root.path().join("seen-policy"))?,
        files::resolve(&output)?
            .join("config.json")
            .to_str()
            .unwrap()
    );
    assert_eq!(
        serde_json::from_slice::<Value>(&fs::read(output.join("summary.json"))?)?,
        result
    );
    Ok(())
}

#[test]
fn unsafe_initial_sample_never_launches_the_child() -> Result<()> {
    let root = tempfile::tempdir()?;
    let mut meter = FakeMeter::new(root.path(), "reject")?;
    let result = capacity::supervise(
        &mut meter,
        &child_command("policy", root.path()),
        &root.path().join("run"),
        0.1,
    )?;
    assert_eq!(result["status"], "stopped");
    assert!(result["command_exit_code"].is_null());
    assert!(!root.path().join("seen-policy").exists());
    Ok(())
}

#[test]
fn incomplete_sample_stops_child_without_inventing_a_zero_total() -> Result<()> {
    let root = tempfile::tempdir()?;
    let mut meter = FakeMeter::new(root.path(), "incomplete")?;
    let result = capacity::supervise(
        &mut meter,
        &child_command("sleep", root.path()),
        &root.path().join("run"),
        0.1,
    )?;
    assert_eq!(result["status"], "stopped");
    assert_eq!(result["failed_samples"], 1);
    assert_eq!(
        result["stop_reasons"],
        json!(["incomplete_capacity_sample"])
    );
    let log = fs::read_to_string(root.path().join("run/samples.jsonl"))?;
    let rejected: Value = serde_json::from_str(log.lines().last().unwrap())?;
    assert!(rejected.get("accounted_allocated_bytes").is_none());
    Ok(())
}

#[test]
fn budget_stop_kills_stubborn_grandchild_and_releases_its_lock() -> Result<()> {
    let root = tempfile::tempdir()?;
    let mut meter = FakeMeter::new(root.path(), "marker")?;
    let result = capacity::supervise(
        &mut meter,
        &child_command("stubborn-parent", root.path()),
        &root.path().join("run"),
        0.1,
    )?;
    assert_eq!(result["status"], "stopped");
    assert_eq!(result["stop_reasons"], json!(["budget_headroom_exhausted"]));
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match files::file_lock(&root.path().join("child.lock"), true, false) {
            Ok(_) => break,
            Err(e) if Instant::now() >= deadline => return Err(e),
            Err(_) => std::thread::sleep(Duration::from_millis(10)),
        }
    }
    Ok(())
}

#[test]
fn internal_guard_failure_cannot_be_overridden_by_zero_child_exit() -> Result<()> {
    let root = tempfile::tempdir()?;
    let mut meter = FakeMeter::new(root.path(), "accept")?;
    let result = capacity::supervise(
        &mut meter,
        &child_command("guard", root.path()),
        &root.path().join("run"),
        0.1,
    )?;
    assert_eq!(result["status"], "stopped");
    assert_eq!(result["command_exit_code"], 0);
    assert_eq!(result["failed_samples"], 0);
    assert_eq!(result["failed_guard_samples"], 1);
    assert_eq!(
        result["rejected_guard_stages"],
        json!(["bootstrap-compact"])
    );
    assert_eq!(result["peak_observed_allocated_bytes"], 123);
    Ok(())
}

#[test]
fn guard_checks_declared_roots_and_retains_honest_failed_measurements() -> Result<()> {
    let root = tempfile::tempdir()?;
    let mut meter = FakeMeter::new(root.path(), "accept")?;
    let events = root.path().join("guards");
    assert!(capacity::guard(
        &mut meter,
        &[root.path().join("../untracked")],
        "test",
        Some(&events)
    )
    .is_err());
    assert!(capacity::guard(
        &mut meter,
        &[],
        "test",
        Some(&root.path().join("../outside-events"))
    )
    .is_err());
    assert_eq!(meter.calls, 0);
    capacity::guard(
        &mut meter,
        &[root.path().join("future-work")],
        "test-guard",
        Some(&events),
    )?;
    let records = || -> Result<Vec<Value>> {
        fs::read_dir(&events)?
            .map(|e| Ok(serde_json::from_slice(&fs::read(e?.path())?)?))
            .collect()
    };
    assert_eq!(records()?[0]["stage"], "test-guard");
    assert_eq!(records()?[0]["accounted_allocated_bytes"], 123);
    meter.mode = "incomplete";
    assert!(capacity::guard(&mut meter, &[], "bootstrap-compact", Some(&events)).is_err());
    let values = records()?;
    let failed = values
        .iter()
        .find(|v| v["stage"] == "bootstrap-compact")
        .unwrap();
    assert_eq!(failed["admitted"], false);
    assert_eq!(failed["reasons"], json!(["incomplete_capacity_sample"]));
    assert!(failed.get("accounted_allocated_bytes").is_none());
    meter.mode = "reject";
    assert!(capacity::guard(&mut meter, &[], "publish", Some(&events)).is_err());
    assert_eq!(records()?.len(), 3);
    meter.mode = "docker-failure";
    assert!(capacity::guard(&mut meter, &[], "scan-failed", Some(&events)).is_err());
    let values = records()?;
    let failed = values.iter().find(|v| v["stage"] == "scan-failed").unwrap();
    assert_eq!(failed["error_type"], "DockerCapacityError");
    assert_eq!(failed["inspection_operation"], "data-directory scan");
    assert_eq!(failed["directory_changed"], true);
    assert!(failed.get("accounted_allocated_bytes").is_none());
    Ok(())
}

#[test]
fn completed_child_cannot_override_a_rejected_final_sample() -> Result<()> {
    let root = tempfile::tempdir()?;
    let mut meter = FakeMeter::new(root.path(), "marker")?;
    let command = vec![
        "/usr/bin/touch".into(),
        root.path().join("child-ready").to_str().unwrap().into(),
    ];
    let report = capacity::supervise(&mut meter, &command, &root.path().join("run"), 1.)?;
    assert_eq!(report["status"], "stopped");
    assert_eq!(report["command_exit_code"], 0);
    // The sample completed and denied admission; failed_samples counts
    // incomplete measurements, not complete measurements over the budget.
    assert_eq!(report["failed_samples"], 0);
    assert_eq!(report["stop_reasons"], json!(["budget_headroom_exhausted"]));
    Ok(())
}

#[test]
fn growth_evidence_requires_fresh_admitted_samples_and_exact_run_binding() -> Result<()> {
    let run = json!({"run_id":"run","database_uuid":"db","identity":{"accounts":["a"]}});
    let mut prefix = json!({"binding":evm_state::cursor::binding(&run)?,"accounts":["a"],"status":"unverified-bootstrap"});
    let mut sample =
        json!({"admitted":true,"accounted_allocated_bytes":10,"sample_finished_ns":100});
    assert!(evm_state::qualification::admit_growth(
        &prefix, &run, &sample, 100
    )?);
    assert!(!evm_state::qualification::admit_growth(
        &prefix,
        &run,
        &sample,
        60_000_000_101
    )?);
    assert!(!evm_state::qualification::admit_growth(
        &prefix, &run, &sample, 99
    )?);
    sample["admitted"] = json!(false);
    assert!(!evm_state::qualification::admit_growth(
        &prefix, &run, &sample, 100
    )?);
    prefix["binding"]["run_id"] = json!("another");
    assert!(evm_state::qualification::admit_growth(&prefix, &run, &sample, 100).is_err());
    Ok(())
}
