use anyhow::Result;
use evm_state::{
    files::atomic_json,
    throughput_qualification::{distribution, live_window, sink_events, summarize},
};
use serde_json::{json, Value};
use std::{fs, path::Path};

fn samples() -> Vec<Value> {
    (0..30u64).map(|i| json!({"started_at_unix_ns":i*5_000_000_000,"rpc_finalized_block":i*10+100,
        "durable_block":i*10+95,"finalized_lag_blocks":5,"durable_block_age_seconds":3,"sample_duration_seconds":0.1})).collect()
}
#[test]
fn missing_telemetry_stays_unknown_and_only_declared_fields_escape_logs() -> Result<()> {
    let root = tempfile::tempdir()?;
    let path = root.path().join("run.log");
    fs::write(
        &path,
        "date INFO substreams stream stats {\"last_block\":\"None\"}\n",
    )?;
    let observed = sink_events(&path)?;
    assert!(observed["max_observed_running_jobs"].is_null());
    assert!(observed["last_reported_processed_blocks"].is_null());
    fs::write(&path, "date INFO substreams stream stats {\"progress_total_processed_blocks\":0,\"progress_running_jobs\":{\"stage 0\":0},\"credential\":\"private\"}\ndate INFO unrelated {\"dsn\":\"private\"}\n")?;
    let observed = sink_events(&path)?;
    assert_eq!(observed["max_observed_running_jobs"], 0);
    assert_eq!(observed["last_reported_processed_blocks"], 0);
    assert!(!observed.to_string().contains("private"));
    fs::write(
        &path,
        "date INFO substreams stream stats {\"progress_running_jobs\":{\"stage 0\":-1}}\n",
    )?;
    assert!(sink_events(&path).is_err());
    Ok(())
}
#[test]
fn fixed_startup_exclusion_keeps_slow_samples_and_sampling_skew() -> Result<()> {
    let mut data = samples();
    data[0]["finalized_lag_blocks"] = json!(999);
    data[20]["finalized_lag_blocks"] = json!(100);
    data[21]["finalized_lag_blocks"] = json!(-1);
    let result = live_window(&data, 60_000_000_000, 150_000_000_000)?;
    assert_eq!(result["samples"], 18);
    assert_eq!(result["finalized_lag_blocks"]["max"], 100.0);
    assert_eq!(result["finalized_lag_blocks"]["min"], -1.0);
    assert!(distribution(&[])?.is_null());
    assert!(distribution(&[f64::NAN]).is_err());
    Ok(())
}
#[test]
fn failed_incomplete_stalled_regressed_or_unordered_live_observations_fail() {
    for defect in [
        "failed",
        "missing",
        "stalled",
        "regressed",
        "time",
        "duration",
        "too-few",
        "overlap",
    ] {
        let mut data = samples();
        match defect {
            "failed" => data[15]["error_type"] = json!("Timeout"),
            "missing" => {
                data[15]
                    .as_object_mut()
                    .unwrap()
                    .remove("finalized_lag_blocks");
            }
            "stalled" => {
                for row in &mut data {
                    row["durable_block"] = json!(95);
                }
            }
            "regressed" => data[15]["durable_block"] = json!(0),
            "time" => data[15]["started_at_unix_ns"] = json!(60_000_000_000u64),
            "duration" => data[15]["sample_duration_seconds"] = json!(-1),
            "too-few" => data.truncate(16),
            _ => {}
        }
        assert!(
            live_window(
                &data,
                60_000_000_000,
                if defect == "overlap" {
                    1
                } else {
                    150_000_000_000
                }
            )
            .is_err(),
            "{defect}"
        );
    }
}
fn workload(root: &Path) -> Result<()> {
    for phase in ["cold", "warm", "live", "live-tuned"] {
        let dir = root.join(phase);
        fs::create_dir_all(dir.join("capacity"))?;
        let common = json!({"database":phase,"run_id":phase,"module_hash":"module","package_sha256":"package","accounts":["a"],"start_block":11,"blocks":4});
        let mut run = common.clone();
        run.as_object_mut().unwrap().extend(json!({"failed_lag_samples":0,"lag_samples":30,"stop_block_exclusive":15,"started_at_unix_ns":0,"finished_at_unix_ns":150_000_000_000u64}).as_object().unwrap().clone());
        let mut output = common;
        output.as_object_mut().unwrap().extend(
            json!({"end_block":14,"ordered_output_sha256":"digest","logical_protobuf_bytes":123})
                .as_object()
                .unwrap()
                .clone(),
        );
        atomic_json(&dir.join("result.json"), &run, false)?;
        atomic_json(&dir.join("output.json"), &output, false)?;
        atomic_json(
            &dir.join("capacity/summary.json"),
            &json!({"status":"completed","samples":1,"guard_samples":1,"failed_samples":0,"peak_observed_allocated_bytes":400,"maximum_sample_gap_ns":1,"rejected_guard_stages":[],"config":{"budget_bytes":1000,"headroom_bytes":100,"min_free_bytes":1}}),
            false,
        )?;
        fs::write(
            dir.join("capacity/samples.jsonl"),
            "{\"local_components\":{\"spool\":{\"allocated_bytes\":1}}}\n",
        )?;
        fs::write(
            dir.join("lag-samples.jsonl"),
            samples()
                .iter()
                .map(|v| format!("{v}\n"))
                .collect::<String>(),
        )?;
        fs::write(
            dir.join("run.log"),
            format!(
                "date INFO substreams stream stats {}\n",
                json!({"progress_total_processed_blocks":if phase == "cold" {4} else {0},"progress_running_jobs":{"stage 0":if phase == "cold" {1} else {0}}})
            ),
        )?;
    }
    for file in ["quiet-reads.json", "large-reads.json"] {
        atomic_json(
            &root.join(file),
            &json!({"calls":[],"sample":"retained"}),
            false,
        )?;
    }
    atomic_json(
        &root.join("cache-design.json"),
        &json!({"sentinel":"declared"}),
        false,
    )?;
    Ok(())
}
#[test]
fn full_summary_binds_runs_outputs_raw_samples_and_cache_telemetry() -> Result<()> {
    let temp = tempfile::tempdir()?;
    workload(temp.path())?;
    let result = summarize(temp.path(), &temp.path().join("summary.json"))?;
    assert_eq!(
        result["runs"]["cold"]["native_log"]["last_reported_processed_blocks"],
        4
    );
    assert_eq!(result["artifact_sha256"].as_object().unwrap().len(), 27);
    assert_eq!(result["overlapping_live_window"]["live"]["samples"], 18);
    assert!(result["reads"]["large-reads.json"].get("calls").is_none());
    assert!(summarize(temp.path(), &temp.path().join("summary.json")).is_err());
    Ok(())
}
#[test]
fn damaged_full_evidence_never_publishes_summary() -> Result<()> {
    for defect in [
        "output-identity",
        "output-digest",
        "output-end",
        "capacity",
        "raw-failure",
        "raw-missing",
        "missing-progress",
        "warm-work",
        "no-cold-jobs",
    ] {
        let temp = tempfile::tempdir()?;
        workload(temp.path())?;
        let root = temp.path();
        let file = match defect {
            "output-identity" | "output-digest" | "output-end" => "warm/output.json",
            "capacity" => "warm/capacity/summary.json",
            _ => "",
        };
        if !file.is_empty() {
            let mut value: Value = serde_json::from_slice(&fs::read(root.join(file))?)?;
            match defect {
                "output-identity" => value["module_hash"] = json!("wrong"),
                "output-digest" => value["ordered_output_sha256"] = json!("wrong"),
                "output-end" => value["end_block"] = json!(15),
                "capacity" => value["failed_samples"] = json!(1),
                _ => unreachable!(),
            }
            atomic_json(&root.join(file), &value, true)?;
        } else if defect.starts_with("raw-") {
            let mut rows = samples();
            if defect == "raw-failure" {
                rows[0]["error_type"] = json!("failed");
            } else {
                rows.pop();
            }
            fs::write(
                root.join("cold/lag-samples.jsonl"),
                rows.iter().map(|r| format!("{r}\n")).collect::<String>(),
            )?;
        } else {
            let phase = if defect == "no-cold-jobs" {
                "cold"
            } else {
                "warm"
            };
            let stats = if defect == "missing-progress" {
                json!({})
            } else {
                json!({"progress_total_processed_blocks":4,"progress_running_jobs":{"stage 0":0}})
            };
            fs::write(
                root.join(phase).join("run.log"),
                format!("date INFO substreams stream stats {stats}\n"),
            )?;
        }
        let output = root.join("summary.json");
        assert!(summarize(root, &output).is_err(), "{defect}");
        assert!(!output.exists());
    }
    Ok(())
}

#[test]
fn throughput_ranges_reject_unbounded_future_conflicting_and_overflowing_work() -> Result<()> {
    use evm_state::throughput_qualification::ThroughputOptions;
    let base = ThroughputOptions {
        database: "test".into(),
        root: "run".into(),
        package: "package".into(),
        accounts: "a".into(),
        endpoint: "endpoint".into(),
        start_block: Some(100),
        stop_block: Some(200),
        live_blocks: None,
        interval: 5.,
        timeout: 60,
        decode_batch_size: 1,
        spool_max_idle_ms: 100,
    };
    assert_eq!(base.range(199)?, (100, 200));
    for defect in [
        "future",
        "empty",
        "large",
        "interval",
        "infinite",
        "timeout",
        "mixed",
        "live-large",
        "overflow",
        "short",
    ] {
        let mut options = base.clone();
        let mut finalized = 199;
        match defect {
            "future" => options.stop_block = Some(201),
            "empty" => options.stop_block = Some(100),
            "large" => {
                options.stop_block = Some(100102);
                finalized = 200000;
            }
            "interval" => options.interval = 0.,
            "infinite" => options.interval = f64::INFINITY,
            "timeout" => options.timeout = 0,
            "mixed" => options.live_blocks = Some(1),
            "live-large" | "overflow" | "short" => {
                options.start_block = None;
                options.stop_block = None;
                options.live_blocks = Some(if defect == "live-large" { 10001 } else { 1 });
                finalized = if defect == "overflow" { u64::MAX } else { 99 };
            }
            _ => unreachable!(),
        }
        assert!(options.range(finalized).is_err(), "{defect}");
    }
    let mut live = base;
    live.start_block = None;
    live.stop_block = None;
    live.live_blocks = Some(2000);
    assert_eq!(live.range(5000)?, (4900, 7001));
    Ok(())
}
