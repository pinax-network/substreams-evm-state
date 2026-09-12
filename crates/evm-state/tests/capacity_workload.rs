use anyhow::Result;
use evm_state::{
    capacity::Meter,
    capacity_qualification::{self, StressOptions},
    ch::ClickHouse,
    control::new_id,
};
#[test]
#[ignore = "requires ClickHouse in a local published Docker container"]
fn real_directory_meter_accounts_for_allocated_server_data_and_local_workspace() -> Result<()> {
    use serde_json::json;
    use std::{process::Command, time::Duration};
    let root = tempfile::tempdir()?;
    let spool = root.path().join("native/spool");
    std::fs::create_dir_all(&spool)?;
    std::fs::write(spool.join("workspace-data"), vec![b'x'; 8192])?;
    let client = ClickHouse::new("default")?;
    let port = reqwest::Url::parse(&client.url)?
        .port_or_known_default()
        .unwrap();
    let mut command = Command::new("docker");
    command.args([
        "ps",
        "--filter",
        &format!("publish={port}"),
        "--format",
        "{{.ID}}",
    ]);
    let output = evm_state::process::capture(&mut command, Duration::from_secs(30))?.unwrap();
    assert!(output.status.success());
    let text = String::from_utf8(output.stdout)?;
    let names = text.lines().collect::<Vec<_>>();
    assert_eq!(
        names.len(),
        1,
        "requires one local published ClickHouse container"
    );
    let config = json!({"format_version":1,"clickhouse_container":names[0],"databases":["default"],"local_paths":[root.path()],"components":{"native":[spool.parent().unwrap()],"spool":[spool],"future_export":[root.path().join("export")]},"budget_bytes":100_000_000_000u64,"headroom_bytes":10_000_000_000u64,"min_free_bytes":0});
    let measured = Meter::new(&client, config)?.sample()?;
    assert_eq!(measured["admitted"], true);
    let server = measured["server_data_allocated_bytes"].as_u64().unwrap();
    assert!(server > 0);
    assert!(measured["local"]["logical_bytes"].as_u64().unwrap() >= 8192);
    assert_eq!(
        measured["local_components"]["native"]["logical_bytes"],
        8192
    );
    assert_eq!(measured["local_components"]["spool"]["logical_bytes"], 8192);
    assert_eq!(
        measured["local_components"]["future_export"]["allocated_bytes"],
        0
    );
    assert_eq!(
        measured["accounted_allocated_bytes"].as_u64().unwrap(),
        server + measured["local"]["allocated_bytes"].as_u64().unwrap()
    );
    assert!(measured["selected_database_parts"]
        .as_array()
        .unwrap()
        .iter()
        .all(|row| row["database"] == "default"));
    let parts: u64 = measured["selected_database_parts"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| evm_state::ch::uint(&v["bytes"]).unwrap())
        .sum();
    assert!(parts <= server);
    Ok(())
}
#[test]
#[ignore = "requires ClickHouse and pinned substreams CLI"]
fn real_synthetic_state_stress_rotates_pins_and_reverifies_export_restore() -> Result<()> {
    let root = tempfile::tempdir()?;
    let admin = ClickHouse::new("default")?;
    let options = StressOptions {
        prefix: format!("evm_test_rust_{}", new_id()),
        output: root.path().join("run"),
        package: std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../spkg/evm-state-v0.1.0.spkg"),
        accounts: 3,
        hot_slots: 8,
        quiet_slots: 2,
        merge_only_mib: 0,
        budget_bytes: 100_000_000_000,
    };
    let operation = (|| -> Result<()> {
        let result = capacity_qualification::measure(
            &admin,
            &options,
            "clickhouse://evm_state:local-development-only@localhost:19000/{database}",
        )?;
        assert_eq!(result["total_nonzero_slots"], 12);
        assert_eq!(result["cleared_and_replaced_slots"], 4);
        assert_eq!(result["initial_checksum"], result["restored_checksum"]);
        assert_ne!(result["initial_checksum"], result["updated_checksum"]);
        assert!(options.output.join("result.json").is_file());
        assert!(result["export_bytes"].as_u64().unwrap() > 0);
        assert!(capacity_qualification::measure(&admin, &options, "unused").is_err());
        Ok(())
    })();
    for suffix in ["_source", "_checkpoints", "_restored"] {
        admin.execute(
            &format!("DROP DATABASE IF EXISTS {}{suffix}", options.prefix),
            &Default::default(),
        )?;
    }
    operation
}
#[test]
#[ignore = "requires ClickHouse"]
fn real_merge_fixture_retains_rows_and_finishes_all_eight_parts() -> Result<()> {
    let root = tempfile::tempdir()?;
    let admin = ClickHouse::new("default")?;
    let options = StressOptions {
        prefix: format!("evm_test_rust_{}", new_id()),
        output: root.path().join("run"),
        package: "unused".into(),
        accounts: 1,
        hot_slots: 2,
        quiet_slots: 0,
        merge_only_mib: 1,
        budget_bytes: 100_000_000_000,
    };
    let operation = (|| -> Result<()> {
        let result = capacity_qualification::measure(&admin, &options, "")?;
        assert_eq!(result["rows"], 256);
        assert!(result["parts_bytes_after"].as_u64().unwrap() > 1_000_000);
        Ok(())
    })();
    admin.execute(
        &format!("DROP DATABASE IF EXISTS {}_checkpoints", options.prefix),
        &Default::default(),
    )?;
    operation
}
