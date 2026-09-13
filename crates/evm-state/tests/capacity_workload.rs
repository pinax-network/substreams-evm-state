use anyhow::Result;
use evm_state::{
    capacity::Meter,
    capacity_qualification::{self, StressOptions},
    ch::ClickHouse,
    control::new_id,
};

#[test]
#[ignore = "requires ClickHouse with query logging"]
fn growth_matches_compaction_destination_and_rejects_duplicate_writes() -> Result<()> {
    use evm_state::{
        ch::params,
        qualification::{compaction_query, generation_parts},
    };
    use serde_json::json;
    let admin = ClickHouse::new("default")?;
    let name = format!("evm_test_rust_{}", new_id());
    admin.execute(&format!("CREATE DATABASE {name}"), &Default::default())?;
    let client = admin.with_database(&name)?;
    let operation = (|| -> Result<()> {
        client.execute("CREATE TABLE bootstrap_storage (generation String, n UInt64) ENGINE=MergeTree PARTITION BY generation ORDER BY generation", &Default::default())?;
        client.execute("CREATE TABLE bootstrap_generations (generation String, manifest String) ENGINE=MergeTree PARTITION BY generation ORDER BY generation", &Default::default())?;
        let first = new_id();
        let second = new_id();
        let arguments = params(json!({"first":first,"second":second}))?;
        client.execute(
            "INSERT INTO bootstrap_storage SELECT {first:String},number FROM numbers(2)",
            &arguments,
        )?;
        client.execute("INSERT INTO bootstrap_storage SELECT {second:String},n FROM bootstrap_storage WHERE generation={first:String}", &arguments)?;
        client.execute("INSERT INTO bootstrap_generations SELECT {first:String},'first manifest' UNION ALL SELECT {second:String},'second manifest'", &arguments)?;
        admin.execute("SYSTEM FLUSH LOGS", &Default::default())?;
        let original = compaction_query(&client, &first, 2)?;
        let successor = compaction_query(&client, &second, 2)?;
        assert!(!original.is_null() && !successor.is_null());
        assert_ne!(original["query_id"], successor["query_id"]);
        assert!(compaction_query(&client, &first, 1).is_err());
        assert!(compaction_query(&client, "invalid-generation", 2).is_err());
        assert!(compaction_query(&client, &new_id(), 2)?.is_null());
        let measured = generation_parts(&client, &first)?;
        assert_eq!(measured["active_storage_rows"], 2);
        assert_eq!(measured["active_manifest_rows"], 1);
        assert!(measured["active_part_bytes"].as_u64().unwrap() > 0);
        assert_eq!(
            generation_parts(&client, &second)?["active_storage_rows"],
            2
        );
        assert_eq!(
            generation_parts(&client, &new_id())?["active_part_bytes"],
            0
        );

        // A real second write to the same destination remains an error.
        client.execute(
            "INSERT INTO bootstrap_storage SELECT {first:String},number FROM numbers(2)",
            &arguments,
        )?;
        admin.execute("SYSTEM FLUSH LOGS", &Default::default())?;
        assert!(compaction_query(&client, &first, 2).is_err());
        assert_eq!(generation_parts(&client, &first)?["active_storage_rows"], 4);
        Ok(())
    })();
    admin.execute(&format!("DROP DATABASE {name}"), &Default::default())?;
    operation
}
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
    let measured = Meter::new(&client, config.clone())?.sample()?;
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
    let inspected = evm_state::process::capture(
        Command::new("docker").args(["inspect", names[0]]),
        Duration::from_secs(30),
    )?
    .unwrap();
    assert!(inspected.status.success());
    let inspected: serde_json::Value = serde_json::from_slice(&inspected.stdout)?;
    let has_data_bind = inspected[0]["Mounts"]
        .as_array()
        .unwrap()
        .iter()
        .any(|m| m["Destination"] == "/var/lib/clickhouse" && m["Type"] == "bind");
    let mut host_config = config;
    host_config["data_scan_mode"] = json!("host_bind");
    let host_result = Meter::new(&client, host_config)?.sample();
    if has_data_bind {
        let host = host_result?;
        assert_eq!(host["data_scan_mode"], "host_bind");
        assert_eq!(
            host["host_bind_scan"]["fresh_probes_verified_before_and_after"],
            true
        );
        assert!(host["server_data_allocated_bytes"].as_u64().unwrap() >= parts);
        assert_eq!(
            host["accounted_allocated_bytes"].as_u64().unwrap(),
            host["server_data_allocated_bytes"].as_u64().unwrap()
                + host["local"]["allocated_bytes"].as_u64().unwrap()
        );
    } else {
        // The default CI service uses a named volume, which must not be
        // misrepresented as a readable directory on this process's host.
        assert!(host_result.is_err());
    }
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
