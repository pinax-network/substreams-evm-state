use anyhow::Result;
use evm_state::{
    capacity_qualification::{self, StressOptions},
    ch::ClickHouse,
    control::new_id,
};
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
