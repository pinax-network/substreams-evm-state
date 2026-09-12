//! Stable OS identity; explicit compatibility with existing hostname attestations.
use crate::files::{canonical_json, resolve};
use anyhow::{ensure, Context, Result};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    fs,
    path::Path,
    process::{Command, Stdio},
    time::Duration,
};
use wait_timeout::ChildExt;

pub fn from_os_value(system: &str, value: &str) -> Result<String> {
    let value = value.trim().to_ascii_lowercase();
    let valid = match system {
        "Linux" => value.len() == 32 && value.bytes().all(|v| v.is_ascii_hexdigit()),
        "Darwin" => {
            value.len() == 36
                && value.bytes().enumerate().all(|(i, v)| {
                    if [8, 13, 18, 23].contains(&i) {
                        v == b'-'
                    } else {
                        v.is_ascii_hexdigit()
                    }
                })
        }
        _ => false,
    };
    ensure!(
        valid && value.bytes().any(|v| v != b'0' && v != b'-'),
        "persistent local machine identity is unavailable"
    );
    Ok(format!(
        "machine-sha256:{}",
        hex::encode(Sha256::digest(format!("{system}:{value}")))
    ))
}

pub fn machine_id() -> Result<String> {
    if cfg!(target_os = "linux") {
        return from_os_value("Linux", &fs::read_to_string("/etc/machine-id")?);
    }
    ensure!(
        cfg!(target_os = "macos"),
        "persistent local machine identity is unavailable"
    );
    let mut child = Command::new("/usr/sbin/ioreg")
        .args(["-rd1", "-c", "IOPlatformExpertDevice"])
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()?;
    if child.wait_timeout(Duration::from_secs(10))?.is_none() {
        let _ = child.kill();
        let _ = child.wait();
        anyhow::bail!("persistent local machine identity is unavailable");
    }
    let output = child.wait_with_output()?;
    ensure!(
        output.status.success(),
        "persistent local machine identity is unavailable"
    );
    let text = String::from_utf8(output.stdout)?;
    let raw = text
        .lines()
        .find_map(|line| {
            let (_, right) = line.split_once("\"IOPlatformUUID\"")?;
            let (_, right) = right.split_once('=')?;
            right
                .trim()
                .strip_prefix('"')?
                .split_once('"')
                .map(|(value, _)| value)
        })
        .context("persistent local machine identity is unavailable")?;
    from_os_value("Darwin", raw)
}

pub fn recovery_record_for(record: &Value, directory: &Path, machine: &str) -> Result<Value> {
    Ok(
        json!({"format_version":1,"original_host":crate::proof::string(record,"host")?,
        "record_sha256":hex::encode(Sha256::digest(canonical_json(record)?.as_bytes())),
        "directory":resolve(directory)?.to_str().context("invalid controller path")?,"machine_id":machine}),
    )
}

pub fn matches_for(
    record: &Value,
    directory: &Path,
    machine: &str,
    hostname: &str,
) -> Result<bool> {
    let Some(expected) = record
        .get("host")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
    else {
        return Ok(false);
    };
    if expected.starts_with("machine-sha256:") {
        return Ok(expected == machine);
    }
    let recovery = directory.join("host-rebinding.json");
    if recovery.exists() {
        let actual = fs::read(&recovery)
            .ok()
            .and_then(|v| serde_json::from_slice::<Value>(&v).ok());
        return Ok(actual == Some(recovery_record_for(record, directory, machine)?));
    }
    Ok(expected == hostname)
}

pub fn matches(record: &Value, directory: &Path) -> Result<bool> {
    matches_for(
        record,
        directory,
        &machine_id()?,
        hostname::get()?.to_str().context("invalid hostname")?,
    )
}
