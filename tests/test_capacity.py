import json
import os
from pathlib import Path
import sys
import time

import pytest

from evm_state import capacity
from evm_state.files import exclusive_lock


def config(root):
    return {"format_version": 1, "clickhouse_container": "qualified-clickhouse", "local_paths": [str(root)],
            "databases": ["checkpoints"], "budget_bytes": 1_000_000_000, "headroom_bytes": 100_000_000,
            "min_free_bytes": 0}


class Client:
    database, url, user, password = "checkpoints", "http://127.0.0.1:18123", "user", "dummy-password"
    def rows(self, sql):
        if "system.disks" in sql:
            return iter([{"name": "default", "path": "/var/lib/clickhouse/", "type": "Local", "is_remote": 0,
                          "total_space": 1000000000, "free_space": 900000000, "unreserved_space": 800000000}])
        if "system.parts" in sql:
            return iter([{"database": "checkpoints", "active": 1, "bytes": 100},
                         {"database": "other", "active": 1, "bytes": 200}])
        if "system.detached_parts" in sql:
            return iter([{"database": "checkpoints", "bytes": 50}])
        if "system.merges" in sql:
            return iter([{"database": "checkpoints", "input_bytes": 80}])
        raise AssertionError(sql)


@pytest.fixture
def mocked_meter(tmp_path, monkeypatch):
    info = {"Id": "owned-container", "State": {"Running": True}, "NetworkSettings": {
        "Ports": {"8123/tcp": [{"HostIp": "127.0.0.1", "HostPort": "18123"}]}},
        "Mounts": [{"Type": "volume", "Destination": "/var/lib/clickhouse"}]}
    def docker(args, **kwargs):
        if args[0] == "inspect": return json.dumps([info]).encode()
        assert args[:3] == ["exec", "owned-container", "du"]
        return b"600\t/var/lib/clickhouse\x00600\ttotal\x00"
    monkeypatch.setattr(capacity, "_docker", docker)
    client = Client()
    monkeypatch.setattr(capacity, "ClickHouse", lambda *args: client)
    return capacity.Meter(client, config(tmp_path)), info


def test_whole_directory_measurement_includes_more_than_table_parts(mocked_meter, tmp_path):
    meter, _ = mocked_meter
    (tmp_path / "spool").write_bytes(b"a" * 100)
    result = meter.sample()
    assert result["server_data_allocated_bytes"] == 600
    assert result["accounted_allocated_bytes"] == 600 + result["local"]["allocated_bytes"]
    assert result["selected_database_parts"] == [{"database": "checkpoints", "active": 1, "bytes": 100}]
    assert result["selected_database_detached_parts"][0]["bytes"] == 50
    assert result["selected_database_merges"][0]["input_bytes"] == 80
    assert result["local"]["logical_bytes"] == 100
    assert result["admitted"]


def test_component_breakdown_does_not_double_count_the_measured_total(mocked_meter, tmp_path):
    meter, _ = mocked_meter
    spool = tmp_path / "native/spool"
    spool.mkdir(parents=True)
    (spool / "segment").write_bytes(b"x" * 8192)
    meter.components = {"native": [spool.parent], "spool": [spool], "future_export": [tmp_path / "export"]}
    result = meter.sample()
    assert result["local_components"]["native"]["logical_bytes"] == 8192
    assert result["local_components"]["spool"]["logical_bytes"] == 8192
    assert result["local_components"]["future_export"]["allocated_bytes"] == 0
    assert result["accounted_allocated_bytes"] == 600 + result["local"]["allocated_bytes"]


@pytest.mark.parametrize("defect", ["port", "stopped", "replacement", "unmounted", "remote_endpoint"])
def test_wrong_or_changed_capacity_target_fails_closed(mocked_meter, defect):
    meter, info = mocked_meter
    if defect == "port": info["NetworkSettings"]["Ports"]["8123/tcp"][0]["HostPort"] = "9999"
    elif defect == "stopped": info["State"]["Running"] = False
    elif defect == "replacement": info["Id"] = "another-container"
    elif defect == "unmounted": info["Mounts"] = []
    else: meter.client.url = "http://remote.example.invalid:18123"
    with pytest.raises(ValueError):
        meter.sample()


def test_budget_and_disk_headroom_are_independent(mocked_meter):
    meter, _ = mocked_meter
    meter.budget, meter.headroom, meter.min_free = 500, 100, 900000000
    result = meter.sample()
    assert not result["admitted"]
    assert result["reasons"] == ["budget_headroom_exhausted", "filesystem_free_space_below_floor"]


def test_nonatomic_disk_counters_use_the_lower_reading(mocked_meter):
    meter, _ = mocked_meter
    rows = meter.client.rows
    def skewed(sql):
        values = list(rows(sql))
        if "system.disks" in sql:
            values[0].update(free_space=799_995_904, unreserved_space=800_000_000)
        return iter(values)
    meter.client.rows = skewed
    meter.min_free = 800_000_000
    result = meter.sample()
    assert result["reasons"] == ["filesystem_free_space_below_floor"]
    meter.min_free = 799_995_904
    assert meter.sample()["admitted"]


def test_invalid_disk_counter_still_fails_measurement(mocked_meter):
    meter, _ = mocked_meter
    rows = meter.client.rows
    def invalid(sql):
        values = list(rows(sql))
        if "system.disks" in sql:
            values[0]["unreserved_space"] = -1
        return iter(values)
    meter.client.rows = invalid
    with pytest.raises(ValueError, match="invalid ClickHouse disk capacity counters"):
        meter.sample()


def test_local_measurement_deduplicates_nested_roots_and_hard_links(tmp_path):
    nested = tmp_path / "nested"
    nested.mkdir()
    (nested / "data").write_bytes(b"hello")
    os.link(nested / "data", tmp_path / "hardlink")
    result = capacity.local_usage([tmp_path, nested])
    assert result["logical_bytes"] == 5
    assert result["files"] == 1
    assert result["allocated_bytes"] >= (nested / "data").stat().st_blocks * 512


def test_symlinks_cannot_hide_unmeasured_local_storage(tmp_path):
    (tmp_path / "outside").symlink_to("/not/a/declared/root")
    with pytest.raises(ValueError, match="symlink"):
        capacity.local_usage([tmp_path])


class ProcessMeter:
    def __init__(self, root, admitted=lambda: True):
        self.paths, self.config, self.admitted = [root], config(root), admitted
    def sample(self):
        allowed = self.admitted()
        now = time.time_ns()
        return {"sample_finished_ns": now, "accounted_allocated_bytes": 123,
                "admitted": allowed, "reasons": [] if allowed else ["budget_headroom_exhausted"]}


def test_capacity_supervisor_records_exit_and_passes_frozen_policy(tmp_path):
    marker = tmp_path / "seen-policy"
    code = "import os,pathlib; pathlib.Path(" + repr(str(marker)) + ").write_text(os.environ['EVM_STATE_CAPACITY_CONFIG'])"
    report = capacity.supervise(ProcessMeter(tmp_path), [sys.executable, "-c", code], tmp_path / "run", 0.1)
    assert report["status"] == "completed"
    assert report["command_exit_code"] == 0
    assert report["samples"] >= 2
    assert marker.read_text() == str(tmp_path / "run/config.json")
    assert json.loads((tmp_path / "run/summary.json").read_text()) == report


def test_unsafe_initial_sample_never_starts_a_command(tmp_path):
    marker = tmp_path / "should-not-exist"
    code = "import pathlib; pathlib.Path(" + repr(str(marker)) + ").touch()"
    report = capacity.supervise(ProcessMeter(tmp_path, lambda: False), [sys.executable, "-c", code], tmp_path / "run", 0.1)
    assert report["status"] == "stopped"
    assert report["command_exit_code"] is None
    assert not marker.exists()


def test_budget_stop_kills_child_that_ignores_sigterm_and_releases_its_lock(tmp_path):
    marker, lock = tmp_path / "child-ready", tmp_path / "child.lock"
    code = f"""import fcntl,os,pathlib,signal,time
if os.fork() == 0:
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    with open({str(lock)!r}, 'w') as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        pathlib.Path({str(marker)!r}).touch()
        time.sleep(60)
else:
    time.sleep(60)
"""
    report = capacity.supervise(ProcessMeter(tmp_path, lambda: not marker.exists()),
                               [sys.executable, "-c", code], tmp_path / "run", 0.1)
    assert report["status"] == "stopped"
    assert report["stop_reasons"] == ["budget_headroom_exhausted"]
    deadline = time.monotonic() + 5
    while True:
        try:
            with exclusive_lock(lock): break
        except ValueError:
            if time.monotonic() >= deadline: raise
            time.sleep(0.01)


def test_incomplete_sample_stops_instead_of_counting_missing_data_as_zero(tmp_path):
    meter = ProcessMeter(tmp_path)
    measured = meter.sample
    calls = 0
    def incomplete():
        nonlocal calls
        calls += 1
        if calls > 1: raise OSError("unreadable capacity directory")
        return measured()
    meter.sample = incomplete
    report = capacity.supervise(meter, [sys.executable, "-c", "import time; time.sleep(60)"], tmp_path / "run", 0.1)
    assert report["status"] == "stopped"
    assert report["failed_samples"] == 1
    assert report["stop_reasons"] == ["incomplete_capacity_sample"]


def test_guard_rejects_work_outside_declared_roots_and_records_peak(tmp_path, monkeypatch):
    meter = ProcessMeter(tmp_path)
    policy = tmp_path / "config.json"
    policy.write_text(json.dumps(meter.config))
    events = tmp_path / "guards"
    events.mkdir()
    monkeypatch.setenv("EVM_STATE_CAPACITY_CONFIG", str(policy))
    monkeypatch.setenv("EVM_STATE_CAPACITY_EVENTS", str(events))
    monkeypatch.setattr(capacity, "Meter", lambda *args: meter)
    with pytest.raises(ValueError, match="outside"):
        capacity.check(Client(), [tmp_path.parent / "untracked"])
    capacity.check(Client(), [tmp_path / "future-work"], "test-guard")
    event = json.loads(next(events.glob("*.json")).read_text())
    assert event["stage"] == "test-guard"
    assert event["accounted_allocated_bytes"] == 123


def test_finished_child_with_rejected_final_sample_still_writes_an_honest_report(tmp_path, monkeypatch):
    marker = tmp_path / "completed"
    code = "import pathlib; pathlib.Path(" + repr(str(marker)) + ").touch()"
    def denied(*args):
        raise PermissionError("cannot confirm process group state")
    monkeypatch.setattr(capacity.os, "killpg", denied)
    report = capacity.supervise(ProcessMeter(tmp_path, lambda: not marker.exists()),
        [sys.executable, "-c", code], tmp_path / "run", 1)
    assert report["status"] == "stopped"
    assert report["command_exit_code"] == 0
    assert report["termination_error"] == "could_not_confirm_process_group_termination"
