"""Do not turn absent telemetry or bad live samples into a fast/zero-work claim."""
import importlib.util
from pathlib import Path
import sys

import pytest

SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"


def script(name):
    spec = importlib.util.spec_from_file_location(name, SCRIPTS / (name + ".py"))
    value = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(value)
    return value


throughput = script("qualify_throughput")
sys.modules.setdefault("qualify_throughput", throughput)
summary = script("summarize_throughput")


def test_absent_progress_is_unknown_not_zero_work(tmp_path):
    path = tmp_path / "run.log"
    path.write_text('2026-09-12T09:00:00.000-0400 INFO substreams stream stats {"last_block":"None"}\n')
    result = throughput.sink_events(path)
    assert result["max_observed_running_jobs"] is None
    assert result["last_reported_processed_blocks"] is None


def test_log_summary_only_carries_declared_telemetry(tmp_path):
    path = tmp_path / "run.log"
    path.write_text('2026-09-12T09:00:00.000-0400 INFO substreams stream stats '
                    '{"progress_total_processed_blocks":0,"progress_running_jobs":{"stage 0":0},"credential":"private"}\n'
                    '2026-09-12T09:00:00.000-0400 INFO unrelated {"dsn":"private"}\n')
    result = throughput.sink_events(path)
    assert result["max_observed_running_jobs"] == result["last_reported_processed_blocks"] == 0
    assert "private" not in str(result)


def live_samples():
    return [{"started_at_unix_ns": i * 5_000_000_000, "rpc_finalized_block": i * 10 + 100,
             "durable_block": i * 10 + 95, "finalized_lag_blocks": 5,
             "durable_block_age_seconds": 3, "sample_duration_seconds": .1} for i in range(30)]


def test_live_window_excludes_startup_by_time_and_keeps_slow_samples():
    samples = live_samples()
    samples[0]["finalized_lag_blocks"] = 999
    samples[20]["finalized_lag_blocks"] = 100
    value = summary.live_window(samples, 60_000_000_000, 150_000_000_000)
    assert value["samples"] == 18
    assert value["finalized_lag_blocks"]["max"] == 100


@pytest.mark.parametrize("defect", ["failed", "missing", "stalled", "regressed"])
def test_live_summary_rejects_incomplete_or_nonfollowing_samples(defect):
    samples = live_samples()
    if defect == "failed":
        samples[15]["error_type"] = "TimeoutError"
    elif defect == "missing":
        del samples[15]["finalized_lag_blocks"]
    elif defect == "stalled":
        for sample in samples:
            sample["durable_block"] = 95
    else:
        samples[15]["durable_block"] = 0
    with pytest.raises(ValueError):
        summary.live_window(samples, 60_000_000_000, 150_000_000_000)
