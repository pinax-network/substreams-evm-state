import json
import subprocess
import urllib.parse

import pytest

from evm_state.capacity import Meter
from evm_state.checkpoint import build
from evm_state.export import export_checkpoint
from evm_state.importer import import_checkpoint
from export_fixtures import checkpoint
from state_fixtures import A, block, insert_blocks, proof_bundle, source, state
from test_capacity import config

pytestmark = pytest.mark.clickhouse


def test_real_clickhouse_directory_meter_includes_allocated_data_and_local_workspace(databases, tmp_path):
    target, _ = checkpoint(databases)
    port = urllib.parse.urlsplit(target.url).port
    found = subprocess.run(["docker", "ps", "--filter", f"publish={port}", "--format", "{{.ID}}"],
                           capture_output=True, text=True, check=True).stdout.splitlines()
    assert len(found) == 1, "integration test requires one local published ClickHouse container"
    settings = {**config(tmp_path), "clickhouse_container": found[0], "databases": [target.database],
                "budget_bytes": 100_000_000_000, "headroom_bytes": 10_000_000_000}
    (tmp_path / "workspace-data").write_bytes(b"x" * 8192)
    measured = Meter(target, settings).sample()
    assert measured["admitted"]
    assert measured["server_data_allocated_bytes"] > 0
    assert measured["local"]["logical_bytes"] >= 8192
    assert sum(row["bytes"] for row in measured["selected_database_parts"]) <= measured["server_data_allocated_bytes"]


@pytest.mark.parametrize("stage", ["checkpoint-trie", "checkpoint-publish"])
def test_capacity_failure_keeps_checkpoint_candidate_unpublished(databases, monkeypatch, stage):
    stream, target = databases(), databases(False)
    bundle = proof_bundle(101, {A: state({1: 7})})
    insert_blocks(stream, [block(100, bundle, storage={(A, 1): 7}), block(101, bundle)])
    declared = source(stream, 100, target=target)
    def guard(client, paths, current):
        if current == stage: raise ValueError("injected capacity exhaustion")
    monkeypatch.setattr("evm_state.checkpoint.capacity_check", guard)
    with pytest.raises(ValueError, match="capacity exhaustion"):
        build(target, bundle, [declared])
    assert int(target.one("SELECT count() AS n FROM checkpoints")["n"]) == 0
    assert int(target.one("SELECT count() AS n FROM checkpoint_storage")["n"]) == 1


def test_capacity_failure_prevents_export_and_restore_publication(databases, tmp_path, monkeypatch):
    target, ready = checkpoint(databases)
    directory = tmp_path / "export"
    def guard(client, paths, stage):
        if stage in {"export-publish", "import-publish"}: raise ValueError("injected capacity exhaustion")
    with monkeypatch.context() as patch:
        patch.setattr("evm_state.export.capacity_check", guard)
        with pytest.raises(ValueError, match="capacity exhaustion"):
            export_checkpoint(target, ready["snapshot_id"], directory)
    assert not (directory / "manifest.json").exists()
    complete = tmp_path / "complete"
    export_checkpoint(target, ready["snapshot_id"], complete)
    restored = databases(False)
    monkeypatch.setattr("evm_state.importer.capacity_check", guard)
    with pytest.raises(ValueError, match="capacity exhaustion"):
        import_checkpoint(restored, complete)
    assert int(restored.one("SELECT count() AS n FROM checkpoints")["n"]) == 0
    assert target.one("SELECT snapshot_id FROM checkpoints")["snapshot_id"] == ready["snapshot_id"]


def test_capacity_rejection_before_bootstrap_commit_preserves_raw_recovery_input(databases, monkeypatch):
    from evm_state.bootstrap import compact
    from test_bootstrap import initial
    stream, target, _, directory = initial(databases)
    def guard(client, paths, stage):
        if stage == "bootstrap-commit": raise ValueError("injected capacity exhaustion")
    with monkeypatch.context() as changes:
        changes.setattr("evm_state.bootstrap.capacity_check", guard)
        with pytest.raises(ValueError, match="capacity exhaustion"):
            compact(stream, directory)
    assert not (directory / "bootstrap.json").exists()
    assert int(stream.one("SELECT count() AS n FROM state_blocks")["n"]) == 2
    assert int(target.one("SELECT count() AS n FROM checkpoints")["n"]) == 0
    assert compact(stream, directory)["nonzero_slots"] == 2


@pytest.mark.parametrize("after_progress", [False, True], ids=["first-batch", "durable-cursor"])
def test_capacity_stopped_native_sink_resumes(databases, tmp_path, native_proxy, monkeypatch, after_progress):
    import os
    from conftest import SPKG, native_dsn
    from evm_state.ingest import ingest, prepare
    from evm_state.cursor import load_progress
    from native_stream import NativeStream
    for name in os.environ:
        if name.startswith("SUBSTREAMS_"): monkeypatch.delenv(name)
    stream, target = databases(False), databases(False)
    bundle = proof_bundle(103, {A: state({1: 7}, balance=50)})
    rows = [block(100, bundle, storage={(A, 1): 7}), block(101, bundle),
            block(102, bundle, balances={A: 50}), block(103, bundle)]
    def hold(server, context):
        while context.is_active() and not server.closed.wait(0.02): pass
    server = NativeStream(rows[:2], native_proxy, after_blocks=hold, backfill=True)
    args = (stream, SPKG, server.endpoint, [A], 100, tmp_path / "native", native_dsn(stream.database))
    run = prepare(*args, checkpoint_database=target.database)
    def guard(client, paths, stage):
        progress = tmp_path / "native/durable_progress.json"
        ready = progress.exists() and json.loads(progress.read_text())["position"]["block"]["number"] == 101
        if stage == "native-progress" and (not after_progress or ready):
            raise ValueError("injected capacity exhaustion")
    try:
        with monkeypatch.context() as changes:
            changes.setattr("evm_state.ingest.capacity_check", guard)
            with pytest.raises(ValueError, match="capacity exhaustion"):
                ingest(*args, stop_block=104, checkpoint_database=target.database, decode_batch_size=1)
        if after_progress:
            assert load_progress(stream, run, tmp_path / "native")["position"]["block"]["number"] == 101
        server.blocks, server.after_blocks = rows, None
        result = ingest(*args, stop_block=104, checkpoint_database=target.database, decode_batch_size=1)
        assert build(target, bundle, [result["source"]])["nonzero_slots"] == 1
        assert not server.errors
    finally:
        server.close()
