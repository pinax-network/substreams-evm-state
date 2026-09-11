import json
import os
import signal
import subprocess
import sys

import pytest

from conftest import SPKG, native_dsn, native_env
from evm_state.checkpoint import build
from evm_state.cursor import load_progress, observe, save_progress
from evm_state.ingest import ingest, prepare, recover_cursor
from evm_state.proof import VerificationError
from native_stream import CURSORS, NativeStream, wait_until
from state_fixtures import A, CODE, block, insert_blocks, proof_bundle, state, word

pytestmark = pytest.mark.clickhouse


def planted(databases, tmp_path):
    client = databases(False)
    args = (client, SPKG, "http://127.0.0.1:1", [A], 100, tmp_path, native_dsn(client.database))
    run = prepare(*args)
    bundle = proof_bundle(101, {A: state()})
    rows = [block(n, bundle) for n in [100, 101]]
    insert_blocks(client, rows)
    return client, args, run


@pytest.mark.parametrize("damage", ["missing", "empty", "truncated"])
def test_recover_cursor_from_verified_atomic_progress(databases, tmp_path, damage):
    client, args, run = planted(databases, tmp_path)
    saved = save_progress(client, run, tmp_path, CURSORS["1"]["101"])
    if damage != "missing":
        (tmp_path / "cursor.txt").write_text("" if damage == "empty" else "truncated")
    assert recover_cursor(*args)["position"] == saved["position"]
    assert (tmp_path / "cursor.txt").read_text() == CURSORS["1"]["101"]
    # A completed bounded run is idempotent and need not contact the endpoint.
    assert ingest(*args, stop_block=102)["already_complete"]


@pytest.mark.parametrize("damage", ["missing", "identity", "position", "cursor", "block", "marker"])
def test_recovery_rejects_missing_mismatched_or_incomplete_backup(databases, tmp_path, damage):
    client, args, run = planted(databases, tmp_path)
    saved = save_progress(client, run, tmp_path, CURSORS["1"]["101"])
    path = tmp_path / "durable_progress.json"
    if damage == "missing": path.unlink()
    elif damage in {"identity", "position", "cursor"}:
        saved[{"identity": "run_id", "position": "position", "cursor": "cursor"}[damage]] = "wrong"
        path.write_text(json.dumps(saved))
    elif damage == "block": client.execute("TRUNCATE TABLE state_blocks")
    else: client.execute("TRUNCATE TABLE _blocks_")
    with pytest.raises(VerificationError):
        recover_cursor(*args)
    assert not (tmp_path / "cursor.txt").exists()


def test_cursor_ahead_of_data_or_behind_durable_progress_is_rejected(databases, tmp_path):
    client, _, run = planted(databases, tmp_path)
    with pytest.raises(VerificationError, match="complete block"):
        save_progress(client, run, tmp_path, CURSORS["1"]["102"])
    save_progress(client, run, tmp_path, CURSORS["1"]["101"])
    with pytest.raises(VerificationError, match="regressed"):
        save_progress(client, run, tmp_path, CURSORS["1"]["100"])


def test_torn_native_file_cannot_replace_last_verified_progress(databases, tmp_path):
    client, _, run = planted(databases, tmp_path)
    saved = save_progress(client, run, tmp_path, CURSORS["1"]["100"])
    (tmp_path / "cursor.txt").write_text("half-a-cursor")
    assert observe(client, run, tmp_path) is None
    assert load_progress(client, run, tmp_path) == saved


@pytest.mark.parametrize("backfill", [False, True], ids=["live", "spooled"])
def test_kill_torn_cursor_recovery_and_native_resume_publish_complete_state(databases, tmp_path, native_proxy, backfill):
    client, target = databases(False), databases(False)
    bundle = proof_bundle(103, {A: state({2: 9}, nonce=3, balance=50)})
    rows = [block(100, bundle, storage={(A, 1): 7, (A, 2): 8}, nonces={A: 3}, codes={A: CODE}),
            block(101, bundle, storage={(A, 1): 0, (A, 2): 9}), block(102, bundle, balances={A: 50}), block(103, bundle)]
    def hold(stream, context):
        while context.is_active() and not stream.closed.wait(0.02):
            pass
    stream = NativeStream(rows[:2], native_proxy, after_blocks=hold, backfill=backfill)
    args = (client, SPKG, stream.endpoint, [A], 100, tmp_path, native_dsn(client.database))
    run = prepare(*args, checkpoint_database=target.database)
    command = [sys.executable, "-m", "evm_state.cli", "--database", client.database, "ingest",
        "--package", str(SPKG), "--endpoint", stream.endpoint, "--accounts", A, "--start-block", "100",
        "--stop-block", "104", "--state-dir", str(tmp_path), "--max-retries", "0",
        "--checkpoint-database", target.database, "--decode-batch-size", "1"]
    process = None
    try:
        with (tmp_path / "crash.log").open("w") as log:
            process = subprocess.Popen(command, env=native_env(client.database), start_new_session=True,
                                       stdout=log, stderr=subprocess.STDOUT)
            def saved():
                path = tmp_path / "durable_progress.json"
                return path.exists() and json.loads(path.read_text())["position"]["block"]["number"] == 101
            wait_until(saved, process)
            os.killpg(process.pid, signal.SIGKILL)
            process.wait(timeout=10)
            (tmp_path / "cursor.txt").write_text("truncated after process crash")
            expected = CURSORS["17" if backfill else "1"]["101"]
            recover_cursor(*args, checkpoint_database=target.database)
            assert (tmp_path / "cursor.txt").read_text() == expected
            stream.blocks = rows
            stream.after_blocks = None
            result = subprocess.run(command, env=native_env(client.database), stdout=log,
                                    stderr=subprocess.STDOUT, timeout=30)
            assert result.returncode == 0, (tmp_path / "crash.log").read_text()[-5000:]
            assert stream.requests[-1].start_cursor == expected
            assert not stream.errors, stream.errors
            source = {key: run["identity"][key] for key in ["database", "accounts", "start_block", "module_hash", "final_blocks_only"]}
            ready = build(target, bundle, [source])
            assert ready["nonzero_slots"] == 1
            assert client.one("SELECT count() AS n FROM state_blocks FINAL")["n"] == 4
            assert load_progress(client, run, tmp_path)["position"]["block"]["number"] == 103
    finally:
        if process is not None:
            try: os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError: pass
            process.wait(timeout=10)
        stream.close()
