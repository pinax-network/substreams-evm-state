"""Fault injection into a NEW container only; never restart the configured database."""
import json
import os
import socket
import subprocess
import sys
import time
import uuid

import pytest

from conftest import SPKG, native_env
from evm_state.ch import ClickHouse
from evm_state.checkpoint import build, read_account
from evm_state.ingest import recover_cursor
from evm_state.retention import prune
from native_stream import NativeStream
from state_fixtures import A, CODE, block, proof_bundle, state

pytestmark = [pytest.mark.clickhouse, pytest.mark.database_crash]


def docker(*args):
    return subprocess.run(["docker", *args], capture_output=True, text=True, check=True, timeout=60).stdout.strip()


def wait_database(client):
    deadline = time.monotonic() + 45
    while True:
        try:
            assert client.one("SELECT 1 AS n")["n"] == 1
            return
        except (OSError, RuntimeError):
            if time.monotonic() > deadline:
                raise
            time.sleep(0.2)


def test_database_hard_restart_preserves_publication_and_recovery_boundaries(tmp_path, native_proxy, monkeypatch):
    name = "evm-crash-" + uuid.uuid4().hex[:12]
    stream = None
    # Docker reassigns dynamically published ports after stop/start. Choose two
    # available ports once and publish them explicitly so run identity is stable.
    with socket.socket() as http_port, socket.socket() as native_port:
        http_port.bind(("127.0.0.1", 0)); native_port.bind(("127.0.0.1", 0))
        http_number, native_number = http_port.getsockname()[1], native_port.getsockname()[1]
    docker("run", "-d", "--name", name, "--memory", "2g", "--cpus", "2",
           "-p", f"127.0.0.1:{http_number}:8123", "-p", f"127.0.0.1:{native_number}:9000",
           "-e", "CLICKHOUSE_USER=evm_state", "-e", "CLICKHOUSE_PASSWORD=local-development-only",
           "-e", "CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1", "clickhouse/clickhouse-server:26.3.33.24")
    try:
        ports = json.loads(docker("inspect", "--format", "{{json .NetworkSettings.Ports}}", name))
        url = "http://127.0.0.1:" + ports["8123/tcp"][0]["HostPort"]
        dsn = "clickhouse://evm_state:local-development-only@127.0.0.1:" + ports["9000/tcp"][0]["HostPort"] + "/source"
        client, target = ClickHouse("source", url), ClickHouse("checkpoints", url)
        admin = ClickHouse("default", url)
        wait_database(admin)
        monkeypatch.setenv("EVM_STATE_HOME", str(tmp_path / "control"))
        initial = proof_bundle(101, {A: state({2: 9}, nonce=3)})
        rows = [block(100, initial, storage={(A, 1): 7, (A, 2): 8}, nonces={A: 3}, codes={A: CODE}),
                block(101, initial, storage={(A, 1): 0, (A, 2): 9})]
        stream = NativeStream(rows, native_proxy)
        directory = tmp_path / "native"
        command = [sys.executable, "-m", "evm_state.cli", "--database", client.database, "ingest", "--package", str(SPKG),
            "--endpoint", stream.endpoint, "--accounts", A, "--start-block", "100", "--state-dir", str(directory),
            "--checkpoint-database", target.database, "--decode-batch-size", "1", "--max-retries", "0"]
        env = dict(native_env(client.database), SUBSTREAMS_SINK_DSN=dsn, CH_HTTP_URL=url)
        with (tmp_path / "native.log").open("w") as log:
            result = subprocess.run(command + ["--stop-block", "102"], env=env, stdout=log, stderr=subprocess.STDOUT, timeout=30)
            assert result.returncode == 0, (tmp_path / "native.log").read_text()[-4000:]
            run = json.loads((directory / "run.json").read_text())
            source = {key: run["identity"][key] for key in ["database", "accounts", "start_block", "module_hash", "final_blocks_only"]}
            insert = target.insert
            def fail_before_publication(table, values, **kwargs):
                if table == "checkpoints":
                    docker("kill", "--signal", "KILL", name)
                    raise OSError("database stopped before publication")
                return insert(table, values, **kwargs)
            with monkeypatch.context() as fault:
                fault.setattr(target, "insert", fail_before_publication)
                with pytest.raises(OSError, match="before publication"):
                    build(target, initial, [source])
            docker("start", name)
            wait_database(admin)
            assert int(target.one("SELECT count() AS n FROM checkpoints")["n"]) == 0
            assert prune(target)["unpublished_candidates"]
            first = build(target, initial, [source])

            # A second hard database restart occurs after publication. The
            # acknowledged native rows, ready snapshot and local backup survive.
            docker("kill", "--signal", "KILL", name)
            (directory / "cursor.txt").write_text("damaged native cursor")
            docker("start", name)
            wait_database(admin)
            assert read_account(target, first["snapshot_id"], A)["nonzero_slots"] == 1
            recovered = recover_cursor(client, SPKG, stream.endpoint, [A], 100, directory, dsn, target.database)
            assert recovered["position"]["block"]["number"] == 101
            latest = proof_bundle(103, {A: state({2: 9}, nonce=3, balance=50)})
            stream.blocks = rows + [block(102, latest, balances={A: 50}), block(103, latest)]
            result = subprocess.run(command + ["--stop-block", "104"], env=env, stdout=log, stderr=subprocess.STDOUT, timeout=30)
            assert result.returncode == 0, (tmp_path / "native.log").read_text()[-4000:]
            second = build(target, latest, [{**source, "start_block": 102}], first["snapshot_id"])
            assert read_account(target, first["snapshot_id"], A)["balance"] == "25"
            assert read_account(target, second["snapshot_id"], A)["balance"] == "50"
            assert not stream.errors, stream.errors
    finally:
        if stream is not None:
            stream.close()
        docker("rm", "-f", name)
