import os
from pathlib import Path
import subprocess
import uuid

import pytest

from evm_state.postgres import snapshot, verify
from evm_state.proof import keccak256
from state_fixtures import A, CODE, proof_bundle, state, word

pytestmark = pytest.mark.postgres


@pytest.fixture
def database(monkeypatch):
    # Never alter existing baseline tables. Every read and write in this fixture
    # uses its own schema, including subprocesses invoked by the production code.
    schema = "evm_test_" + uuid.uuid4().hex
    dsn = os.environ.get("PG_DSN", "postgresql://dev-node:insecure-change-me-in-prod@localhost:5432/dev-node")
    command = ["psql", "-XAtq", "--set=ON_ERROR_STOP=1", "--dbname", dsn]
    def sql(value):
        result = subprocess.run(command, input=value, capture_output=True, text=True, timeout=30)
        assert result.returncode == 0, result.stderr
        return result.stdout
    sql(f"CREATE SCHEMA {schema}")
    monkeypatch.setenv("PG_DSN", dsn)
    monkeypatch.setenv("PGOPTIONS", f"-c search_path={schema}")
    try:
        root = Path(__file__).resolve().parents[1]
        sql((root / "postgres/schema.0.blocks.sql").read_text() + (root / "postgres/schema.1.state.sql").read_text())
        bundle = proof_bundle(101, {A: state({1: 7})})
        code_hash = "0x" + keccak256(bytes.fromhex(CODE[2:])).hex()
        sql(f"""INSERT INTO blocks(block_num,block_hash,parent_hash,timestamp,state_root,coinbase,transaction_count)
            VALUES (101,'{word(101)}','{word(100)}',now(),'{bundle['header']['state_root']}','{A}',1);
            INSERT INTO code VALUES ('{code_hash}',decode('{CODE[2:]}','hex'),4,100);
            INSERT INTO accounts VALUES ('{A}',25,1,'{code_hash}',100,100,100,100);
            INSERT INTO storage VALUES ('{A}','{word(1)}','{word(7)}',100,1);""")
        yield sql, command, bundle
    finally:
        sql(f"DROP SCHEMA {schema} CASCADE")


def test_actual_postgres_snapshot_and_complete_verification(database):
    sql, _, bundle = database
    captured = snapshot(A)
    assert verify(captured, bundle, complete=True)["storage_slots_checked"] == 1
    sql("UPDATE accounts SET nonce=2")
    with pytest.raises(ValueError, match="nonce/balance mismatch"):
        verify(snapshot(A), bundle, complete=True)


def test_snapshot_never_mixes_committed_blocks_during_concurrent_updates(database, tmp_path):
    sql, command, _ = database
    sql(f"UPDATE accounts SET balance=101; UPDATE storage SET value='{word(101)}'")
    commands = []
    for number in range(102, 202):
        commands.append(f"""BEGIN;
            UPDATE blocks SET block_num={number},block_hash='{word(number)}';
            UPDATE accounts SET balance={number},block_num={number};
            UPDATE storage SET value='{word(number)}',block_num={number};
            COMMIT;
            SELECT pg_sleep(0.01);""")
    script = tmp_path / "writer.sql"
    script.write_text("\n".join(commands))
    writer = subprocess.Popen([*command, "--file", str(script)], stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
    seen = set()
    try:
        for _ in range(20):
            captured = snapshot(A)
            number = captured["header"]["number"]
            seen.add(number)
            assert int(captured["accounts"][0]["balance"]) == number
            assert int(captured["storage"][0]["value"], 16) == number
        _, errors = writer.communicate(timeout=20)
        assert writer.returncode == 0, errors
        assert len(seen) > 1
    finally:
        if writer.poll() is None:
            writer.kill()
            writer.communicate(timeout=10)
