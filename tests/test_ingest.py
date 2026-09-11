import json

import pytest

from conftest import SPKG, native_dsn
from evm_state.ingest import ingest, prepare
from evm_state.files import exclusive_lock
from evm_state.proof import VerificationError
from state_fixtures import A, B, block, insert_blocks, proof_bundle, state

pytestmark = pytest.mark.clickhouse
ENDPOINT = "bsc.substreams.pinax.network:443"


def prepared(databases, tmp_path):
    client = databases(False)
    args = (client, SPKG, ENDPOINT, [A], 100, tmp_path, native_dsn(client.database))
    return client, args, prepare(*args)


def test_repeated_prepare_preserves_identity_and_rejects_filter_or_range_drift(databases, tmp_path):
    client, args, first = prepared(databases, tmp_path)
    assert prepare(*args) == first
    assert first["identity"]["final_blocks_only"]
    assert client.one("SELECT run_id FROM _evm_state_run")["run_id"] == first["run_id"]
    for i, value in [(2, "another.example:443"), (3, [B]), (4, 99)]:
        changed = list(args); changed[i] = value
        with pytest.raises(VerificationError, match="identity changed"):
            prepare(*changed)


def test_missing_cursor_for_existing_data_is_not_silently_restarted(databases, tmp_path):
    client, args, _ = prepared(databases, tmp_path)
    bundle = proof_bundle(100, {A: state()})
    insert_blocks(client, [block(100, bundle)])
    with pytest.raises(VerificationError, match="cursor is missing"):
        ingest(*args, stop_block=101)
    (tmp_path / "cursor.txt").write_text("")
    with pytest.raises(VerificationError, match="cursor is missing"):
        ingest(*args, stop_block=101)


def test_cursor_without_database_rows_requires_restore(databases, tmp_path):
    _, args, _ = prepared(databases, tmp_path)
    (tmp_path / "cursor.txt").write_text("stale cursor")
    with pytest.raises(VerificationError, match="without its block data"):
        ingest(*args, stop_block=101)


@pytest.mark.parametrize("defect", ["schema", "package", "owner", "replaced_database", "phase"])
def test_damaged_or_replaced_run_metadata_fails_closed(databases, tmp_path, defect):
    client, args, _ = prepared(databases, tmp_path)
    if defect == "schema":
        (tmp_path / "meta" / f"{client.database}_schema_hash.txt").unlink()
    elif defect == "package":
        (tmp_path / "package.spkg").write_bytes(b"changed package")
    elif defect == "owner":
        client.execute("DROP TABLE _evm_state_run SYNC")
    elif defect == "phase":
        path = tmp_path / "run.json"
        record = json.loads(path.read_text()); record["phase"] = "corrupt"
        path.write_text(json.dumps(record))
    else:
        client.execute(f"DROP DATABASE {client.database} SYNC")
        # Use default DB because the old connection now names a missing database.
        from evm_state.ch import ClickHouse
        ClickHouse("default").execute(f"CREATE DATABASE {client.database}")
    with pytest.raises(VerificationError):
        prepare(*args)


def test_second_state_directory_cannot_claim_an_existing_run(databases, tmp_path):
    client, args, _ = prepared(databases, tmp_path / "first")
    other = list(args); other[5] = tmp_path / "second"
    with pytest.raises(VerificationError, match="already contains tables"):
        prepare(*other)


def test_second_local_writer_is_excluded(databases, tmp_path):
    _, args, _ = prepared(databases, tmp_path)
    with exclusive_lock(tmp_path / "run.lock"):
        with pytest.raises(ValueError, match="another process"):
            prepare(*args)
