import json
from pathlib import Path

import pytest

from conftest import SPKG, native_dsn
from evm_state.checkpoint import build
from evm_state.files import exclusive_lock
from evm_state.ingest import prepare
from evm_state.proof import VerificationError
from evm_state.source import verified_source
from state_fixtures import A, B, block, insert_blocks, proof_bundle, source, state

pytestmark = pytest.mark.clickhouse


@pytest.mark.parametrize("defect", ["module", "finality", "accounts", "start", "owner_file", "phase",
                                  "package", "schema", "database_uuid", "run_id", "provenance", "old_format"])
def test_checkpoint_rejects_source_identity_drift_before_state_allocation(databases, defect):
    stream, target = databases(), databases(False)
    bundle = proof_bundle(100, {A: state({1: 2})})
    insert_blocks(stream, [block(100, bundle, storage={(A, 1): 2})])
    data = source(stream, 100, target=target)
    owner = stream.one("SELECT run_id,identity FROM _evm_state_run")
    identity = json.loads(owner["identity"])
    directory = Path(identity["state_directory"])
    path = directory / "run.json"
    record = json.loads(path.read_text())
    if defect == "module": data["module_hash"] = "a" * 40
    elif defect == "finality": data["final_blocks_only"] = False
    elif defect == "accounts": data["accounts"] = [B]
    elif defect == "start": data["start_block"] = 99
    elif defect == "owner_file": path.unlink()
    elif defect == "package": (directory / "package.spkg").write_bytes(b"another package")
    elif defect == "schema": (directory / "meta" / f"{stream.database}_schema_hash.txt").write_text("changed")
    elif defect == "provenance": data["database_uuid"] = "another database"
    elif defect == "old_format":
        identity["format_version"] = 1
        stream.execute("TRUNCATE TABLE _evm_state_run")
        stream.insert("_evm_state_run", [{**owner, "identity": json.dumps(identity)}])
    else:
        record[defect] = "different"
        path.write_text(json.dumps(record))
    with pytest.raises(VerificationError):
        build(target, bundle, [data])
    assert int(target.one("SELECT count() AS n FROM checkpoint_storage")["n"]) == 0
    assert int(target.one("SELECT count() AS n FROM checkpoints")["n"]) == 0


def test_source_json_alone_cannot_claim_native_ownership(databases):
    stream, target = databases(), databases(False)
    bundle = proof_bundle(100, {A: state()})
    insert_blocks(stream, [block(100, bundle)])
    invented = {"database": stream.database, "start_block": 100, "accounts": [A],
                "module_hash": "f" * 40, "final_blocks_only": True}
    with pytest.raises(VerificationError, match="ownership"):
        build(target, bundle, [invented])


def test_source_lock_protects_input_history_during_checkpoint_reads(databases):
    stream = databases()
    data = source(stream, 100)
    with verified_source(stream, data) as checked:
        assert checked["run_id"] and checked["database_uuid"] and checked["package_sha256"]
        with pytest.raises(ValueError, match="another process"):
            with exclusive_lock(Path(checked["state_directory"]) / "source_readers.lock"):
                pass
    with exclusive_lock(Path(checked["state_directory"]) / "source_readers.lock"):
        pass


def test_real_prepared_run_identity_is_recorded_in_checkpoint(databases, tmp_path):
    stream, target = databases(False), databases(False)
    run = prepare(stream, SPKG, "bsc.substreams.pinax.network:443", [A], 100,
                  tmp_path / "native", native_dsn(stream.database), checkpoint_database=target.database)
    bundle = proof_bundle(100, {A: state({1: 2})})
    insert_blocks(stream, [block(100, bundle, storage={(A, 1): 2})])
    from evm_state.cursor import save_progress
    from state_fixtures import native_cursor
    save_progress(stream, run, tmp_path / "native", native_cursor(100, bundle["header"]["hash"]))
    data = {key: run["identity"][key] for key in ["database", "start_block", "accounts", "module_hash", "final_blocks_only"]}
    result = build(target, bundle, [data])
    provenance = result["sources"][0]
    assert provenance["run_id"] == run["run_id"]
    assert provenance["database_uuid"] == run["database_uuid"]
    assert provenance["package_sha256"] == run["identity"]["package_sha256"]
