import json

import pytest

from evm_state.checkpoint import build, read_account
from evm_state.export import export_checkpoint
from evm_state.importer import import_checkpoint
from evm_state.retention import prune
from export_fixtures import checkpoint, complete_bundle
from state_fixtures import A, B, block, insert_blocks, source, state, word

pytestmark = pytest.mark.clickhouse


def test_verified_export_restores_into_new_database_and_continues_from_its_block(databases, tmp_path):
    origin, old = checkpoint(databases, {A: state({1: 2, 2: 3}, nonce=2**63 + 9), B: state({7: 8})})
    directory = tmp_path / "export"
    export_checkpoint(origin, old["snapshot_id"], directory, page_size=1)
    target = databases(False)
    restored = import_checkpoint(target, directory, expected_hash=old["header"]["hash"])
    assert restored["snapshot_id"] != old["snapshot_id"]
    assert restored["state_sha256"] == old["state_sha256"]
    assert read_account(target, restored["snapshot_id"], A)["nonce"] == 2**63 + 9
    stream = databases()
    bundle = complete_bundle(101, {A: state({2: 3}, nonce=2**63 + 9), B: state({7: 8})}, old["header"]["hash"])
    row = block(101, bundle, storage={(A, 1): 0})
    row.update(hash=bundle["header"]["hash"], parent_hash=bundle["header"]["parent_hash"])
    insert_blocks(stream, [row])
    advanced = build(target, bundle, [source(stream, 101, [A, B])], restored["snapshot_id"])
    assert advanced["nonzero_slots"] == 2
    assert prune(target, keep_latest=1)["remove"] == [restored["snapshot_id"]]
    assert read_account(target, advanced["snapshot_id"], B)["nonzero_slots"] == 1


def test_failure_during_restore_keeps_old_checkpoint_readable_and_candidate_unpublished(databases, tmp_path, monkeypatch):
    origin, imported = checkpoint(databases)
    directory = tmp_path / "export"
    export_checkpoint(origin, imported["snapshot_id"], directory)
    target, old = checkpoint(databases)
    original = target.insert
    def fail(table, rows, **kwargs):
        if table == "checkpoints":
            assert read_account(target, old["snapshot_id"], A)["nonzero_slots"] == 5
            raise OSError("injected restore interruption")
        return original(table, rows, **kwargs)
    monkeypatch.setattr(target, "insert", fail)
    with pytest.raises(OSError, match="injected"):
        import_checkpoint(target, directory)
    assert target.one("SELECT snapshot_id FROM checkpoints FINAL")["snapshot_id"] == old["snapshot_id"]


def test_post_insert_verification_rejects_altered_database_values(databases, tmp_path, monkeypatch):
    origin, old = checkpoint(databases)
    directory = tmp_path / "export"
    export_checkpoint(origin, old["snapshot_id"], directory)
    target = databases(False)
    original = target.insert
    def corrupt(table, rows, **kwargs):
        rows = list(rows)
        if table == "checkpoint_storage":
            rows[0]["value"] = word(42)
        return original(table, rows, **kwargs)
    monkeypatch.setattr(target, "insert", corrupt)
    with pytest.raises(ValueError, match="storage root"):
        import_checkpoint(target, directory)
    assert int(target.one("SELECT count() AS n FROM checkpoints FINAL")["n"]) == 0


def test_changed_import_file_cannot_target_an_existing_checkpoint(databases, tmp_path, monkeypatch):
    origin, source_record = checkpoint(databases)
    directory = tmp_path / "export"
    export_checkpoint(origin, source_record["snapshot_id"], directory)
    target, ready = checkpoint(databases, {A: state({8: 9})})
    import evm_state.importer as module
    original = module._check_file
    def swap_after_checksum(folder, item):
        path = original(folder, item)
        rows = json.loads(path.read_text())
        rows[0]["snapshot_id"] = ready["snapshot_id"]
        path.write_text(json.dumps(rows))
        return path
    monkeypatch.setattr(module, "_check_file", swap_after_checksum)
    with pytest.raises(ValueError, match="account fields"):
        import_checkpoint(target, directory)
    assert read_account(target, ready["snapshot_id"], A)["nonzero_slots"] == 1
    assert target.one("SELECT snapshot_id FROM checkpoints FINAL")["snapshot_id"] == ready["snapshot_id"]
