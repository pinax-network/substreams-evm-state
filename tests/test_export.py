import copy
import gzip
import json

import pytest

from evm_state.ch import ClickHouse
from evm_state.export import export_checkpoint, file_hash, verify_export
from evm_state.proof import VerificationError
from evm_state.retention import prune
from export_fixtures import checkpoint
from state_fixtures import A, B, state, word

pytestmark = pytest.mark.clickhouse


def exported(databases, tmp_path):
    target, ready = checkpoint(databases, {A: state({1: 2, 2: 3, 3: 4}), B: state({}, nonce=0, balance=0, code="0x", exists=False)})
    directory = tmp_path / "export"
    result = export_checkpoint(target, ready["snapshot_id"], directory, page_size=2)
    return target, ready, directory, result


def test_export_is_paginated_deterministic_and_verifiable_without_database_access(databases, tmp_path, monkeypatch):
    target, ready, directory, result = exported(databases, tmp_path)
    assert result["pages"] == 2
    again = tmp_path / "again"
    export_checkpoint(target, ready["snapshot_id"], again, page_size=2)
    assert {p.name: file_hash(p) for p in directory.iterdir()} == {p.name: file_hash(p) for p in again.iterdir()}
    with monkeypatch.context() as offline:
        offline.setattr(ClickHouse, "request", lambda *args, **kwargs: pytest.fail("offline verifier accessed ClickHouse"))
        verified = verify_export(directory, expected_hash=ready["header"]["hash"])
    assert verified["state_sha256"] == ready["state_sha256"]
    assert verified["nonzero_slots"] == 3
    assert verified["accounts"] == [A, B]


@pytest.mark.parametrize("defect", ["missing_page", "changed_value", "extra_slot", "missing_slot", "wrong_nonce", "wrong_code", "wrong_root", "wrong_header", "wrong_checksum", "duplicate_page", "path_traversal", "missing_manifest"])
def test_corrupt_or_incomplete_export_is_rejected_even_if_file_checksums_are_rewritten(databases, tmp_path, defect):
    _, ready, directory, _ = exported(databases, tmp_path)
    manifest_path = directory / "manifest.json"
    layout = json.loads(manifest_path.read_text())
    if defect == "missing_manifest":
        manifest_path.unlink()
        with pytest.raises((VerificationError, OSError)):
            verify_export(directory)
        return
    if defect == "missing_page": (directory / layout["storage_pages"][0]["file"]).unlink()
    elif defect in {"changed_value", "extra_slot", "missing_slot"}:
        item = layout["storage_pages"][0]
        path = directory / item["file"]
        rows = [json.loads(line) for line in gzip.decompress(path.read_bytes()).splitlines()]
        if defect == "changed_value": rows[0]["value"] = word(123)
        elif defect == "extra_slot": rows.insert(0, {"address": A, "slot": word(0), "value": word(99)})
        else: rows.pop(0)
        path.write_bytes(gzip.compress(b"".join((json.dumps(r)+"\n").encode() for r in rows)))
        item.update(bytes=path.stat().st_size, sha256=file_hash(path), rows=len(rows), first_slot=rows[0]["slot"], last_slot=rows[-1]["slot"])
    elif defect in {"wrong_nonce", "wrong_code"}:
        path = directory / "accounts.json"
        rows = json.loads(path.read_text())
        if defect == "wrong_nonce": rows[0]["nonce"] = "99"
        else: rows[0]["code"] = "0x"
        path.write_text(json.dumps(rows))
        layout["account_file"].update(bytes=path.stat().st_size, sha256=file_hash(path))
    elif defect == "wrong_root": layout["checkpoint"]["proof_bundle"]["header"]["state_root"] = word(55)
    elif defect == "wrong_header": layout["checkpoint"]["proof_bundle"]["header_rlp"] = "0xc0"
    elif defect == "wrong_checksum": layout["checkpoint"]["state_sha256"] = "f" * 64
    elif defect == "duplicate_page": layout["storage_pages"].append(copy.deepcopy(layout["storage_pages"][0]))
    else: layout["storage_pages"][0]["file"] = "../outside.jsonl.gz"
    manifest_path.write_text(json.dumps(layout))
    with pytest.raises(VerificationError):
        verify_export(directory, expected_hash=ready["header"]["hash"])


def test_export_blocks_retention_and_never_publishes_an_incomplete_manifest(databases, tmp_path, monkeypatch):
    target, ready = checkpoint(databases)
    import evm_state.export as module
    def fail(*args, **kwargs):
        with pytest.raises(ValueError, match="another process"):
            prune(target)
        raise OSError("injected export failure")
    monkeypatch.setattr(module, "_write_page", fail)
    directory = tmp_path / "interrupted"
    with pytest.raises(OSError, match="injected"):
        export_checkpoint(target, ready["snapshot_id"], directory, page_size=1)
    assert not (directory / "manifest.json").exists()


def test_export_refuses_an_existing_destination(databases, tmp_path):
    target, ready, directory, _ = exported(databases, tmp_path)
    before = (directory / "manifest.json").read_bytes()
    with pytest.raises(FileExistsError):
        export_checkpoint(target, ready["snapshot_id"], directory)
    assert (directory / "manifest.json").read_bytes() == before


def test_export_preserves_uint64_nonces_for_json_consumers(databases, tmp_path):
    nonce = 2**63 + 7
    target, ready = checkpoint(databases, {A: state({1: 2}, nonce=nonce)})
    directory = tmp_path / "uint64"
    export_checkpoint(target, ready["snapshot_id"], directory)
    assert json.loads((directory / "accounts.json").read_text())[0]["nonce"] == str(nonce)
    assert verify_export(directory)["state_sha256"] == ready["state_sha256"]
