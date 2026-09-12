"""Keep captured producer fixtures bound to their original header and RPC evidence."""
import hashlib
import json
from pathlib import Path

import pytest

from evm_state.header import verify_header
from evm_state.proof import keccak256, unhex, verify_account

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "tests/fixtures/lifecycle"


def test_captured_lifecycle_messages_and_headers_match_provenance():
    evidence = json.loads((FIXTURES / "manifest.json").read_text())
    assert evidence["chain_id"] == 56
    assert len(evidence["records"]) == 24
    assert {record["producer_version"] for record in evidence["records"]} == {3, 4, 5}
    for record in evidence["records"]:
        raw = (FIXTURES / record["filename"]).read_bytes()
        assert len(raw) == record["bytes"]
        assert hashlib.sha256(raw).hexdigest() == record["sha256"]
        assert hashlib.sha256((FIXTURES / record["header_filename"]).read_bytes()).hexdigest() == record["header_sha256"]
        verify_header(record["header_rlp"], record["header"], record["block_hash"])
        assert record["header"]["number"] == record["block"]
        assert (int(record["rpc_receipt_status"], 16) == 1) == (record["status"] == 1)
        for account in record["rpc_block_end_state"].values():
            for value in account.values():
                assert "0x" + keccak256(unhex(value["code"])).hex() == value["code_hash"]
        if "rpc_calltrace" in record:
            trace = record["rpc_calltrace"]
            assert hashlib.sha256((FIXTURES / trace["filename"]).read_bytes()).hexdigest() == trace["sha256"]
            calls = json.loads((FIXTURES / trace["filename"]).read_text())["calls"]
            assert any(call["type"] == "CREATE2" and call["to"] ==
                       "0x58066f069811a69b8b5bac97c1dd76d54d428a72" for call in calls)


def test_real_native_lifecycle_metadata_matches_saved_account_proofs():
    evidence = json.loads((ROOT / "docs/evidence/bsc-lifecycle-matrix-2026-09-12.json").read_text())
    raw = (FIXTURES / "recent-proofs.json").read_bytes()
    assert hashlib.sha256(raw).hexdigest() == evidence["proof_bundle_sha256"]
    bundle = json.loads(raw)
    assert bundle["header"] == evidence["header"]
    verify_header(bundle["header_rlp"], bundle["header"],
                  "0x7cc97d80f89e23bc713c37d6150b27bb65bb56eb78955c082c12a53a78ee0b04")
    fields = evidence["metadata_verified_against_account_proofs"]
    assert len(fields) == 8 and sum(map(len, fields.values())) == 24
    for address, value in bundle["accounts"].items():
        account = verify_account(bundle["header"]["state_root"], address, value["proof"])
        assert keccak256(unhex(value["code"])) == account.code_hash
        expected = {**account.json(), "code": value["code"]}
        assert all(expected[key] == actual for key, actual in fields[address].items())


def test_native_recreation_parity_matches_captured_fields_and_slot_reset():
    evidence = json.loads((ROOT / "docs/evidence/bsc-recreation-2026-09-12.json").read_text())
    records = {record["filename"]: record for record in json.loads((FIXTURES / "manifest.json").read_text())["records"]}
    assert evidence["blocks"] == 145 and evidence["storage_patches_in_interval"] == 2
    assert len(evidence["comparisons"]) == 5
    slot = "0xb82207f487d5f82a808c4a79eaef2903fd056d9256cb1af55d518291f0176329"
    for comparison in evidence["comparisons"]:
        record = records[comparison["fixture"]]
        assert record["sha256"] == comparison["fixture_sha256"]
        assert record["block_hash"] == comparison["block_hash"]
        expected = record["rpc_block_end_state"][evidence["account"]]["after"]
        assert all(expected[k] == v for k, v in comparison["native_metadata"].items())
        assert comparison["code_bytes"] == len(unhex(expected["code"]))
        expected_value = 2**55 if comparison["block"] == 37741154 else 0
        assert int(comparison["tracked_slot_values"][slot], 16) == expected_value
        if comparison["block"] in {37741077, 37741218}:
            assert any(v["kind"] == "storage_reset" and v["ordinal"] == record["tx_end_ordinal"]
                       for v in comparison["lifecycle"])


def test_repeated_authority_native_updates_match_saved_account_proofs():
    evidence = json.loads((ROOT / "docs/evidence/bsc-authorization-edges-2026-09-12.json").read_text())
    raw = (FIXTURES / "authorization-edge-proofs.json").read_bytes()
    assert hashlib.sha256(raw).hexdigest() == evidence["proof_bundle_sha256"]
    bundle = json.loads(raw)
    assert bundle["header"] == evidence["header"]
    verify_header(bundle["header_rlp"], bundle["header"],
                  "0x352f27ef9341ca0976bdba3f3f444ddfc40ed27d434996168f8c13a58eb9ba11")
    assert evidence["blocks"] == 9740
    fields = evidence["metadata_verified_against_account_proofs"]
    assert len(fields) == 5 and sum(map(len, fields.values())) == 9
    for address, value in bundle["accounts"].items():
        account = verify_account(bundle["header"]["state_root"], address, value["proof"])
        assert keccak256(unhex(value["code"])) == account.code_hash
        expected = {**account.json(), "code": value["code"]}
        assert all(expected[key] == actual for key, actual in fields[address].items())


def test_failed_distinct_authority_native_updates_match_saved_account_proofs():
    evidence = json.loads((ROOT / "docs/evidence/bsc-failed-distinct-authorities-2026-09-12.json").read_text())
    raw = (FIXTURES / "failed-distinct-proofs.json").read_bytes()
    assert hashlib.sha256(raw).hexdigest() == evidence["proof_bundle_sha256"]
    bundle = json.loads(raw)
    assert bundle["header"] == evidence["header"]
    verify_header(bundle["header_rlp"], bundle["header"],
                  "0x848c7f22bf3e1846f080d80623eef146f266db5bf8085818aa6ab7ea7eed5cb0")
    assert evidence["blocks"] == 80813 and evidence["start_block"] == 121403152
    fields = evidence["metadata_verified_against_account_proofs"]
    assert len(fields) == 3 and sum(map(len, fields.values())) == 8
    for address, value in bundle["accounts"].items():
        account = verify_account(bundle["header"]["state_root"], address, value["proof"])
        assert keccak256(unhex(value["code"])) == account.code_hash
        expected = {**account.json(), "code": value["code"]}
        assert all(expected[key] == actual for key, actual in fields[address].items())
    for authority in ["0xa96669262c911d4158e26b972aaeabbb08979ddb", "0x0dcc966314b622bf094c7afb31b6632d646880f9"]:
        assert fields[authority]["nonce"] == 1
        assert fields[authority]["code"] == "0xef0100cb4dd2ac21ee75be478989d8b05897de225e5910"


@pytest.mark.parametrize("version,accounts,code_bytes", [(3, 6, 21), (5, 1, 4)])
def test_existing_account_selfdestruct_native_markers_match_surviving_archive_state(version, accounts, code_bytes):
    evidence = json.loads((ROOT / f"docs/evidence/bsc-existing-selfdestruct-v{version}-2026-09-12.json").read_text())
    records = {r["filename"]: r for r in json.loads((FIXTURES / "manifest.json").read_text())["records"]}
    record = records[evidence["fixture"]]
    assert evidence["fixture_sha256"] == record["sha256"]
    assert evidence["header"] == record["header"] and evidence["blocks"] == 2
    assert len(evidence["comparisons"]) == accounts
    for address, checked in evidence["comparisons"].items():
        states = record["rpc_block_end_state"][address]
        assert checked["selfdestruct_ordinals"]
        for state in states.values():
            assert checked["code_hash_before_and_after"] == state["code_hash"]
            assert checked["nonce_before_and_after"] == state["nonce"] == 1
            assert checked["code_bytes_before_and_after"] == len(unhex(state["code"])) == code_bytes
        assert all(states["after"][key] == actual for key, actual in checked["observed_native_fields"].items())


@pytest.mark.parametrize("name,proof_file,blocks,field_count", [
    ("bsc-existing-selfdestruct-recent", "existing-selfdestruct-proofs.json", 280137, 7),
    ("bsc-failed-clears", "failed-clear-proofs.json", 959503, 10),
])
def test_selfdestruct_and_failed_clear_replays_match_saved_account_proofs(name, proof_file, blocks, field_count):
    evidence = json.loads((ROOT / f"docs/evidence/{name}-2026-09-12.json").read_text())
    raw = (FIXTURES / proof_file).read_bytes()
    assert hashlib.sha256(raw).hexdigest() == evidence["proof_bundle_sha256"]
    bundle = json.loads(raw)
    assert bundle["header"] == evidence["header"] and evidence["blocks"] == blocks
    verify_header(bundle["header_rlp"], bundle["header"])
    fields = evidence["metadata_verified_against_account_proofs"]
    assert len(fields) == 3 and sum(map(len, fields.values())) == field_count
    assert evidence["final_touched_slots_verified_against_archive_rpc"] == {}
    for address, value in bundle["accounts"].items():
        account = verify_account(bundle["header"]["state_root"], address, value["proof"])
        assert keccak256(unhex(value["code"])) == account.code_hash
        expected = {**account.json(), "code": value["code"]}
        assert all(expected[key] == actual for key, actual in fields[address].items())


@pytest.mark.parametrize("name,blocks,cases", [
    ("bsc-failed-clears-captured", 60249, 2), ("bsc-invalid-self-clear", 40373, 1),
])
def test_captured_clear_block_end_evidence_cannot_be_replaced_by_later_updates(name, blocks, cases):
    evidence = json.loads((ROOT / f"docs/evidence/{name}-2026-09-12.json").read_text())
    records = {r["filename"]: r for r in json.loads((FIXTURES / "manifest.json").read_text())["records"]}
    assert evidence["blocks"] == blocks and len(evidence["comparisons"]) == cases
    expected = {"v5-failed-authority-clear.pb": (3442, 10581),
                "v5-failed-self-clear.pb": (1170, 40), "v5-invalid-self-clear-noop.pb": (None, 102)}
    for checked in evidence["comparisons"]:
        record = records[checked["fixture"]]
        assert checked["fixture_sha256"] == record["sha256"]
        assert checked["block_hash"] == record["block_hash"] and checked["block"] == record["block"]
        for address, fields in checked["native_metadata"].items():
            after = record["rpc_block_end_state"][address]["after"]
            assert all(after[key] == value for key, value in fields.items())
        ordinal, nonce = expected[checked["fixture"]]
        assert checked["native_metadata"][checked["authority"]]["nonce"] == nonce
        assert checked["storage_patches"] == 0 and checked["code_bytes_before"] == 23
        if ordinal is None:
            assert checked["native_code_patches"] == [] and checked["native_lifecycle"] == []
            assert checked["code_bytes_after"] == 23
        else:
            assert checked["code_bytes_after"] == 0
            assert checked["native_code_patches"] == [{"code": "0x", "ordinal": ordinal}]
            assert checked["native_lifecycle"] == [{"kind": "code_cleared", "ordinal": ordinal}]
