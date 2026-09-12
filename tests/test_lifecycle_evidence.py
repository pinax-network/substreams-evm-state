"""Keep captured producer fixtures bound to their original header and RPC evidence."""
import hashlib
import json
from pathlib import Path

from evm_state.header import verify_header
from evm_state.proof import keccak256, unhex, verify_account

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "tests/fixtures/lifecycle"


def test_captured_lifecycle_messages_and_headers_match_provenance():
    evidence = json.loads((FIXTURES / "manifest.json").read_text())
    assert evidence["chain_id"] == 56
    assert len(evidence["records"]) == 15
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
