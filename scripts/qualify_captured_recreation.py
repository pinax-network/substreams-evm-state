#!/usr/bin/env python3
"""Compare an owned native interval with the captured BSC recreation cases.

Archive RPC metadata is recorded in the capture manifest. This diagnostic does
not claim complete storage, account-proof verification or publish a checkpoint.
"""
import argparse
import hashlib
import json
from pathlib import Path

from evm_state.ch import ClickHouse
from evm_state.checkpoint import ZERO, _observed_fields, _union_storage, validate_interval
from evm_state.files import atomic_json
from evm_state.header import verify_header
from evm_state.proof import VerificationError
from evm_state.rpc import RPC
from evm_state.source import verified_source


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True)
    parser.add_argument("--state-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    fixture_dir = Path(__file__).resolve().parents[1] / "tests/fixtures/lifecycle"
    manifest = json.loads((fixture_dir / "manifest.json").read_text())
    records = sorted([r for r in manifest["records"] if r["filename"].startswith("v3-metamorphic-")],
                     key=lambda r: r["block"])
    if len(records) != 5:
        raise VerificationError("expected five captured recreation/storage cases")
    directory = args.state_dir.resolve()
    run = json.loads((directory / "run.json").read_text())
    declared = {key: run["identity"][key] for key in
                ["database", "accounts", "start_block", "module_hash", "final_blocks_only"]}
    address = "0xe82c715e37f2f2e190dd2ca86fb796cafaf0beff"
    if declared["accounts"] != [address] or declared["start_block"] != records[0]["block"] - 1:
        raise VerificationError("unexpected native filter or interval")
    client, rpc = ClickHouse(args.database), RPC()
    header = records[-1]["header"]
    comparisons = []
    known_slots = sorted({slot for record in records
                          for slot in record["rpc_block_end_state"][address]["after"]["storage"]})
    with verified_source(client, declared, end_block=header["number"]) as checked:
        if checked["state_directory"] != str(directory):
            raise VerificationError("directory does not own the native source")
        blocks = validate_interval(client, checked, header)
        for record in records:
            verify_header(record["header_rlp"], record["header"], record["block_hash"])
            raw = (fixture_dir / record["filename"]).read_bytes()
            if hashlib.sha256(raw).hexdigest() != record["sha256"]:
                raise VerificationError("captured transaction changed")
            number = record["block"]
            params = {"start0": declared["start_block"], "end": number}
            observed = _observed_fields(client, [checked], None, params)
            expected = record["rpc_block_end_state"][address]["after"]
            if set(observed) != {address} or set(observed[address]) != {"balance", "nonce", "code", "code_hash"}:
                raise VerificationError("unexpected native metadata coverage")
            if any(expected[key] != value for key, value in observed[address].items()):
                raise VerificationError("native metadata differs from captured archive RPC state")
            row = client.one("SELECT hash,`lifecycle.kind`,`lifecycle.ordinal`,length(`storage.slot`) AS slots "
                             "FROM state_blocks FINAL WHERE number={end:UInt64}", params)
            live_header = rpc.call("eth_getBlockByNumber", [hex(number), False])
            if row["hash"] != record["block_hash"] or live_header["hash"].lower() != row["hash"]:
                raise VerificationError("native/captured/current RPC block identity differs")
            union = _union_storage([checked], None, client, params)
            slots = {r["slot"]: r["value"] for r in client.rows(
                "SELECT slot,argMax(value,position) AS value FROM (" + union + ") GROUP BY slot", params)}
            slot_checks = {}
            for slot in known_slots:
                value = rpc.call("eth_getStorageAt", [address, slot, hex(number)]).lower()
                if slots.get(slot, ZERO) != value:
                    raise VerificationError("native storage/reset differs from archive RPC")
                slot_checks[slot] = value
            comparisons.append({"block": number, "block_hash": row["hash"], "transaction_hash": record["hash"],
                "fixture": record["filename"], "fixture_sha256": record["sha256"],
                "native_metadata": {key: value for key, value in observed[address].items() if key != "code"},
                "code_bytes": (len(observed[address]["code"]) - 2) // 2,
                "lifecycle": [{"kind": kind, "ordinal": ordinal} for kind, ordinal in
                              zip(row["lifecycle.kind"], row["lifecycle.ordinal"], strict=True)],
                "storage_patches": row["slots"], "tracked_slot_values": slot_checks})
        storage = int(client.one("SELECT sum(length(`storage.slot`)) AS n FROM state_blocks FINAL "
                                 "WHERE number BETWEEN {start:UInt64} AND {end:UInt64}",
                                 {"start": declared["start_block"], "end": header["number"]})["n"])
    result = {"format_version": 1, "chain_id": 56, "database": client.database, "account": address,
        "start_block": declared["start_block"], "header": header, "blocks": blocks,
        "run_id": run["run_id"], "module_hash": declared["module_hash"],
        "package_sha256": run["identity"]["package_sha256"], "comparisons": comparisons,
        "storage_patches_in_interval": storage,
        "qualification": "Native metadata and lifecycle parity against captured archive RPC values; "
                         "no account/storage root verification, untouched-slot coverage or ready checkpoint."}
    atomic_json(args.output, result)
    print(json.dumps({"blocks": blocks, "cases": len(comparisons), "storage_patches": storage}))


if __name__ == "__main__":
    main()
