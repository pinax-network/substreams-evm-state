#!/usr/bin/env python3
"""Check a guarded native update interval against saved proofs and archive RPC.

This diagnostic checks every observed account field and final touched slot. It
does not enumerate untouched storage or publish a complete-state checkpoint.
Capture the proof bundle before replay so old eth_getProof availability does not
limit an otherwise reproducible historical update check.
"""
import argparse
import hashlib
import json
from pathlib import Path

from evm_state.ch import ClickHouse
from evm_state.checkpoint import _observed_fields, _union_storage, validate_interval
from evm_state.files import atomic_json
from evm_state.header import verify_header
from evm_state.proof import VerificationError, keccak256, unhex, verify_account
from evm_state.rpc import RPC
from evm_state.source import verified_source


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True)
    parser.add_argument("--state-dir", type=Path, required=True)
    parser.add_argument("--proofs", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        parser.error("output already exists")
    directory = args.state_dir.resolve()
    run = json.loads((directory / "run.json").read_text())
    bundle = json.loads(args.proofs.read_text())
    if bundle.get("format_version") != 1 or bundle.get("chain_id") != 56:
        raise VerificationError("expected a BSC proof bundle")
    if bundle.get("header_trust") not in {"provider-finalized-header", "operator-pinned-hash"}:
        raise VerificationError("proof bundle has no declared header trust")
    header = bundle["header"]
    verify_header(bundle["header_rlp"], header)
    declared = {key: run["identity"][key] for key in
                ["database", "accounts", "start_block", "module_hash", "final_blocks_only"]}
    if sorted(bundle["accounts"]) != declared["accounts"]:
        raise VerificationError("proof bundle and native filter differ")
    client, rpc = ClickHouse(args.database), RPC()
    if int(rpc.call("eth_chainId", []), 16) != 56:
        raise VerificationError("expected the BSC RPC")
    params = {"start0": declared["start_block"], "end": header["number"]}
    metadata_checks = {}
    slots_checked = {}
    slot_digest = hashlib.sha256()
    with verified_source(client, declared, end_block=header["number"]) as checked:
        if checked["state_directory"] != str(directory):
            raise VerificationError("measurement directory does not own the native source")
        blocks = validate_interval(client, checked, header)
        observed = _observed_fields(client, [checked], None, params)
        if not observed or set(observed) - set(bundle["accounts"]):
            raise VerificationError("native account field coverage is empty or unexpected")
        for address, value in bundle["accounts"].items():
            proven = verify_account(header["state_root"], address, value["proof"])
            if keccak256(unhex(value["code"])) != proven.code_hash:
                raise VerificationError("saved bytecode differs from account proof")
            expected = {**proven.json(), "code": value["code"]}
            fields = observed.get(address, {})
            for field, actual in fields.items():
                if actual != expected[field]:
                    raise VerificationError(f"native {field} differs from proven account {address}")
            # Record only tested values; absent changes remain unobserved.
            metadata_checks[address] = fields
        query = _union_storage([checked], None, client, params)
        for row in client.rows("SELECT address,slot,argMax(value,position) AS value FROM (" + query +
                               ") GROUP BY address,slot ORDER BY address,slot", params):
            address = row["address"]
            if address not in bundle["accounts"]:
                raise VerificationError("unexpected storage account")
            actual = rpc.call("eth_getStorageAt", [address, row["slot"], hex(header["number"])]).lower()
            if unhex(actual, 32) != unhex(row["value"], 32):
                raise VerificationError(f"native storage differs from RPC for {address} at {row['slot']}")
            slot_digest.update(unhex(address, 20) + unhex(row["slot"], 32) + unhex(actual, 32))
            slots_checked[address] = slots_checked.get(address, 0) + 1
        after = rpc.call("eth_getBlockByNumber", [hex(header["number"]), False])
        if after["hash"].lower() != header["hash"]:
            raise VerificationError("RPC header changed during verification")
    result = {"format_version": 1, "chain_id": 56, "header": header,
        "header_trust": bundle["header_trust"], "start_block": declared["start_block"],
        "blocks": blocks, "database": client.database, "run_id": run["run_id"],
        "module_hash": declared["module_hash"], "package_sha256": run["identity"]["package_sha256"],
        "proof_bundle_sha256": hashlib.sha256(args.proofs.read_bytes()).hexdigest(),
        "metadata_verified_against_account_proofs": metadata_checks,
        "final_touched_slots_verified_against_archive_rpc": slots_checked,
        "ordered_slot_values_sha256": slot_digest.hexdigest(),
        "slot_digest_encoding": "ascending address/slot, concatenated 20-byte address and 32-byte slot/value",
        "qualification": "Observed updates only; untouched storage completeness and BSC consensus are not verified."}
    atomic_json(args.output, result)
    print(json.dumps({"blocks": blocks, "accounts": len(metadata_checks),
                      "observed_fields": sum(map(len, metadata_checks.values())),
                      "touched_slots": sum(slots_checked.values()), "output": str(args.output)}))


if __name__ == "__main__":
    main()
