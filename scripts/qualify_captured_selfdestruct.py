#!/usr/bin/env python3
"""Verify native post-Cancun SELFDESTRUCT markers against captured archive state.

This checks the captured block and its predecessor. It does not reconstruct
untouched storage, obtain historical account proofs or publish a checkpoint.
"""
import argparse
import hashlib
import json
from pathlib import Path

from evm_state.ch import ClickHouse
from evm_state.checkpoint import _observed_fields, validate_interval
from evm_state.files import atomic_json
from evm_state.header import verify_header
from evm_state.proof import VerificationError
from evm_state.rpc import RPC
from evm_state.source import verified_source


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True)
    parser.add_argument("--state-dir", type=Path, required=True)
    parser.add_argument("--fixture", required=True, choices=[
        "v3-post-cancun-existing-selfdestruct.pb", "v5-post-cancun-existing-selfdestruct.pb"])
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        parser.error("output already exists")
    fixture_dir = Path(__file__).resolve().parents[1] / "tests/fixtures/lifecycle"
    manifest = json.loads((fixture_dir / "manifest.json").read_text())
    record = next(r for r in manifest["records"] if r["filename"] == args.fixture)
    if hashlib.sha256((fixture_dir / args.fixture).read_bytes()).hexdigest() != record["sha256"]:
        raise VerificationError("captured transaction changed")
    verify_header(record["header_rlp"], record["header"], record["block_hash"])
    directory = args.state_dir.resolve()
    run = json.loads((directory / "run.json").read_text())
    declared = {key: run["identity"][key] for key in
                ["database", "accounts", "start_block", "module_hash", "final_blocks_only"]}
    if declared["start_block"] != record["block"] - 1:
        raise VerificationError("expected the captured block and its predecessor")
    accounts = set(record["rpc_block_end_state"]) - {record["from"]}
    if not accounts or not accounts.issubset(declared["accounts"]):
        raise VerificationError("native filter is missing captured SELFDESTRUCT accounts")
    client, rpc = ClickHouse(args.database), RPC()
    if int(rpc.call("eth_chainId", []), 16) != 56:
        raise VerificationError("expected BSC RPC")
    params = {"start0": declared["start_block"], "end": record["block"]}
    comparisons = {}
    with verified_source(client, declared, end_block=record["block"]) as source:
        if source["state_directory"] != str(directory):
            raise VerificationError("directory does not own the native source")
        blocks = validate_interval(client, source, record["header"])
        row = client.one("SELECT hash,`lifecycle.address`,`lifecycle.kind`,`lifecycle.ordinal`,"
                         "`codes.address`,`nonces.address`,`storage.address` FROM state_blocks FINAL "
                         "WHERE number={end:UInt64}", params)
        current = rpc.call("eth_getBlockByNumber", [hex(record["block"]), False])
        if row["hash"] != record["block_hash"] or current["hash"].lower() != row["hash"]:
            raise VerificationError("native/captured/current RPC block identity differs")
        if any(accounts.intersection(row[key]) for key in ["codes.address", "nonces.address", "storage.address"]):
            raise VerificationError("expected no nonce, code or storage patch for the selected accounts")
        markers = [item for item in zip(row["lifecycle.address"], row["lifecycle.kind"], row["lifecycle.ordinal"],
                                       strict=True) if item[0] in accounts]
        if {address for address, _, _ in markers} != accounts or any(
                kind != "selfdestruct" for _, kind, _ in markers):
            raise VerificationError("expected diagnostic SELFDESTRUCT markers without account deletion")
        observed = _observed_fields(client, [source], None, params)
        for address in sorted(accounts):
            saved = record["rpc_block_end_state"][address]
            before, after = saved["before"], saved["after"]
            if before["code"] == "0x" or before["code"] != after["code"] or before["nonce"] != after["nonce"]:
                raise VerificationError("capture does not demonstrate an existing account surviving SELFDESTRUCT")
            for side in [before, after]:
                number = hex(side["block_number"])
                if rpc.call("eth_getCode", [address, number]).lower() != side["code"] or int(
                        rpc.call("eth_getTransactionCount", [address, number]), 16) != side["nonce"]:
                    raise VerificationError("archive code/nonce differs from the capture")
            fields = observed.get(address, {})
            if any(after[key] != actual for key, actual in fields.items()):
                raise VerificationError("observed native metadata differs from archive RPC")
            comparisons[address] = {"code_hash_before_and_after": before["code_hash"],
                "code_bytes_before_and_after": (len(before["code"]) - 2) // 2,
                "nonce_before_and_after": before["nonce"], "observed_native_fields": fields,
                "selfdestruct_ordinals": [ordinal for account, _, ordinal in markers if account == address]}
    result = {"format_version": 1, "chain_id": 56, "database": client.database,
        "start_block": declared["start_block"], "blocks": blocks, "header": record["header"],
        "fixture": args.fixture, "fixture_sha256": record["sha256"], "transaction_hash": record["hash"],
        "run_id": run["run_id"], "module_hash": declared["module_hash"],
        "package_sha256": run["identity"]["package_sha256"], "comparisons": comparisons,
        "qualification": "Native diagnostic markers and unchanged code/nonce agree with captured archive RPC. "
                         "No complete initial storage, historical account-root proof or ready checkpoint is claimed."}
    atomic_json(args.output, result)
    print(json.dumps({"blocks": blocks, "accounts": len(comparisons), "output": str(args.output)}))


if __name__ == "__main__":
    main()
