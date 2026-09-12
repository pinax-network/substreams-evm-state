#!/usr/bin/env python3
"""Check captured failed authorization clears at their exact native block ends.

Archive metadata and actual patch/marker checks prevent a later update from
masking a bad clear. This diagnostic does not prove complete initial storage.
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


CASES = {
    "v4-failed-clear-reinstall.pb": ("0x2eecb88952aced531a7b29ac7320feca57e73a62", 9041),
    "v5-failed-authority-clear.pb": ("0x73d718b4cf0d2d86eb4ac522f6fedf599bbbdfb7", 3442),
    "v5-failed-self-clear.pb": ("0xbbb90cdb4e271be14df46b7e84f4fbf3bab17b6e", 1170),
    "v5-invalid-self-clear-noop.pb": ("0x213864e51cdacf3fdacbdc12726dba9f12167514", None),
}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True)
    parser.add_argument("--state-dir", type=Path, required=True)
    parser.add_argument("--fixture", action="append", choices=CASES, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        parser.error("output already exists")
    fixture_dir = Path(__file__).resolve().parents[1] / "tests/fixtures/lifecycle"
    manifest = json.loads((fixture_dir / "manifest.json").read_text())
    records = sorted([r for r in manifest["records"] if r["filename"] in args.fixture], key=lambda r: r["block"])
    if len(records) != len(set(args.fixture)):
        raise VerificationError("missing captured case")
    directory = args.state_dir.resolve()
    run = json.loads((directory / "run.json").read_text())
    declared = {key: run["identity"][key] for key in
                ["database", "accounts", "start_block", "module_hash", "final_blocks_only"]}
    if declared["start_block"] >= records[0]["block"]:
        raise VerificationError("native interval must include the preceding block")
    client, rpc = ClickHouse(args.database), RPC()
    if int(rpc.call("eth_chainId", []), 16) != 56:
        raise VerificationError("expected BSC RPC")
    comparisons = []
    with verified_source(client, declared, end_block=records[-1]["block"]) as source:
        if source["state_directory"] != str(directory):
            raise VerificationError("directory does not own the native source")
        blocks = validate_interval(client, source, records[-1]["header"])
        for record in records:
            verify_header(record["header_rlp"], record["header"], record["block_hash"])
            if hashlib.sha256((fixture_dir / record["filename"]).read_bytes()).hexdigest() != record["sha256"]:
                raise VerificationError("captured transaction changed")
            captured = record["rpc_block_end_state"]
            if not set(captured).issubset(declared["accounts"]):
                raise VerificationError("native filter does not cover the captured accounts")
            params = {"start0": declared["start_block"], "end": record["block"]}
            observed = _observed_fields(client, [source], None, params)
            row = client.one("SELECT hash,`lifecycle.address`,`lifecycle.kind`,`lifecycle.ordinal`,"
                             "`codes.address`,`codes.code`,`codes.ordinal`,`storage.address` "
                             "FROM state_blocks FINAL WHERE number={end:UInt64}", params)
            live = rpc.call("eth_getBlockByNumber", [hex(record["block"]), False])
            if row["hash"] != record["block_hash"] or live["hash"].lower() != row["hash"]:
                raise VerificationError("native/captured/current RPC block identity differs")
            fields = {}
            for address, sides in captured.items():
                before, after = sides["before"], sides["after"]
                if "nonce" not in observed.get(address, {}) or any(
                        after[key] != value for key, value in observed[address].items()):
                    raise VerificationError("observed native metadata differs from captured archive RPC")
                for state in [before, after]:
                    number = hex(state["block_number"])
                    if (rpc.call("eth_getCode", [address, number]).lower() != state["code"] or
                            int(rpc.call("eth_getTransactionCount", [address, number]), 16) != state["nonce"] or
                            str(int(rpc.call("eth_getBalance", [address, number]), 16)) != state["balance"]):
                        raise VerificationError("archive metadata differs from the capture")
                fields[address] = {key: value for key, value in observed[address].items() if key != "code"}
            authority, ordinal = CASES[record["filename"]]
            codes = [dict(code=code, ordinal=position) for address, code, position in
                     zip(row["codes.address"], row["codes.code"], row["codes.ordinal"], strict=True)
                     if address == authority]
            markers = [dict(kind=kind, ordinal=position) for address, kind, position in
                       zip(row["lifecycle.address"], row["lifecycle.kind"], row["lifecycle.ordinal"], strict=True)
                       if address == authority]
            before, after = captured[authority]["before"], captured[authority]["after"]
            if row["storage.address"]:
                raise VerificationError("unexpected storage patches in captured clear block")
            if ordinal is None:
                if codes or markers or before["code"] == "0x" or before["code"] != after["code"]:
                    raise VerificationError("invalid authorization unexpectedly clears delegation")
            elif record["filename"] == "v4-failed-clear-reinstall.pb":
                if (codes != [{"code": after["code"], "ordinal": 9043}] or
                        markers != [{"kind": "code_cleared", "ordinal": ordinal}] or
                        before["code"] == "0x" or after["code"] == "0x" or before["code"] == after["code"] or
                        observed[authority].get("code") != after["code"]):
                    raise VerificationError("native clear/reinstallation ordering differs from captured case")
            elif (codes != [{"code": "0x", "ordinal": ordinal}] or
                  markers != [{"kind": "code_cleared", "ordinal": ordinal}] or
                  before["code"] == "0x" or after["code"] != "0x" or
                  observed[authority].get("code") != "0x"):
                raise VerificationError("native pre-execution clear differs from captured case")
            comparisons.append({"fixture": record["filename"], "fixture_sha256": record["sha256"],
                "block": record["block"], "block_hash": row["hash"], "transaction_hash": record["hash"],
                "authority": authority, "native_metadata": fields, "native_code_patches": codes,
                "native_lifecycle": markers, "storage_patches": 0,
                "code_bytes_before": (len(before["code"]) - 2) // 2,
                "code_bytes_after": (len(after["code"]) - 2) // 2})
    result = {"format_version": 1, "chain_id": 56, "database": client.database,
        "start_block": declared["start_block"], "header": records[-1]["header"], "blocks": blocks,
        "run_id": run["run_id"], "module_hash": declared["module_hash"],
        "package_sha256": run["identity"]["package_sha256"], "comparisons": comparisons,
        "qualification": "Exact captured block-end updates and clear markers match archive RPC. "
                         "No untouched storage completeness, account-root proof or ready checkpoint is claimed."}
    atomic_json(args.output, result)
    print(json.dumps({"blocks": blocks, "cases": len(comparisons), "output": str(args.output)}))


if __name__ == "__main__":
    main()
