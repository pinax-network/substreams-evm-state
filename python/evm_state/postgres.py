"""Consistent, fail-closed diagnostics for the legacy PostgreSQL current tables.

These commands do not publish checkpoints. The root check proves captured state
at the exact database head; the sampled RPC check cannot establish completeness.
The legacy snapshot is materialized in memory, so use ClickHouse's streaming
checkpoint verifier for large accounts.
"""
import argparse
import json
import os
import subprocess

from .proof import VerificationError, address, unhex, verify_account, verify_complete, verify_metadata
from .rpc import RPC


def snapshot(selected=None, limit=None, dsn=None):
    # Only normalized, fixed-length hex and validated integers reach SQL. A
    # single statement gives every subquery the same PostgreSQL MVCC snapshot.
    selected = address(selected) if selected is not None else None
    if limit is not None and (isinstance(limit, bool) or not isinstance(limit, int) or limit < 1):
        raise ValueError("storage sample limit must be a positive integer")
    where = f"WHERE a.address='{selected}'" if selected else ""
    storage_where = f"WHERE address='{selected}'" if selected else ""
    bound = f"LIMIT {limit}" if limit is not None else ""
    query = f"""SELECT json_build_object(
        'header', (SELECT row_to_json(h) FROM
            (SELECT block_num AS number,block_hash AS hash,state_root FROM blocks ORDER BY block_num DESC LIMIT 1) h),
        'accounts', (SELECT coalesce(json_agg(row_to_json(a)), '[]'::json) FROM
            (SELECT a.address,a.nonce::text AS nonce,a.balance::text AS balance,a.code_hash,
                '0x' || encode(c.code,'hex') AS code,
                greatest(a.block_num,a.balance_block_num,a.nonce_block_num,a.code_block_num,c.first_block_num) AS last_block
             FROM accounts a LEFT JOIN code c ON a.code_hash=c.code_hash {where} ORDER BY a.address) a),
        'storage', (SELECT coalesce(json_agg(row_to_json(s)), '[]'::json) FROM
            (SELECT address,slot,value,block_num AS last_block FROM storage {storage_where}
             ORDER BY address,slot {bound}) s))"""
    try:
        result = subprocess.run(["psql", "-XAt", "--set=ON_ERROR_STOP=1", "--dbname", dsn or os.environ.get(
            "PG_DSN", "postgresql://dev-node:insecure-change-me-in-prod@localhost:5432/dev-node"), "-c", query],
            capture_output=True, text=True, timeout=120)
    except subprocess.TimeoutExpired:
        # TimeoutExpired includes command arguments, potentially the DSN password.
        raise RuntimeError("PostgreSQL snapshot query timed out") from None
    if result.returncode:
        raise RuntimeError(f"PostgreSQL snapshot query failed (exit {result.returncode}); check the database connection and schema")
    value = json.loads(result.stdout)
    if selected and [row["address"] for row in value["accounts"]] != [selected]:
        raise VerificationError("selected account has no complete metadata row in the database")
    return value


def validate_snapshot(value, requested_block=None):
    header = value.get("header")
    if not header or not value.get("accounts"):
        raise VerificationError("database snapshot has no head or account metadata")
    number = header["number"]
    if isinstance(number, bool) or not isinstance(number, int) or number < 0:
        raise VerificationError("invalid database head")
    if requested_block is not None and requested_block != number:
        raise VerificationError("current tables only support their captured head; --block cannot select historical state")
    unhex(header["hash"], 32)
    unhex(header["state_root"], 32)
    accounts = {}
    for row in value["accounts"]:
        selected = address(row["address"])
        if selected in accounts:
            raise VerificationError("duplicate database account")
        if any(row.get(k) is None for k in ["nonce", "balance", "code_hash", "code"]):
            raise VerificationError("missing required account metadata or bytecode; partial state cannot pass verification")
        try:
            nonce = int(row["nonce"])
            if str(nonce) != str(row["nonce"]) or isinstance(row["nonce"], bool):
                raise ValueError()
        except (ValueError, TypeError) as error:
            raise VerificationError("database nonce must be an exact integer") from error
        if not 0 <= nonce < 2**64:
            raise VerificationError("database nonce exceeds uint64")
        unhex(row["code_hash"], 32)
        unhex(row["code"])
        accounts[selected] = {**row, "nonce": nonce}
    keys = set()
    for row in value["accounts"] + value["storage"]:
        last = row.get("last_block")
        if isinstance(last, bool) or not isinstance(last, int) or not 0 <= last <= number:
            raise VerificationError("database state is newer than its block marker or has invalid provenance")
    for row in value["storage"]:
        if address(row["address"]) not in accounts:
            raise VerificationError("storage account has no verified metadata")
        unhex(row["slot"], 32)
        unhex(row["value"], 32)
        key = (row["address"], row["slot"])
        if key in keys:
            raise VerificationError("duplicate database storage slot")
        keys.add(key)
    return accounts


def verify(value, bundle, complete=False, rpc=None, requested_block=None):
    accounts = validate_snapshot(value, requested_block)
    header = value["header"]
    if bundle["chain_id"] != 56 or any(header[k] != bundle["header"][k] for k in ["number", "hash", "state_root"]):
        raise VerificationError("database head differs from the finalized BSC proof header")
    if set(accounts) != set(bundle["accounts"]):
        raise VerificationError("account coverage differs from captured proofs")
    count = 0
    for selected, metadata in accounts.items():
        proven = verify_account(header["state_root"], selected, bundle["accounts"][selected]["proof"])
        if complete:
            count += verify_complete(proven, ((row["slot"], row["value"]) for row in value["storage"]
                if row["address"] == selected and int(row["value"], 16)), metadata["code"], metadata)
        else:
            verify_metadata(proven, metadata["code"], metadata)
    if not complete:
        if rpc is None:
            raise ValueError("sampled storage verification requires RPC")
        for row in value["storage"]:
            got = rpc.call("eth_getStorageAt", [row["address"], row["slot"], hex(header["number"])])
            if unhex(got, 32) != unhex(row["value"], 32):
                raise VerificationError(f"storage sample mismatch for {row['address']} slot {row['slot']}")
            count += 1
        after = rpc.call("eth_getBlockByNumber", [hex(header["number"]), False])
        if after["hash"].lower() != header["hash"]:
            raise VerificationError("RPC header changed during sampled verification")
    return {"status": "root-verified-diagnostic" if complete else "sample-parity-only",
            "header": header, "header_trust": bundle["header_trust"], "accounts": sorted(accounts),
            "storage_slots_checked": count, "storage_completeness_verified": complete,
            "account_metadata_and_code": "verified", "published_checkpoint": False}


def main(complete=False, argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("address" if complete else "--address")
    parser.add_argument("--block", type=int, help="must equal the captured DB head; current tables have no historical reads")
    if not complete:
        parser.add_argument("--limit", type=int, default=1000)
    args = parser.parse_args(argv)
    try:
        value = snapshot(args.address, None if complete else args.limit)
        accounts = validate_snapshot(value, args.block)
        rpc = RPC()
        bundle = rpc.capture(accounts, value["header"]["number"])
        result = verify(value, bundle, complete, rpc, args.block)
        print(json.dumps(result, sort_keys=True, indent=2))
    except (ValueError, RuntimeError, OSError, KeyError, subprocess.TimeoutExpired) as error:
        parser.exit(1, f"verification failed: {error}\n")
