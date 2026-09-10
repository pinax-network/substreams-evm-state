#!/usr/bin/env python3
"""Cross-check the Postgres state tables against a JSON-RPC node.

For every row in `storage` and `accounts` (optionally restricted to one
address) compare with eth_getStorageAt / eth_getBalance /
eth_getTransactionCount / eth_getCode at BLOCK (default: the DB head block).
Also compares the head block hash and state root with eth_getBlockByNumber.

Usage:
  PG_DSN=postgresql://user:pass@host:5432/db \
  RPC_URL=https://bsc.rpc.pinax.network RPC_API_KEY=<key> \
  python3 scripts/verify_rpc.py [--block N] [--address 0x...] [--limit N]

The API key is read from the environment only; never commit it.
"""
import argparse, json, os, subprocess, sys, urllib.request

PG_DSN = os.environ.get("PG_DSN", "postgresql://dev-node:insecure-change-me-in-prod@localhost:5432/dev-node")
RPC_URL = os.environ.get("RPC_URL", "https://bsc.rpc.pinax.network")
RPC_API_KEY = os.environ.get("RPC_API_KEY", "")


def sql(q):
    out = subprocess.run(["psql", PG_DSN, "-At", "-F", "|", "-c", q], capture_output=True, text=True, check=True).stdout
    return [line.split("|") for line in out.strip().splitlines() if line]


def rpc(method, params):
    headers = {"Content-Type": "application/json"}
    if RPC_API_KEY:
        headers["X-Api-Key"] = RPC_API_KEY
    req = urllib.request.Request(RPC_URL, data=json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode(), headers=headers)
    res = json.load(urllib.request.urlopen(req, timeout=30))
    if "error" in res:
        sys.exit(f"rpc error: {res['error']}")
    return res["result"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--block", type=int, help="block to compare at (default: DB head)")
    ap.add_argument("--address", help="restrict to one account")
    ap.add_argument("--limit", type=int, default=1000, help="max storage slots to check")
    a = ap.parse_args()

    block = a.block or int(sql("SELECT max(block_num) FROM blocks")[0][0])
    tag = hex(block)
    where = f"WHERE address = '{a.address.lower()}'" if a.address else ""
    ok = bad = 0

    hdr = rpc("eth_getBlockByNumber", [tag, False])
    db = sql(f"SELECT block_hash, state_root FROM blocks WHERE block_num = {block}")
    if db:
        h_ok = hdr["hash"].lower() == db[0][0].lower()
        r_ok = hdr["stateRoot"].lower() == db[0][1].lower()
        print(f"block {block}: hash {'OK' if h_ok else 'MISMATCH'}, state_root {'OK' if r_ok else 'MISMATCH'}")
        ok += h_ok + r_ok
        bad += (not h_ok) + (not r_ok)

    rows = sql(f"SELECT address, slot, value FROM storage {where} ORDER BY address, slot LIMIT {a.limit}")
    for address, slot, value in rows:
        got = rpc("eth_getStorageAt", [address, slot, tag])
        if got.lower() == value.lower():
            ok += 1
        else:
            bad += 1
            print(f"MISMATCH storage {address} {slot}: db={value} rpc={got}")
    print(f"storage slots checked: {len(rows)}")

    for address, balance, nonce, code_hash in sql(f"SELECT address, coalesce(balance::text,''), coalesce(nonce::text,''), coalesce(code_hash,'') FROM accounts {where}"):
        if balance:
            got = int(rpc("eth_getBalance", [address, tag]), 16)
            if got == int(balance):
                ok += 1
            else:
                bad += 1
                print(f"MISMATCH balance {address}: db={balance} rpc={got}")
        if nonce:
            got = int(rpc("eth_getTransactionCount", [address, tag]), 16)
            if got == int(nonce):
                ok += 1
            else:
                bad += 1
                print(f"MISMATCH nonce {address}: db={nonce} rpc={got}")
        if code_hash:
            got = rpc("eth_getCode", [address, tag])
            db_code = sql(f"SELECT '0x' || encode(code, 'hex') FROM code WHERE code_hash = '{code_hash}'")
            if db_code and db_code[0][0].lower() == got.lower():
                ok += 1
            else:
                bad += 1
                print(f"MISMATCH code {address}: code_hash={code_hash} rpc_len={len(got)}")

    print(f"checks ok={ok} mismatch={bad}")
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    main()
