#!/usr/bin/env python3
"""Verify storage completeness for one account by recomputing its storage
trie root from `storage_nonzero` and comparing to eth_getProof(...).storageHash.

If the account was tracked from its creation block, the two roots must match;
a match proves the `storage` table holds every non-zero slot at that block.

Usage:
  PG_DSN=postgresql://... RPC_URL=https://bsc.rpc.pinax.network RPC_API_KEY=<key> \
  python3 scripts/verify_storage_root.py 0x<address> [--block N]

--block defaults to the DB head block. Most RPC nodes only serve eth_getProof
for recent blocks ("distance to target block exceeds maximum proof window"), so
verify right after the sink reaches the block you care about.

Requires: pycryptodome (`pip install pycryptodome`) for keccak256.
"""
import argparse, json, os, subprocess, sys, urllib.request

from Crypto.Hash import keccak  # pycryptodome

PG_DSN = os.environ.get("PG_DSN", "postgresql://dev-node:insecure-change-me-in-prod@localhost:5432/dev-node")
RPC_URL = os.environ.get("RPC_URL", "https://bsc.rpc.pinax.network")
RPC_API_KEY = os.environ.get("RPC_API_KEY", "")

EMPTY_ROOT = "0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421"


def keccak256(b: bytes) -> bytes:
    return keccak.new(digest_bits=256, data=b).digest()


# -- minimal RLP -----------------------------------------------------------
def rlp_len(n, offset):
    if n < 56:
        return bytes([offset + n])
    lb = n.to_bytes((n.bit_length() + 7) // 8, "big")
    return bytes([offset + 55 + len(lb)]) + lb


def rlp(x):
    if isinstance(x, bytes):
        if len(x) == 1 and x[0] < 0x80:
            return x
        return rlp_len(len(x), 0x80) + x
    body = b"".join(rlp(i) for i in x)
    return rlp_len(len(body), 0xC0) + body


# -- Merkle Patricia Trie root ---------------------------------------------
def nibbles(b: bytes):
    out = []
    for c in b:
        out += [c >> 4, c & 0xF]
    return out


def hp(path, leaf):
    """Hex-prefix encoding."""
    flag = 2 if leaf else 0
    if len(path) % 2:
        return bytes([(flag + 1) << 4 | path[0]] + [path[i] << 4 | path[i + 1] for i in range(1, len(path), 2)])
    return bytes([flag << 4] + [path[i] << 4 | path[i + 1] for i in range(0, len(path), 2)])


def node_ref(encoded: bytes):
    return encoded if len(encoded) < 32 else keccak256(encoded)


def build(items):
    """items: list of (nibble_list, value_bytes), all keys distinct, sorted. Returns RLP of the node."""
    if not items:
        return b""
    if len(items) == 1:
        path, value = items[0]
        return rlp([hp(path, True), value])
    # common prefix
    first = items[0][0]
    prefix = 0
    while all(len(k) > prefix and k[prefix] == first[prefix] for k, _ in items):
        prefix += 1
    if prefix:
        child = build([(k[prefix:], v) for k, v in items])
        return rlp([hp(first[:prefix], False), node_ref(child)])
    branches = [b""] * 17
    groups = {}
    for k, v in items:
        if not k:
            branches[16] = v
        else:
            groups.setdefault(k[0], []).append((k[1:], v))
    for nib, sub in groups.items():
        child = build(sub)
        branches[nib] = node_ref(child)
    return rlp(branches)


def storage_root(slots):
    """slots: iterable of (slot_bytes32, value_bytes32) with non-zero values."""
    items = []
    for slot, value in slots:
        v = value.lstrip(b"\x00")
        if not v:
            continue
        items.append((nibbles(keccak256(slot)), rlp(v)))
    items.sort(key=lambda kv: kv[0])
    if not items:
        return EMPTY_ROOT
    return "0x" + keccak256(build(items)).hex()


# -- glue ------------------------------------------------------------------
def sql(q):
    out = subprocess.run(["psql", PG_DSN, "-At", "-F", "|", "-c", q], capture_output=True, text=True, check=True).stdout
    return [line.split("|") for line in out.strip().splitlines() if line]


def rpc(method, params):
    headers = {"Content-Type": "application/json"}
    if RPC_API_KEY:
        headers["X-Api-Key"] = RPC_API_KEY
    req = urllib.request.Request(RPC_URL, data=json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode(), headers=headers)
    res = json.load(urllib.request.urlopen(req, timeout=60))
    if "error" in res:
        sys.exit(f"rpc error: {res['error']}")
    return res["result"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("address")
    ap.add_argument("--block", type=int, help="block to compare at (default: DB head)")
    a = ap.parse_args()
    address = a.address.lower()
    block = a.block or int(sql("SELECT max(block_num) FROM blocks")[0][0])

    rows = sql(f"SELECT slot, value FROM storage_nonzero WHERE address = '{address}'")
    slots = [(bytes.fromhex(s[2:]).rjust(32, b"\x00"), bytes.fromhex(v[2:]).rjust(32, b"\x00")) for s, v in rows]
    local = storage_root(slots)

    proof = rpc("eth_getProof", [address, [], hex(block)])
    remote = proof["storageHash"].lower()
    print(f"account       {address}")
    print(f"block         {block}")
    print(f"nonzero slots {len(slots)}")
    print(f"local  root   {local}")
    print(f"remote root   {remote}")
    ok = local == remote
    print("RESULT        " + ("MATCH — storage table is complete for this account at this block" if ok else "MISMATCH — storage table is incomplete or stale (was the account tracked from its creation block?)"))

    acct = sql(f"SELECT coalesce(nonce::text,''), coalesce(balance::text,''), coalesce(code_hash,'') FROM accounts WHERE address = '{address}'")
    if acct:
        nonce, balance, code_hash = acct[0]
        if nonce:
            print(f"nonce         db={nonce} rpc={int(proof['nonce'], 16)} {'OK' if int(nonce) == int(proof['nonce'], 16) else 'MISMATCH'}")
        if balance:
            print(f"balance       db={balance} rpc={int(proof['balance'], 16)} {'OK' if int(balance) == int(proof['balance'], 16) else 'MISMATCH'}")
        if code_hash:
            print(f"code_hash     db={code_hash} rpc={proof['codeHash']} {'OK' if code_hash.lower() == proof['codeHash'].lower() else 'MISMATCH'}")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
