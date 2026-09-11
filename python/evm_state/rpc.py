"""Capture proofs before backprocessing, while recent state is still available."""
import json
import os
import urllib.request

from .proof import VerificationError, address, quantity, unhex, verify_account, keccak256
from .header import encode_rpc_header, verify_header


class RPC:
    def __init__(self, url=None, key=None):
        self.url = url or os.environ.get("RPC_URL", "https://bsc.rpc.pinax.network")
        self.key = key if key is not None else os.environ.get("RPC_API_KEY", os.environ.get("SUBSTREAMS_API_KEY", ""))

    def call(self, method, params):
        headers = {"Content-Type": "application/json"}
        if self.key:
            headers["X-Api-Key"] = self.key
        request = urllib.request.Request(self.url, headers=headers,
            data=json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode())
        with urllib.request.urlopen(request, timeout=60) as response:
            result = json.load(response)
        if "error" in result or result.get("result") is None:
            raise VerificationError(f"RPC {method} failed: {result.get('error', 'null result')}")
        return result["result"]

    def capture(self, accounts, block="finalized", expected_hash=None):
        accounts = sorted({address(a) for a in accounts})
        if not accounts:
            raise ValueError("at least one account is required")
        finalized = self.call("eth_getBlockByNumber", ["finalized", False])
        header = finalized if block == "finalized" else self.call("eth_getBlockByNumber", [hex(int(block)), False])
        number = quantity(header["number"], 64)
        if number > quantity(finalized["number"], 64):
            raise VerificationError("checkpoint block is not finalized")
        if expected_hash is not None and unhex(expected_hash, 32) != unhex(header["hash"], 32):
            raise VerificationError("RPC header differs from the expected block hash")
        bundle = {
            "format_version": 1, "chain_id": quantity(self.call("eth_chainId", [])),
            "header": {"number": number, "hash": header["hash"].lower(),
                "parent_hash": header["parentHash"].lower(), "state_root": header["stateRoot"].lower(),
                "timestamp": quantity(header["timestamp"], 64)},
            "header_trust": "operator-pinned-hash" if expected_hash else "provider-finalized-header",
            "accounts": {},
        }
        bundle["header_rlp"] = encode_rpc_header(header)
        verify_header(bundle["header_rlp"], bundle["header"], expected_hash)
        for account in accounts:
            proof = self.call("eth_getProof", [account, [], hex(number)])
            proven = verify_account(header["stateRoot"], account, proof)
            code = self.call("eth_getCode", [account, hex(number)])
            if keccak256(unhex(code)) != proven.code_hash:
                raise VerificationError("RPC bytecode differs from the proven code hash")
            bundle["accounts"][account] = {"proof": proof, "code": code.lower()}
        after = self.call("eth_getBlockByNumber", [hex(number), False])
        if after["hash"].lower() != header["hash"].lower():
            raise VerificationError("checkpoint header changed during proof capture")
        return bundle
