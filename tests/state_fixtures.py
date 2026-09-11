"""Synthetic chain state with actual Ethereum trie proofs, for adversarial tests."""
import rlp
from trie import HexaryTrie

from evm_state.proof import keccak256, storage_root

A = "0x" + "11" * 20
B = "0x" + "22" * 20
CODE = "0x60006000"


def word(value):
    return "0x" + f"{value:064x}"


def state(slots=None, nonce=1, balance=25, code=CODE, exists=True):
    return {"slots": slots or {}, "nonce": nonce, "balance": balance, "code": code, "exists": exists}


def proof_bundle(number, accounts):
    tree = HexaryTrie({})
    metadata = {}
    for account, data in accounts.items():
        root, _ = storage_root((word(k), word(v)) for k, v in data["slots"].items() if v)
        code_hash = keccak256(bytes.fromhex(data["code"][2:]))
        if data["exists"]:
            tree[keccak256(bytes.fromhex(account[2:]))] = rlp.encode([data["nonce"], data["balance"], root, code_hash])
        metadata[account] = {"address": account, "nonce": hex(data["nonce"]), "balance": hex(data["balance"]),
                             "storageHash": "0x" + root.hex(), "codeHash": "0x" + code_hash.hex()}
    return {"format_version": 1, "chain_id": 56,
        "header": {"number": number, "hash": word(number), "parent_hash": word(number - 1),
                   "state_root": "0x" + tree.root_hash.hex(), "timestamp": 1700000000 + number},
        "header_trust": "provider-finalized-header",
        "accounts": {account: {"code": data["code"], "proof": {**metadata[account],
            "accountProof": ["0x" + rlp.encode(node).hex() for node in tree.get_proof(keccak256(bytes.fromhex(account[2:])))]}}
            for account, data in accounts.items()}}


def block(number, bundle, selected=None, storage=None, balances=None, nonces=None, codes=None, lifecycle=None):
    out = {"number": number, "hash": word(number), "parent_hash": word(number - 1),
           "timestamp": 1700000000 + number, "state_root": bundle["header"]["state_root"],
           "accounts": ",".join(sorted(selected or bundle["accounts"])), "producer_version": 5, "schema_version": 1,
           "storage": [], "balances": [], "nonces": [], "codes": [], "lifecycle": lifecycle or []}
    for i, ((account, slot), value) in enumerate((storage or {}).items()):
        out["storage"].append({"address": account, "slot": word(slot), "value": word(value), "ordinal": i + 1})
    for group, values in [("balances", balances), ("nonces", nonces)]:
        for account, value in (values or {}).items():
            out[group].append({"address": account, "value": str(value) if group == "balances" else value, "ordinal": 10})
    for account, code in (codes or {}).items():
        out["codes"].append({"address": account, "code": code,
                             "hash": "0x" + keccak256(bytes.fromhex(code[2:])).hex(), "ordinal": 20})
    return out


def insert_blocks(client, blocks):
    """Insert synthetic native rows using the schema generated from the release package."""
    rows = []
    for i, data in enumerate(blocks):
        row = {k: v for k, v in data.items() if k not in {"storage", "balances", "nonces", "codes", "lifecycle"}}
        row.update(_block_number_=data["number"], _block_timestamp_=data["timestamp"], _version_=i + 1, _deleted_=False)
        for group, fields in [("storage", ["address", "slot", "value", "ordinal"]),
            ("balances", ["address", "value", "ordinal"]), ("nonces", ["address", "value", "ordinal"]),
            ("codes", ["address", "hash", "code", "ordinal"]), ("lifecycle", ["address", "kind", "ordinal"])]:
            for field in fields:
                row[group + "." + field] = [v[field] for v in data[group]]
        rows.append(row)
    client.insert("state_blocks", rows)


def source(client, start, accounts=(A,)):
    return {"database": client.database, "start_block": start, "accounts": list(accounts),
            "final_blocks_only": True, "module_hash": "synthetic-fixture"}
