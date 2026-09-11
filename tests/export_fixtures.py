"""Complete synthetic checkpoint with an actually hash-committed header."""
import rlp

from evm_state.checkpoint import build
from evm_state.header import FIELDS, encode_rpc_header
from evm_state.proof import keccak256
from state_fixtures import A, block, insert_blocks, proof_bundle, source, state


def complete_bundle(number, values, parent_hash=None):
    bundle = proof_bundle(number, values)
    header = {}
    for name, length, bits in FIELDS[:15]:
        header[name] = "0x0" if bits else "0x" + "00" * (length or 0)
    header.update(number=hex(number), timestamp=hex(bundle["header"]["timestamp"]),
                  stateRoot=bundle["header"]["state_root"], parentHash=parent_hash or bundle["header"]["parent_hash"])
    fields = [int(header[name], 16) if bits else bytes.fromhex(header[name][2:]) for name, _, bits in FIELDS[:15]]
    header["hash"] = "0x" + keccak256(rlp.encode(fields)).hex()
    bundle["header"].update(hash=header["hash"], parent_hash=header["parentHash"])
    bundle["header_rlp"] = encode_rpc_header(header)
    return bundle


def checkpoint(databases, values=None, number=100, target=None):
    target = target or databases(False)
    stream = databases()
    values = values or {A: state({0: 1, 1: 2, 2: 3, 7: 8, 99: 100})}
    bundle = complete_bundle(number, values)
    changes = {(account, key): value for account, data in values.items() for key, value in data["slots"].items()}
    row = block(number, bundle, storage=changes)
    row.update(hash=bundle["header"]["hash"], parent_hash=bundle["header"]["parent_hash"])
    insert_blocks(stream, [row])
    ready = build(target, bundle, [source(stream, number, values, target=target)])
    return target, ready
