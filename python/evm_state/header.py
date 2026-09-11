"""Bind a checkpoint state root to its header hash; this does not verify consensus.

Field order follows bnb-chain/bsc core/types/Header and gen_header_rlp.go.
Unknown future encodings fail the hash check rather than silently trusting a
provider's hash property.
"""
import rlp

from .proof import VerificationError, keccak256, quantity, unhex

# (RPC name, fixed byte length or None, integer bit width or None)
FIELDS = [
    ("parentHash", 32, None), ("sha3Uncles", 32, None), ("miner", 20, None),
    ("stateRoot", 32, None), ("transactionsRoot", 32, None), ("receiptsRoot", 32, None),
    ("logsBloom", 256, None), ("difficulty", None, 256), ("number", None, 64),
    ("gasLimit", None, 64), ("gasUsed", None, 64), ("timestamp", None, 64),
    ("extraData", None, None), ("mixHash", 32, None), ("nonce", 8, None),
    ("baseFeePerGas", None, 256), ("withdrawalsRoot", 32, None),
    ("blobGasUsed", None, 64), ("excessBlobGas", None, 64),
    ("parentBeaconBlockRoot", 32, None), ("requestsHash", 32, None),
    ("balHash", 32, None), ("slotNumber", None, 64),
]


def encode_rpc_header(header):
    last = max([14] + [i for i, (name, _, _) in enumerate(FIELDS) if header.get(name) is not None])
    fields = []
    for i, (name, length, bits) in enumerate(FIELDS[:last + 1]):
        value = header.get(name)
        if value is None:
            if i < 15:
                raise VerificationError(f"RPC header missing {name}")
            fields.append(b"")
        elif bits:
            fields.append(quantity(value, bits))
        else:
            fields.append(unhex(value, length))
    encoded = rlp.encode(fields)
    if keccak256(encoded) != unhex(header["hash"], 32):
        raise VerificationError("encoded RPC header does not match its block hash")
    return "0x" + encoded.hex()


def verify_header(encoded_hex, summary, expected_hash=None):
    encoded = unhex(encoded_hex)
    if len(encoded) > 102_400 + 2048:
        raise VerificationError("oversized block header")
    try:
        fields = rlp.decode(encoded)
        if not isinstance(fields, list) or not 15 <= len(fields) <= len(FIELDS):
            raise ValueError("unsupported header field count")
        for i, (value, (_, length, bits)) in enumerate(zip(fields, FIELDS)):
            if not isinstance(value, bytes):
                raise ValueError("header fields must be bytes")
            if bits and (value.startswith(b"\0") or len(value) > bits // 8):
                raise ValueError("invalid header integer")
            if length and len(value) != length and not (i >= 15 and value == b""):
                raise ValueError("invalid header field length")
    except (ValueError, rlp.DecodingError) as error:
        raise VerificationError("invalid RLP block header") from error
    digest = keccak256(encoded)
    if digest != unhex(summary["hash"], 32) or (expected_hash and digest != unhex(expected_hash, 32)):
        raise VerificationError("encoded header differs from checkpoint block hash")
    if (fields[0] != unhex(summary["parent_hash"], 32) or fields[3] != unhex(summary["state_root"], 32)
            or int.from_bytes(fields[8], "big") != summary["number"]
            or int.from_bytes(fields[11], "big") != summary["timestamp"]):
        raise VerificationError("checkpoint metadata differs from its encoded header")
    return digest
