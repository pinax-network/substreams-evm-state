"""Account and complete-storage verification against an explicitly chosen state root.

This verifies trie proofs, not BSC consensus. The caller records how it trusts
the finalized block header; a provider-supplied header is a distinct trust source.
"""
from dataclasses import dataclass
import re

from Crypto.Hash import keccak
import rlp
from trie import HexaryTrie

EMPTY_STORAGE_ROOT = bytes.fromhex("56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421")


class VerificationError(ValueError):
    pass


def keccak256(value: bytes) -> bytes:
    return keccak.new(digest_bits=256, data=value).digest()


EMPTY_CODE_HASH = keccak256(b"")


def unhex(value: str, size: int | None = None) -> bytes:
    if not isinstance(value, str) or not re.fullmatch(r"0x(?:[0-9a-fA-F]{2})*", value):
        raise VerificationError("expected an even-length 0x-prefixed hex string")
    decoded = bytes.fromhex(value[2:])
    if size is not None and len(decoded) != size:
        raise VerificationError(f"expected {size} bytes, got {len(decoded)}")
    return decoded


def address(value: str) -> str:
    return "0x" + unhex(value, 20).hex()


def quantity(value: str, bits: int = 256) -> int:
    if not isinstance(value, str) or not re.fullmatch(r"0x[0-9a-fA-F]+", value):
        raise VerificationError("expected a JSON-RPC hex quantity")
    parsed = int(value, 16)
    if parsed >= 2**bits:
        raise VerificationError(f"quantity exceeds uint{bits}")
    return parsed


@dataclass(frozen=True)
class Account:
    address: str
    exists: bool
    nonce: int
    balance: int
    storage_root: bytes
    code_hash: bytes

    def json(self) -> dict:
        return {
            "address": self.address, "exists": self.exists, "nonce": self.nonce,
            "balance": str(self.balance), "storage_root": "0x" + self.storage_root.hex(),
            "code_hash": "0x" + self.code_hash.hex(),
        }


def verify_account(state_root: str, expected_address: str, proof: dict) -> Account:
    """Verify inclusion/non-inclusion and every RPC-reported account field."""
    root = unhex(state_root, 32)
    expected_address = address(expected_address)
    if address(proof.get("address", expected_address)) != expected_address:
        raise VerificationError("proof is for a different account")
    try:
        nodes = tuple(rlp.decode(unhex(node)) for node in proof["accountProof"])
        encoded = HexaryTrie.get_from_proof(root, keccak256(unhex(expected_address, 20)), nodes)
    except Exception as error:
        # py-trie raises several validation/missing-node classes. Keep one public
        # error boundary; do not downgrade invalid or incomplete proofs to absence.
        raise VerificationError("invalid or incomplete account proof") from error
    if encoded:
        try:
            fields = rlp.decode(encoded)
            if not isinstance(fields, list) or len(fields) != 4:
                raise ValueError("account must have four fields")
            nonce_bytes, balance_bytes, storage_root, code_hash = fields
            if any(not isinstance(item, bytes) for item in fields):
                raise ValueError("invalid account field encoding")
            if nonce_bytes.startswith(b"\0") or balance_bytes.startswith(b"\0"):
                raise ValueError("non-canonical account integer")
            nonce, balance = int.from_bytes(nonce_bytes, "big"), int.from_bytes(balance_bytes, "big")
            if nonce >= 2**64 or balance >= 2**256 or len(storage_root) != 32 or len(code_hash) != 32:
                raise ValueError("invalid account field length")
            account = Account(expected_address, True, nonce, balance, storage_root, code_hash)
        except Exception as error:
            raise VerificationError("invalid account trie value") from error
    else:
        account = Account(expected_address, False, 0, 0, EMPTY_STORAGE_ROOT, EMPTY_CODE_HASH)
    try:
        reported = (quantity(proof["nonce"], 64), quantity(proof["balance"]),
                    unhex(proof["storageHash"], 32), unhex(proof["codeHash"], 32))
    except (KeyError, TypeError) as error:
        raise VerificationError("missing account proof metadata") from error
    if reported != (account.nonce, account.balance, account.storage_root, account.code_hash):
        raise VerificationError("RPC account metadata does not match the proven account")
    return account


def storage_root(slots, database=None) -> tuple[bytes, int]:
    """Hash all nonzero (32-byte slot, 32-byte value) pairs.

    Supply a disk-backed byte mapping as database for large reconstructions.
    Duplicates are rejected even when their values agree.
    """
    tree = HexaryTrie({} if database is None else database, prune=True)
    count = 0
    for slot, value in slots:
        slot_bytes, value_bytes = unhex(slot, 32), unhex(value, 32)
        key = keccak256(slot_bytes)
        if tree.get(key):
            raise VerificationError("duplicate nonzero storage slot")
        number = int.from_bytes(value_bytes, "big")
        if number == 0:
            raise VerificationError("complete-storage input must contain nonzero slots only")
        tree[key] = rlp.encode(number)
        count += 1
    return tree.root_hash, count


def verify_complete(account: Account, slots, code: str, metadata: dict, database=None) -> int:
    """Fail on a missing field, wrong account state, incomplete storage or wrong code."""
    try:
        nonce = metadata["nonce"]
        balance = metadata["balance"]
        if isinstance(nonce, bool) or not isinstance(nonce, int):
            raise VerificationError("nonce must be an exact integer")
        if not isinstance(balance, str) or not re.fullmatch(r"0|[1-9][0-9]*", balance):
            raise VerificationError("balance must be an exact decimal string")
        if nonce != account.nonce or int(balance) != account.balance:
            raise VerificationError("account nonce/balance mismatch")
        if unhex(metadata["code_hash"], 32) != account.code_hash:
            raise VerificationError("account code hash mismatch")
    except (KeyError, TypeError) as error:
        raise VerificationError("missing required account metadata") from error
    if keccak256(unhex(code)) != account.code_hash:
        raise VerificationError("bytecode does not match the proven code hash")
    root, count = storage_root(slots, database)
    if root != account.storage_root:
        raise VerificationError("storage root mismatch: incomplete, stale or incorrect account storage")
    return count
