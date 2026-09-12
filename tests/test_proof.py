import copy

import pytest
import rlp
from trie import HexaryTrie

from evm_state.proof import (
    EMPTY_CODE_HASH, EMPTY_STORAGE_ROOT, VerificationError, keccak256,
    storage_root, verify_account, verify_complete,
)

ADDRESS = "0x" + "11" * 20
SLOTS = [("0x" + f"{key:064x}", "0x" + f"{value:064x}") for key, value in [(0, 10), (7, 2**255)]]
CODE = "0x60006000"


@pytest.fixture(params=["memory", "sorted-disk"])
def storage_database(request, tmp_path):
    if request.param == "memory":
        yield None
    else:
        from evm_state.triedb import StorageSortDB
        database = StorageSortDB(tmp_path / "storage.sqlite")
        try:
            yield database
        finally:
            database.close()


def bundle():
    storage = HexaryTrie({})
    for slot, value in SLOTS:
        storage[keccak256(bytes.fromhex(slot[2:]))] = rlp.encode(int(value, 16))
    code_hash = keccak256(bytes.fromhex(CODE[2:]))
    state = HexaryTrie({})
    key = keccak256(bytes.fromhex(ADDRESS[2:]))
    state[key] = rlp.encode([3, 2**200, storage.root_hash, code_hash])
    proof = {
        "address": ADDRESS, "nonce": "0x3", "balance": hex(2**200),
        "storageHash": "0x" + storage.root_hash.hex(), "codeHash": "0x" + code_hash.hex(),
        "accountProof": ["0x" + rlp.encode(node).hex() for node in state.get_proof(key)],
    }
    return "0x" + state.root_hash.hex(), proof


def test_account_and_all_slots_verified_at_exact_root(storage_database):
    root, proof = bundle()
    account = verify_account(root, ADDRESS, proof)
    assert verify_complete(account, SLOTS, CODE, account.json(), storage_database) == 2
    assert account.balance == 2**200
    assert storage_root([]) == (EMPTY_STORAGE_ROOT, 0)


@pytest.mark.parametrize("field,value", [("nonce", "0x4"), ("balance", "0x0"),
    ("storageHash", "0x" + "00" * 32), ("codeHash", "0x" + "00" * 32)])
def test_provider_metadata_cannot_override_proven_values(field, value):
    root, proof = bundle()
    proof[field] = value
    with pytest.raises(VerificationError):
        verify_account(root, ADDRESS, proof)


def test_missing_proof_wrong_root_and_wrong_address_fail():
    root, proof = bundle()
    for changed_root, changed_address, changed_proof in [
        ("0x" + "22" * 32, ADDRESS, proof),
        (root, "0x" + "22" * 20, proof),
        (root, ADDRESS, {**proof, "accountProof": []}),
    ]:
        with pytest.raises(VerificationError):
            verify_account(changed_root, changed_address, changed_proof)


@pytest.mark.parametrize("defect", ["missing_slot", "extra_slot", "duplicate_slot", "zero_slot",
    "wrong_nonce", "missing_balance", "wrong_code", "missing_code_hash"])
def test_complete_verification_rejects_partial_or_inconsistent_state(defect, storage_database):
    root, proof = bundle()
    account = verify_account(root, ADDRESS, proof)
    metadata, slots, code = account.json(), copy.copy(SLOTS), CODE
    if defect == "missing_slot": slots.pop()
    elif defect == "extra_slot": slots.append(("0x" + "ff" * 32, "0x" + "11" * 32))
    elif defect == "duplicate_slot": slots.append(slots[0])
    elif defect == "zero_slot": slots.append(("0x" + "ff" * 32, "0x" + "00" * 32))
    elif defect == "wrong_nonce": metadata["nonce"] += 1
    elif defect == "missing_balance": del metadata["balance"]
    elif defect == "wrong_code": code = "0x"
    elif defect == "missing_code_hash": del metadata["code_hash"]
    with pytest.raises(VerificationError):
        verify_complete(account, slots, code, metadata, storage_database)


def test_non_inclusion_is_proven_and_not_confused_with_missing_metadata(storage_database):
    empty = HexaryTrie({})
    proof = {"address": ADDRESS, "nonce": "0x0", "balance": "0x0",
             "storageHash": "0x" + EMPTY_STORAGE_ROOT.hex(),
             "codeHash": "0x" + EMPTY_CODE_HASH.hex(), "accountProof": []}
    account = verify_account("0x" + empty.root_hash.hex(), ADDRESS, proof)
    assert not account.exists
    assert verify_complete(account, [], "0x", account.json(), storage_database) == 0
    proof.pop("nonce")
    with pytest.raises(VerificationError):
        verify_account("0x" + empty.root_hash.hex(), ADDRESS, proof)


def test_disk_backed_trie_matches_memory_and_prunes_old_nodes(tmp_path):
    from evm_state.triedb import TrieDB
    slots = [("0x" + f"{n:064x}", "0x" + f"{n + 1:064x}") for n in range(1000)]
    disk = TrieDB(tmp_path / "nodes.sqlite")
    try:
        assert storage_root(slots, disk) == storage_root(slots)
        assert len(disk) < 2000
    finally:
        disk.close()
