"""Differential checks against py-trie's independent incremental assembler."""
import random
import sqlite3

import pytest
import rlp
from trie import HexaryTrie
from trie.utils.nibbles import encode_nibbles

from evm_state.proof import (
    EMPTY_STORAGE_ROOT, VerificationError, _ordered_storage_root, storage_root,
)
from evm_state.triedb import StorageSortDB


def word(number):
    return "0x" + number.to_bytes(32, "big").hex()


def oracle(entries):
    trie = HexaryTrie({})
    for key, value in reversed(entries):
        trie[key] = value
    return trie.root_hash


@pytest.mark.parametrize("depth", [0, 1, 2, 31, 32, 61, 62, 63])
def test_unary_paths_and_full_branches_at_every_boundary(depth):
    # Include a branch outside the common prefix and another branch below it,
    # so leaf/extension/branch collapsing is exercised within one root.
    keys = {bytes.fromhex("a" * depth + f"{n:x}" + "0" * (63 - depth)) for n in range(16)}
    keys |= {b"\xff" * 32, bytes.fromhex("a" * depth + "b" * (64 - depth))}
    values = [1, 127, 128, 255, 256, 2**256 - 1]
    entries = [(key, rlp.encode(values[i % len(values)])) for i, key in enumerate(sorted(keys))]
    assert _ordered_storage_root(iter(entries)) == oracle(entries)


@pytest.mark.parametrize("encoded_length", [31, 32, 33])
def test_child_inline_reference_boundary(encoded_length):
    value = next(rlp.encode(2**bits - 1) for bits in range(1, 257)
                 if len(rlp.encode([encode_nibbles((16,)), rlp.encode(2**bits - 1)])) == encoded_length)
    # Keys differ at their very last nibble: empty-path leaves are children of
    # a deep branch, with encoded lengths on both sides of the hash boundary.
    entries = [(b"\x11" * 31 + bytes([n]), value) for n in range(16)]
    assert _ordered_storage_root(entries) == oracle(entries)


@pytest.mark.parametrize("size", [0, 1, 2, 17, 257, 2048])
def test_seeded_random_keyspace_matches_incremental_trie(size):
    rng = random.Random(431 + size)
    entries = sorted((rng.getrandbits(256).to_bytes(32, "big"), rlp.encode(rng.randrange(1, 2**256)))
                     for _ in range(size))
    assert _ordered_storage_root(entries) == oracle(entries)


@pytest.mark.parametrize("keys", [
    [b"x" * 31], [b"x" * 33], ["x" * 32],
    [b"a" * 32, b"a" * 32], [b"b" * 32, b"a" * 32],
    [b"a" * 32, b"b" * 32, b"c" * 32, b"b" * 32],
])
def test_ordered_builder_rejects_invalid_keys_even_at_end(keys):
    with pytest.raises(VerificationError, match="unique ordered 32-byte"):
        _ordered_storage_root((key, rlp.encode(1)) for key in keys)


@pytest.mark.parametrize("size", [0, 1, 4097])
def test_disk_sort_matches_memory_and_flushes_staged_rows(tmp_path, size):
    slots = [(word(n), word(n + 1)) for n in range(size)]
    random.Random(177).shuffle(slots)
    path = tmp_path / "storage.sqlite"
    disk = StorageSortDB(path)
    try:
        assert storage_root(iter(slots), disk) == storage_root(slots)
        assert len(disk) == size
        # This is an actual committed sorting workspace, not just dirty cache
        # pages that disappear on close. It still is not a checkpoint export.
        with sqlite3.connect(path) as other:
            assert other.execute("SELECT count(*) FROM storage").fetchone()[0] == size
        with pytest.raises(ValueError, match="fresh"):
            storage_root([], disk)
    finally:
        disk.close()
    with pytest.raises(sqlite3.OperationalError, match="already exists"):
        StorageSortDB(path)


@pytest.mark.parametrize("bad_pair", [
    (word(10000), word(0)), ("0x01", word(1)), (word(10000), "0x01"),
    (word(10000), "0x" + "00" * 33), (None, word(1)),
    ("0x" + "ab" * 32, word(1)),
])
def test_invalid_or_duplicate_input_cannot_produce_root_or_reuse_workspace(tmp_path, bad_pair):
    slots = [("0x" + "AB" * 32, word(1)), *[(word(n), word(n + 1)) for n in range(4097)], bad_pair]
    disk = StorageSortDB(tmp_path / "storage.sqlite")
    try:
        with pytest.raises(VerificationError):
            storage_root(iter(slots), disk)
        with pytest.raises(ValueError, match="fresh"):
            storage_root([], disk)
    finally:
        disk.close()


def test_source_failure_propagates_and_poisoned_workspace_cannot_be_reused(tmp_path):
    def broken():
        yield word(1), word(2)
        raise OSError("source interrupted")
    disk = StorageSortDB(tmp_path / "storage.sqlite")
    try:
        with pytest.raises(OSError, match="source interrupted"):
            storage_root(broken(), disk)
        with pytest.raises(ValueError, match="fresh"):
            storage_root([], disk)
    finally:
        disk.close()
    assert _ordered_storage_root([]) == EMPTY_STORAGE_ROOT
