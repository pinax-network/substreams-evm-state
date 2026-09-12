import json
from pathlib import Path

import pytest

from evm_state.bootstrap import compact, load_prefix, replay
from evm_state.checkpoint import build, read_account
from evm_state.cursor import load_progress
from evm_state.files import exclusive_lock, file_lock
from state_fixtures import A, B, CODE, block, insert_blocks, proof_bundle, source, state, word
from test_history import dated, numbers

pytestmark = pytest.mark.clickhouse


def initial(databases):
    stream, target = databases(), databases(False)
    bundle = proof_bundle(101, {A: state({1: 7, 2: 8})})
    insert_blocks(stream, [dated(100, bundle, storage={(A, 1): 7, (A, 2): 8}, nonces={A: 1}),
                           dated(101, bundle)])
    declared = source(stream, 100, target=target)
    owner = json.loads(stream.one("SELECT identity FROM _evm_state_run")["identity"])
    return stream, target, declared, Path(owner["state_directory"])


def slots(stream, generation):
    return list(stream.rows("SELECT slot,value FROM bootstrap_storage WHERE generation={id:String} ORDER BY slot",
                            {"id": generation}))


def test_repeated_compaction_matches_full_replay_and_proofs_after_slot_clear(databases):
    stream, target, declared, directory = initial(databases)
    prefix = compact(stream, directory)
    assert prefix["status"] == "unverified-bootstrap"
    assert prefix["fields"] == {A: {"nonce": 1}}  # unobserved metadata stays unknown
    assert numbers(stream, "state_blocks") == numbers(stream, "_blocks_") == [101]
    assert int(target.one("SELECT count() AS n FROM checkpoints")["n"]) == 0
    bundle = proof_bundle(103, {A: state({2: 9}, balance=50)})
    delta = dated(102, bundle, storage={(A, 1): 0, (A, 2): 9})
    insert_blocks(stream, [delta])
    source(stream, 100, target=target)
    second = compact(stream, directory)
    assert slots(stream, second["generation"]) == [{"slot": word(2), "value": word(9)}]
    assert numbers(stream, "state_blocks") == [102]
    assert list(stream.rows("SELECT DISTINCT generation FROM bootstrap_storage")) == [{"generation": second["generation"]}]
    final = dated(103, bundle, balances={A: 50})
    insert_blocks(stream, [final])
    source(stream, 100, target=target)
    ready = build(target, bundle, [declared])
    assert read_account(target, ready["snapshot_id"], A)["balance"] == "50"
    plain, plain_target, plain_source, _ = initial(databases)
    insert_blocks(plain, [delta, final])
    source(plain, 100, target=plain_target)
    comparison = build(plain_target, bundle, [plain_source])
    assert ready["state_sha256"] == comparison["state_sha256"]
    assert ready["nonzero_slots"] == 1


def test_storage_reset_discards_untouched_prefix_slots_and_keeps_recreation(databases):
    stream, target, declared, directory = initial(databases)
    compact(stream, directory)
    empty = proof_bundle(102, {A: state({}, nonce=0, balance=0, code="0x", exists=False)})
    insert_blocks(stream, [dated(102, empty, nonces={A: 0}, balances={A: 0}, codes={A: "0x"},
        lifecycle=[{"address": A, "kind": "storage_reset", "ordinal": 30}])])
    source(stream, 100, target=target)
    cleared = compact(stream, directory)
    assert cleared["nonzero_slots"] == 0
    final = proof_bundle(103, {A: state({3: 10})})
    insert_blocks(stream, [dated(103, final, storage={(A, 3): 10}, nonces={A: 1}, balances={A: 25}, codes={A: CODE})])
    source(stream, 100, target=target)
    compact(stream, directory)
    ready = build(target, final, [declared])
    assert ready["nonzero_slots"] == 1
    assert read_account(target, ready["snapshot_id"], A)["code"] == CODE


@pytest.mark.parametrize("defect", ["missing_slot", "wrong_nonce", "wrong_header"])
def test_unverified_prefix_never_bypasses_final_proofs(databases, defect):
    stream, target, declared, directory = initial(databases)
    compact(stream, directory)
    final = proof_bundle(101, {A: state({1: 7, 2: 8, **({3: 9} if defect == "missing_slot" else {})},
                                       nonce=2 if defect == "wrong_nonce" else 1)})
    if defect == "wrong_header":
        final["header"]["hash"] = word(999)
    # Different account state also changes the header root. Append a block with
    # that valid root so this tests complete state verification, not just binding.
    if defect != "wrong_header":
        final["header"].update(number=102, hash=word(102), parent_hash=word(101))
        insert_blocks(stream, [dated(102, final)])
        source(stream, 100, target=target)
    with pytest.raises(ValueError, match="storage root|nonce|proof header"):
        build(target, final, [declared])
    assert int(target.one("SELECT count() AS n FROM checkpoints")["n"]) == 0


@pytest.mark.parametrize("defect", ["storage", "pointer", "missing_pointer", "version", "duplicate"])
def test_prefix_damage_fails_closed_after_raw_history_is_removed(databases, defect):
    stream, target, declared, directory = initial(databases)
    prefix = compact(stream, directory)
    if defect in {"storage", "duplicate"}:
        stream.insert("bootstrap_storage", [{"generation": prefix["generation"], "address": A,
            "slot": word(1 if defect == "duplicate" else 3), "value": word(7)}])
    elif defect == "missing_pointer":
        (directory / "bootstrap.json").unlink()
    else:
        prefix["generation" if defect == "pointer" else "format_version"] = "0" * 32 if defect == "pointer" else 9
        (directory / "bootstrap.json").write_text(json.dumps(prefix))
    with pytest.raises(ValueError):
        build(target, proof_bundle(101, {A: state({1: 7, 2: 8})}), [declared])
    assert numbers(stream, "state_blocks") == [101]


@pytest.mark.parametrize("phase", ["before_pointer", "after_pointer", "between_drops"])
def test_interrupted_compaction_recovers_without_publishing_or_losing_cursor(databases, monkeypatch, phase):
    stream, target, declared, directory = initial(databases)
    import evm_state.bootstrap as bootstrap
    atomic = bootstrap.atomic_json
    execute = stream.execute
    def interrupted_pointer(path, value, **kwargs):
        if phase == "after_pointer":
            atomic(path, value, **kwargs)
        raise OSError("injected pointer interruption")
    def interrupted_drop(sql, params=None):
        if sql.startswith("ALTER TABLE _blocks_"):
            raise OSError("injected partition interruption")
        return execute(sql, params)
    with monkeypatch.context() as changes:
        if phase == "between_drops":
            changes.setattr(stream, "execute", interrupted_drop)
        else:
            changes.setattr(bootstrap, "atomic_json", interrupted_pointer)
        with pytest.raises(OSError, match="injected"):
            compact(stream, directory)
    if phase == "before_pointer":
        assert numbers(stream, "state_blocks") == [100, 101]
    prefix = compact(stream, directory)
    assert prefix["nonzero_slots"] == 2
    assert numbers(stream, "state_blocks") == numbers(stream, "_blocks_") == [101]
    run = json.loads((directory / "run.json").read_text())
    assert load_progress(stream, run, directory)["position"]["block"]["number"] == 101
    assert int(target.one("SELECT count() AS n FROM checkpoints")["n"]) == 0
    assert build(target, proof_bundle(101, {A: state({1: 7, 2: 8})}), [declared])["nonzero_slots"] == 2


def test_candidate_budget_failure_preserves_previous_prefix_and_unfolded_history(databases, monkeypatch):
    stream, target, _, directory = initial(databases)
    first = compact(stream, directory)
    bundle = proof_bundle(102, {A: state({1: 9, 2: 8})})
    insert_blocks(stream, [dated(102, bundle, storage={(A, 1): 9})])
    source(stream, 100, target=target)
    with monkeypatch.context() as changes:
        usage = iter([1, 100_000_000_000])
        changes.setattr(stream, "disk_usage", lambda: next(usage))
        with pytest.raises(ValueError, match="candidate exceeds"):
            compact(stream, directory)
    assert load_prefix(stream, directory) == {k: v for k, v in first.items() if k not in {"removed_partitions", "database_bytes"}}
    assert numbers(stream, "state_blocks") == [101, 102]
    assert compact(stream, directory)["generation"] != first["generation"]


@pytest.mark.parametrize("defect", ["gap", "parent", "filter"])
def test_broken_suffix_cannot_replace_previous_prefix(databases, defect):
    stream, target, _, directory = initial(databases)
    compact(stream, directory)
    before = (directory / "bootstrap.json").read_bytes()
    number = 103 if defect == "gap" else 102
    bundle = proof_bundle(number, {A: state({1: 7, 2: 8})})
    row = dated(number, bundle)
    if defect == "parent": row["parent_hash"] = word(999)
    if defect == "filter": row["accounts"] = B
    insert_blocks(stream, [row])
    if defect == "filter": insert_blocks(stream, [dated(103, bundle)])
    source(stream, 100, target=target)
    with pytest.raises(ValueError, match="missing|continuity|filter"):
        compact(stream, directory)
    assert (directory / "bootstrap.json").read_bytes() == before
    assert numbers(stream, "state_blocks") == ([101, 102, 103] if defect == "filter" else [101, number])


@pytest.mark.parametrize("lock", ["run.lock", "source_readers.lock"])
def test_compaction_excludes_native_writer_and_checkpoint_reader(databases, lock):
    stream, _, _, directory = initial(databases)
    context = exclusive_lock(directory / lock) if lock == "run.lock" else file_lock(directory / lock)
    with context, pytest.raises(ValueError, match="another process"):
        compact(stream, directory)
    assert numbers(stream, "state_blocks") == [100, 101]


def test_new_compacted_cohort_joins_existing_checkpoint_then_continues_normally(databases):
    stream, target, declared, directory = initial(databases)
    first = build(target, proof_bundle(101, {A: state({1: 7, 2: 8})}), [declared])
    with pytest.raises(ValueError, match="already have a checkpoint"):
        compact(stream, directory)
    other = databases()
    final = proof_bundle(103, {A: state({1: 7, 2: 8}), B: state({4: 12})})
    insert_blocks(other, [dated(100, final, selected=[B], storage={(B, 4): 12}),
                          *[dated(n, final, selected=[B]) for n in range(101, 104)]])
    new_source = source(other, 100, accounts=[B], target=target)
    other_dir = Path(json.loads(other.one("SELECT identity FROM _evm_state_run")["identity"])["state_directory"])
    compact(other, other_dir)
    insert_blocks(stream, [dated(n, final, selected=[A]) for n in range(102, 104)])
    ready = build(target, final, [source(stream, 102, target=target), new_source], first["snapshot_id"])
    assert ready["nonzero_slots"] == 3
    next_bundle = proof_bundle(104, {A: state({1: 7, 2: 8}), B: state({})})
    insert_blocks(stream, [dated(104, next_bundle, selected=[A])])
    insert_blocks(other, [dated(104, next_bundle, selected=[B], storage={(B, 4): 0})])
    next_ready = build(target, next_bundle, [source(stream, 104, target=target),
        source(other, 104, accounts=[B], target=target)], ready["snapshot_id"])
    assert next_ready["nonzero_slots"] == 2
    assert "bootstrap" not in next_ready["sources"][1]


def test_real_native_chunked_replay_resumes_compacted_cursor(databases, tmp_path, native_proxy, monkeypatch):
    from conftest import SPKG, native_dsn
    from native_stream import NativeStream
    import os
    for name in os.environ:
        if name.startswith("SUBSTREAMS_"): monkeypatch.delenv(name)
    client, target = databases(False), databases(False)
    bundle = proof_bundle(105, {A: state({2: 9})})
    blocks = [dated(100, bundle, storage={(A, 1): 7}), dated(101, bundle),
              dated(102, bundle, storage={(A, 1): 0, (A, 2): 9}),
              *[dated(n, bundle) for n in range(103, 106)]]
    stream = NativeStream(blocks, native_proxy, backfill=True)
    args = (client, SPKG, stream.endpoint, [A], 100, tmp_path / "native", native_dsn(client.database))
    try:
        partial = replay(*args, stop_block=104, chunk_blocks=2, checkpoint_database=target.database,
                         decode_batch_size=1, prometheus_addr="127.0.0.1:0", parallel_workers=50)
        assert partial["status"] == "unverified-bootstrap"
        final = replay(*args, stop_block=106, chunk_blocks=2, checkpoint_database=target.database,
                       decode_batch_size=1, prometheus_addr="127.0.0.1:0", parallel_workers=100)
        repeated = replay(*args, stop_block=106, chunk_blocks=2, checkpoint_database=target.database, decode_batch_size=1)
        assert repeated == final
        assert len(stream.requests) == 3
        assert stream.requested_workers == ["50", "50", "100"]
        assert stream.requests[1].start_cursor and stream.requests[2].start_cursor
        assert numbers(client, "state_blocks") == numbers(client, "_blocks_") == [105]
        assert not stream.errors
        ready = build(target, bundle, [final["source"]])
        assert ready["nonzero_slots"] == 1
    finally:
        stream.close()
