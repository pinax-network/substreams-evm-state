import json
from pathlib import Path

import pytest

from evm_state.checkpoint import build, read_account
from evm_state.cursor import load_progress
from evm_state.files import exclusive_lock, file_lock
from evm_state.history import cleanup
from evm_state.reader import page, pin, unpin
from evm_state.retention import prune
from state_fixtures import A, block, insert_blocks, proof_bundle, source, state, word

pytestmark = pytest.mark.clickhouse


def dated(number, bundle, **kwargs):
    row = block(number, bundle, **kwargs)
    row["timestamp"] += (number - 100) * 32 * 86400
    return row


def history(databases):
    stream, target = databases(), databases(False)
    initial = proof_bundle(101, {A: state({1: 7})})
    insert_blocks(stream, [dated(100, initial, storage={(A, 1): 7}), dated(101, initial)])
    first = build(target, initial, [source(stream, 100, target=target)])
    latest = proof_bundle(103, {A: state({2: 9})})
    insert_blocks(stream, [dated(102, latest, storage={(A, 1): 0, (A, 2): 9}), dated(103, latest)])
    second = build(target, latest, [source(stream, 102, target=target)], first["snapshot_id"])
    directory = Path(second["sources"][0]["state_directory"])
    return stream, target, first, second, directory


def numbers(client, table):
    return [r["number"] for r in client.rows(f"SELECT number FROM {table} FINAL ORDER BY number")]


def test_history_cleanup_preserves_retained_checkpoint_continuation_and_cursor(databases):
    stream, target, first, second, directory = history(databases)
    protected = pin(target, first["snapshot_id"])
    preview = cleanup(stream, directory, second["snapshot_id"], keep_blocks=1)
    assert preview["remove_before"] == 102
    assert numbers(stream, "state_blocks") == [100, 101, 102, 103]
    result = cleanup(stream, directory, second["snapshot_id"], keep_blocks=1, apply=True)
    assert result["applied"]
    assert numbers(stream, "state_blocks") == numbers(stream, "_blocks_") == [102, 103]
    assert page(target, protected["pin_id"], A)["storage"] == [{"slot": word(1), "value": word(7)}]
    # Rebuild from the older retained checkpoint, using only its surviving
    # continuation interval. Cleared storage must not reappear after cleanup.
    bundle = proof_bundle(104, {A: state({2: 9}, balance=50)})
    insert_blocks(stream, [dated(104, bundle, balances={A: 50})])
    third = build(target, bundle, [source(stream, 102, target=target)], first["snapshot_id"])
    assert third["nonzero_slots"] == 1
    assert read_account(target, third["snapshot_id"], A)["balance"] == "50"
    unpin(target, protected["pin_id"])
    prune(target, keep_latest=1)
    cleanup(stream, directory, third["snapshot_id"], keep_blocks=1, apply=True)
    assert numbers(stream, "state_blocks") == numbers(stream, "_blocks_") == [104]
    run = json.loads((directory / "run.json").read_text())
    assert load_progress(stream, run, directory)["position"]["block"]["number"] == 104


@pytest.mark.parametrize("lock", ["run.lock", "source_readers.lock"])
def test_active_native_writer_or_source_reader_excludes_history_cleanup(databases, lock):
    stream, _, _, ready, directory = history(databases)
    context = exclusive_lock(directory / lock) if lock == "run.lock" else file_lock(directory / lock)
    with context:
        with pytest.raises(ValueError, match="another process"):
            cleanup(stream, directory, ready["snapshot_id"], keep_blocks=1, apply=True)
    assert numbers(stream, "state_blocks") == [100, 101, 102, 103]


def test_interrupted_partition_cleanup_can_resume_without_losing_cursor_or_checkpoint(databases, monkeypatch):
    stream, target, _, ready, directory = history(databases)
    execute = stream.execute
    def fail(sql, params=None):
        if sql.startswith("ALTER TABLE _blocks_"):
            raise OSError("injected history cleanup failure")
        return execute(sql, params)
    monkeypatch.setattr(stream, "execute", fail)
    with pytest.raises(OSError, match="injected"):
        cleanup(stream, directory, ready["snapshot_id"], keep_blocks=1, apply=True)
    assert numbers(stream, "state_blocks") == [102, 103]
    assert numbers(stream, "_blocks_") == [100, 101, 102, 103]
    monkeypatch.setattr(stream, "execute", execute)
    cleanup(stream, directory, ready["snapshot_id"], keep_blocks=1, apply=True)
    assert numbers(stream, "_blocks_") == [102, 103]
    assert read_account(target, ready["snapshot_id"], A)["nonzero_slots"] == 1


def test_cleanup_refuses_cursor_that_differs_from_durable_progress(databases):
    stream, _, _, ready, directory = history(databases)
    (directory / "cursor.txt").write_text("truncated")
    with pytest.raises(ValueError, match="differs from durable"):
        cleanup(stream, directory, ready["snapshot_id"], keep_blocks=1, apply=True)
    assert numbers(stream, "state_blocks") == [100, 101, 102, 103]


def test_source_cannot_publish_into_an_untracked_second_checkpoint_database(databases):
    stream, target, _, _, _ = history(databases)
    bundle = proof_bundle(103, {A: state({2: 9})})
    other = databases(False)
    with pytest.raises(ValueError, match="different checkpoint database"):
        build(other, bundle, [source(stream, 100, target=target)])


@pytest.mark.parametrize("keep", [0, -1, True])
def test_cleanup_requires_at_least_one_retained_native_block(databases, keep):
    stream, _, _, ready, directory = history(databases)
    with pytest.raises(ValueError, match="at least one"):
        cleanup(stream, directory, ready["snapshot_id"], keep_blocks=keep, apply=True)


def test_checkpoint_waits_for_durable_progress_to_cover_its_target(databases):
    stream, target = databases(), databases(False)
    bundle = proof_bundle(101, {A: state()})
    insert_blocks(stream, [block(100, bundle)])
    declared = source(stream, 100, target=target)
    insert_blocks(stream, [block(101, bundle)])
    with pytest.raises(ValueError, match="progress has not reached"):
        build(target, bundle, [declared])
    assert int(target.one("SELECT count() AS n FROM checkpoints")["n"]) == 0
