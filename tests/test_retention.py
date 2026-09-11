import pytest

from evm_state.checkpoint import read_account
from evm_state.control import control
from evm_state.reader import page, pin, unpin
from evm_state.retention import plan, prune
from export_fixtures import checkpoint
from state_fixtures import A, B, state, word

pytestmark = pytest.mark.clickhouse


def test_pinned_pagination_stays_on_one_checkpoint_while_new_state_is_published(databases):
    target, first = checkpoint(databases)
    pinned = pin(target, first["snapshot_id"])
    one = page(target, pinned["pin_id"], A, limit=2)
    _, second = checkpoint(databases, {A: state({9: 42})}, 101, target)
    _, third = checkpoint(databases, {A: state({10: 43})}, 102, target)
    result = prune(target, keep_latest=1)
    assert first["snapshot_id"] in result["keep"]
    assert second["snapshot_id"] in result["remove"]
    rows = one["storage"]
    cursor = one["next_cursor"]
    while cursor:
        current = page(target, pinned["pin_id"], A, cursor, limit=2)
        assert current["header"] == first["header"]
        rows += current["storage"]
        cursor = current["next_cursor"]
    assert [(r["slot"], r["value"]) for r in rows] == [(word(k), word(v)) for k, v in [(0, 1), (1, 2), (2, 3), (7, 8), (99, 100)]]
    unpin(target, pinned["pin_id"])
    assert first["snapshot_id"] in prune(target, keep_latest=1)["remove"]
    assert read_account(target, third["snapshot_id"], A)["nonzero_slots"] == 1
    with pytest.raises((OSError, ValueError)):
        page(target, pinned["pin_id"], A)


def test_retention_keeps_latest_state_for_quiet_accounts(databases):
    target, quiet = checkpoint(databases, {A: state({1: 1})})
    _, old_b = checkpoint(databases, {B: state({2: 2})}, 101, target)
    _, newest = checkpoint(databases, {B: state({3: 3})}, 102, target)
    result = prune(target, keep_latest=1)
    assert result["keep"] == sorted([quiet["snapshot_id"], newest["snapshot_id"]])
    assert old_b["snapshot_id"] in result["remove"]
    assert read_account(target, quiet["snapshot_id"], A)["nonzero_slots"] == 1


def test_unpublished_candidate_parts_are_collected_and_active_reader_blocks_cleanup(databases):
    target, ready = checkpoint(databases)
    candidate = "f" * 32
    target.insert("checkpoint_storage", [{"snapshot_id": candidate, "address": A, "slot": word(55), "value": word(1)}])
    with control(target).reader():
        with pytest.raises(ValueError, match="another process"):
            prune(target)
    preview = plan(target)
    assert preview["unpublished_candidates"] == [candidate]
    assert int(target.one("SELECT count() AS n FROM checkpoint_storage FINAL WHERE snapshot_id={id:String}", {"id": candidate})["n"]) == 1
    result = prune(target)
    assert result["remove"] == [candidate]
    assert int(target.one("SELECT count() AS n FROM checkpoint_storage FINAL WHERE snapshot_id={id:String}", {"id": candidate})["n"]) == 0
    assert read_account(target, ready["snapshot_id"], A)["nonzero_slots"] == 5


def test_cursor_cannot_be_reused_for_another_checkpoint_or_account(databases):
    target, first = checkpoint(databases)
    p1 = pin(target, first["snapshot_id"])
    cursor = page(target, p1["pin_id"], A, limit=1)["next_cursor"]
    _, other = checkpoint(databases, {A: state({1: 3}), B: state({2: 4})}, 101, target)
    p2 = pin(target, other["snapshot_id"])
    for selected in [A, B]:
        with pytest.raises(ValueError, match="cursor"):
            page(target, p2["pin_id"], selected, cursor)


def test_second_controller_directory_cannot_bypass_reader_pins(databases, monkeypatch, tmp_path):
    target, ready = checkpoint(databases)
    pin(target, ready["snapshot_id"])
    monkeypatch.setenv("EVM_STATE_HOME", str(tmp_path / "other"))
    with pytest.raises(ValueError, match="control metadata is missing"):
        prune(target)


def test_missing_database_control_binding_is_not_silently_recreated(databases):
    target, _ = checkpoint(databases)
    target.execute("DROP TABLE _evm_checkpoint_control SYNC")
    with pytest.raises(ValueError, match="ownership is missing"):
        prune(target)


def test_interrupted_cleanup_can_resume_without_republishing_orphans(databases, monkeypatch):
    target, first = checkpoint(databases)
    checkpoint(databases, number=101, target=target)
    original = target.execute
    def fail(sql, params=None):
        if "ALTER TABLE checkpoint_accounts DROP" in sql:
            raise OSError("interrupted retention")
        return original(sql, params)
    monkeypatch.setattr(target, "execute", fail)
    with pytest.raises(OSError, match="interrupted"):
        prune(target, keep_latest=1)
    with pytest.raises(ValueError):
        read_account(target, first["snapshot_id"], A)
    monkeypatch.setattr(target, "execute", original)
    assert first["snapshot_id"] in prune(target, keep_latest=1)["unpublished_candidates"]
