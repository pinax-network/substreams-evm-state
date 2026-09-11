import pytest

from evm_state.checkpoint import build
from native_stream import CURSORS, NativeRun, NativeStream, wait_until
from state_fixtures import A, CODE, block, proof_bundle, source, state

pytestmark = pytest.mark.clickhouse


def sample():
    bundle = proof_bundle(103, {A: state({2: 9}, nonce=3, balance=50)})
    return bundle, [block(100, bundle, storage={(A, 1): 7, (A, 2): 8}, nonces={A: 3}, codes={A: CODE}),
        block(101, bundle, storage={(A, 1): 0, (A, 2): 9}), block(102, bundle, balances={A: 50}), block(103, bundle)]


def assert_whole_interval(client, target, bundle):
    rows = list(client.rows("SELECT number, length(storage.address) AS slots FROM state_blocks FINAL ORDER BY number"))
    assert rows == [{"number": 100, "slots": 2}, {"number": 101, "slots": 2},
                    {"number": 102, "slots": 0}, {"number": 103, "slots": 0}]
    ready = build(target, bundle, [source(client, 100, target=target)])
    assert ready["nonzero_slots"] == 1
    assert ready["verification"][A]["account_proof"] == "verified"
    return ready


def test_cursor_failure_after_data_write_recovers_from_previous_cursor(databases, tmp_path, native_proxy):
    client, target = databases(False), databases(False)
    bundle, blocks = sample()
    run = NativeRun(client, tmp_path, [A])
    saved = tmp_path / "saved-cursor.txt"

    def fail_cursor_write(number, stream, context):
        if number == 101:
            wait_until(lambda: run.cursor.is_file() and run.cursor.read_text() == CURSORS["1"]["100"])
            run.cursor.rename(saved)
            run.cursor.mkdir()  # Startup already succeeded; next data row inserts before cursor write fails.

    stream = NativeStream(blocks[:2], native_proxy, before_block=fail_cursor_write)
    try:
        run.start(stream)
        output = run.finish(expected=1)
        assert not stream.errors, stream.errors
        assert "creating cursor file" in output
        assert int(client.one("SELECT max(number) AS n FROM state_blocks FINAL")["n"]) == 101
        assert saved.read_text() == CURSORS["1"]["100"]
    finally:
        run.close(); stream.close()
    run.cursor.rmdir()
    saved.rename(run.cursor)
    stream = NativeStream(blocks, native_proxy)
    try:
        run.start(stream)
        run.finish()
        assert not stream.errors, stream.errors
        assert stream.requests[0].start_cursor == CURSORS["1"]["100"]
        assert run.cursor.read_text() == CURSORS["1"]["103"]
        assert_whole_interval(client, target, bundle)
    finally:
        run.close(); stream.close()


@pytest.mark.parametrize("backfill", [False, True], ids=["live", "spooled-backfill"])
def test_killed_process_resumes_from_durable_cursor(databases, tmp_path, native_proxy, backfill):
    client, target = databases(False), databases(False)
    bundle, blocks = sample()
    run = NativeRun(client, tmp_path, [A])
    step = "17" if backfill else "1"

    def hold(stream, context):
        while context.is_active() and not stream.closed.wait(0.02):
            pass

    stream = NativeStream(blocks[:2], native_proxy, after_blocks=hold, backfill=backfill)
    try:
        process = run.start(stream)
        wait_until(lambda: run.cursor.exists() and run.cursor.read_text() == CURSORS[step]["101"], process)
        process.kill()
        process.wait(timeout=10)
        assert not stream.errors, stream.errors
    finally:
        run.close(); stream.close()
    stream = NativeStream(blocks, native_proxy, backfill=backfill)
    try:
        run.start(stream)
        run.finish()
        assert not stream.errors, stream.errors
        assert stream.requests[0].start_cursor == CURSORS[step]["101"]
        assert run.cursor.read_text() == CURSORS[step]["103"]
        assert_whole_interval(client, target, bundle)
    finally:
        run.close(); stream.close()
