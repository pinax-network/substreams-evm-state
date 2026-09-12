"""Invalid scheduling requests must fail before claiming a database or run directory."""
import pytest

from evm_state.bootstrap import replay
from evm_state.ingest import ingest


@pytest.mark.parametrize("runner", [ingest, replay])
@pytest.mark.parametrize("workers", [0, -1, True, "100"])
def test_invalid_parallel_workers_do_not_prepare_or_mutate_a_source(tmp_path, runner, workers):
    directory = tmp_path / "not-created"
    with pytest.raises(ValueError, match="parallel workers must be a positive integer"):
        runner(None, None, None, [], 0, directory, None, stop_block=1, parallel_workers=workers)
    assert not directory.exists()
