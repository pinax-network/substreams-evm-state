import json

import pytest

from evm_state.files import atomic_json


def test_atomic_metadata_never_overwrites_another_capture(tmp_path):
    target = tmp_path / "proof.json"
    atomic_json(target, {"generation": 1})
    with pytest.raises(FileExistsError):
        atomic_json(target, {"generation": 2})
    assert json.loads(target.read_text()) == {"generation": 1}
    assert list(tmp_path.iterdir()) == [target]
    atomic_json(target, {"generation": 3}, overwrite=True)
    assert json.loads(target.read_text()) == {"generation": 3}
