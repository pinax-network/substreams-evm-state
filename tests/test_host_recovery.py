import json

import pytest

from evm_state import host
from evm_state.bootstrap import compact
from evm_state.control import control
from evm_state.files import exclusive_lock
from evm_state.host_recovery import rebind
from evm_state.proof import VerificationError
from test_bootstrap import initial

pytestmark = pytest.mark.clickhouse


def legacy(databases, monkeypatch):
    monkeypatch.setattr(host.socket, "gethostname", lambda: "legacy-host")
    monkeypatch.setattr(host, "machine_id", lambda: "legacy-host")
    client, target, declared, directory = initial(databases)
    prefix = compact(client, directory)
    control_dir = control(target).path
    monkeypatch.setattr(host, "machine_id", lambda: "machine-sha256:" + "c" * 64)
    monkeypatch.setattr(host.socket, "gethostname", lambda: "renamed-host")
    return client, target, directory, control_dir, prefix


def test_recovery_preserves_state_and_rejects_another_machine(databases, monkeypatch):
    client, target, directory, control_dir, prefix = legacy(databases, monkeypatch)
    original = {name: (directory / name).read_bytes() for name in
                ["run.json", "bootstrap.json", "durable_progress.json", "cursor.txt"]}
    with pytest.raises(VerificationError, match="host"):
        compact(client, directory)
    result = rebind(client, directory, "legacy-host")
    assert result["rebound"] and result["prefix_block"] == prefix["header"]
    assert rebind(client, directory, "legacy-host") == result
    assert all((directory / name).read_bytes() == raw for name, raw in original.items())
    assert compact(client, directory)["state_sha256"] == prefix["state_sha256"]
    monkeypatch.setattr(host, "machine_id", lambda: "machine-sha256:" + "d" * 64)
    with pytest.raises(VerificationError, match="host"):
        compact(client, directory)
    with pytest.raises(VerificationError, match="different state or machine"):
        rebind(client, directory, "legacy-host")


@pytest.mark.parametrize("defect", ["wrong_host", "package", "prefix", "controller"])
def test_recovery_rejects_damaged_or_mismatched_state_before_writing(databases, monkeypatch, defect):
    client, target, directory, control_dir, _ = legacy(databases, monkeypatch)
    if defect == "package":
        (directory / "package.spkg").write_bytes(b"changed")
    if defect == "prefix":
        path = directory / "bootstrap.json"
        value = json.loads(path.read_text()); value["nonzero_slots"] += 1
        path.write_text(json.dumps(value))
    if defect == "controller":
        (control_dir / "initialized").write_text("wrong-owner")
    with pytest.raises(VerificationError):
        rebind(client, directory, "wrong-host" if defect == "wrong_host" else "legacy-host")
    assert not (directory / "host-rebinding.json").exists()
    assert not (control_dir / "host-rebinding.json").exists()


def test_recovery_excludes_writers_and_resumes_an_interrupted_attestation(databases, monkeypatch):
    from evm_state import host_recovery
    client, target, directory, control_dir, _ = legacy(databases, monkeypatch)
    with exclusive_lock(directory / "run.lock"):
        with pytest.raises(ValueError, match="another process"):
            rebind(client, directory, "legacy-host")
    original_write = host_recovery.atomic_json
    def interrupted(path, value):
        if path.parent == control_dir:
            raise OSError("interrupted second attestation")
        original_write(path, value)
    monkeypatch.setattr(host_recovery, "atomic_json", interrupted)
    with pytest.raises(OSError, match="interrupted"):
        rebind(client, directory, "legacy-host")
    assert (directory / "host-rebinding.json").exists()
    assert not (control_dir / "host-rebinding.json").exists()
    monkeypatch.setattr(host_recovery, "atomic_json", original_write)
    assert rebind(client, directory, "legacy-host")["rebound"]
    assert compact(client, directory)["already_compacted"]
