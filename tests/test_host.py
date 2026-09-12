import json
from types import SimpleNamespace

import pytest

from evm_state import host
from evm_state.proof import VerificationError


def test_linux_machine_identity_survives_hostname_changes(monkeypatch):
    monkeypatch.setattr(host.platform, "system", lambda: "Linux")
    monkeypatch.setattr(host.Path, "read_text", lambda _: "1" * 32 + "\n")
    first = host.machine_id()
    monkeypatch.setattr(host.socket, "gethostname", lambda: "changed-dhcp-name")
    assert host.machine_id() == first
    assert "1" * 32 not in first


def test_macos_machine_identity_requires_a_platform_uuid(monkeypatch):
    monkeypatch.setattr(host.platform, "system", lambda: "Darwin")
    monkeypatch.setattr(host.subprocess, "run", lambda *a, **kw: SimpleNamespace(
        stdout='"IOPlatformUUID" = "12345678-1234-1234-1234-123456789abc"'))
    first = host.machine_id()
    assert first.startswith("machine-sha256:")
    monkeypatch.setattr(host.subprocess, "run", lambda *a, **kw: SimpleNamespace(stdout=""))
    with pytest.raises(VerificationError, match="unavailable"):
        host.machine_id()


@pytest.mark.parametrize("value", ["", "0" * 32, "not-a-machine-id"])
def test_missing_or_invalid_machine_identity_does_not_use_hostname(monkeypatch, value):
    monkeypatch.setattr(host.platform, "system", lambda: "Linux")
    monkeypatch.setattr(host.Path, "read_text", lambda _: value)
    with pytest.raises(VerificationError, match="unavailable"):
        host.machine_id()


def test_legacy_recovery_is_bound_to_record_directory_and_machine(monkeypatch, tmp_path):
    monkeypatch.setattr(host, "machine_id", lambda: "machine-sha256:" + "1" * 64)
    monkeypatch.setattr(host.socket, "gethostname", lambda: "new-name")
    record = {"host": "old-name", "database": "example"}
    assert not host.matches(record, tmp_path)
    attestation = host.recovery_record(record, tmp_path)
    (tmp_path / "host-rebinding.json").write_text(json.dumps(attestation))
    assert host.matches(record, tmp_path)
    assert not host.matches({**record, "database": "different"}, tmp_path)
    copied = tmp_path / "copy"; copied.mkdir()
    (copied / "host-rebinding.json").write_text(json.dumps(attestation))
    assert not host.matches(record, copied)
    monkeypatch.setattr(host, "machine_id", lambda: "machine-sha256:" + "2" * 64)
    assert not host.matches(record, tmp_path)
