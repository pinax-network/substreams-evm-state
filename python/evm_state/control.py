"""Single-host coordination for checkpoint publishers, readers and retention.

The ClickHouse binding makes a second control directory fail closed. Keep the
bound EVM_STATE_HOME on durable local storage; all clients must share it.
"""
from contextlib import contextmanager
import json
import os
from pathlib import Path
import re
import uuid

from .files import atomic_json, atomic_write, file_lock
from .proof import VerificationError
from . import host


def object_id(value):
    if not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{32}", value):
        raise VerificationError("expected a 32-character checkpoint or pin ID")
    return value


class Control:
    def __init__(self, client):
        self.client = client
        database_uuid = client.one("SELECT toString(uuid) AS id FROM system.databases WHERE name={db:String}",
                                   {"db": client.database})["id"]
        if not re.fullmatch(r"[0-9a-f-]{36}", database_uuid) or database_uuid == "00000000-0000-0000-0000-000000000000":
            raise VerificationError("checkpoint coordination requires an Atomic ClickHouse database")
        self.path = Path(os.environ.get("EVM_STATE_HOME", "localdata/control")).resolve() / database_uuid
        self.path.mkdir(parents=True, exist_ok=True)
        self.binding = self.path / "binding.json"
        initialized = self.path / "initialized"
        with file_lock(self.path / "initialize.lock", exclusive=True):
            exists = bool(int(client.one("SELECT count() AS n FROM system.tables WHERE database={db:String} "
                "AND name='_evm_checkpoint_control'", {"db": client.database})["n"]))
            if self.binding.exists():
                record = json.loads(self.binding.read_text())
            else:
                if exists:
                    raise VerificationError("checkpoint control metadata is missing; restore the bound EVM_STATE_HOME")
                record = {"format_version": 1, "control_id": uuid.uuid4().hex,
                    "database_uuid": database_uuid, "directory": str(self.path), "host": host.machine_id()}
                atomic_json(self.binding, record)
            if record.get("database_uuid") != database_uuid or record.get("directory") != str(self.path) or not host.matches(record, self.path):
                raise VerificationError("checkpoint controller belongs to another database, directory or host")
            if not exists:
                if initialized.exists():
                    raise VerificationError("database checkpoint ownership is missing; restore matching database and control metadata")
                client.execute("CREATE TABLE _evm_checkpoint_control (control_id String, binding String) "
                    "ENGINE=MergeTree ORDER BY control_id SETTINGS fsync_after_insert=1, fsync_part_directory=1")
                client.insert("_evm_checkpoint_control", [{"control_id": record["control_id"],
                                                           "binding": json.dumps(record, sort_keys=True)}])
            owner = client.one("SELECT control_id,binding FROM _evm_checkpoint_control")
            if owner["control_id"] != record["control_id"] or json.loads(owner["binding"]) != record:
                raise VerificationError("database is bound to a different checkpoint controller")
            if initialized.exists():
                if initialized.read_text() != record["control_id"]:
                    raise VerificationError("checkpoint initialization marker is corrupt")
            else:
                atomic_write(initialized, record["control_id"].encode())
        self.record = record
        self.pins = self.path / "pins"
        self.pins.mkdir(exist_ok=True)

    @contextmanager
    def reader(self):
        with file_lock(self.path / "readers.lock"):
            yield

    @contextmanager
    def publisher(self):
        # Readers of the previous generation remain available during a build.
        with file_lock(self.path / "writer.lock", exclusive=True, blocking=False):
            with self.reader():
                yield

    @contextmanager
    def retention(self):
        # Do not wait behind long exports or silently invalidate a running reader.
        with file_lock(self.path / "writer.lock", exclusive=True, blocking=False):
            with file_lock(self.path / "readers.lock", exclusive=True, blocking=False):
                yield


def control(client):
    # Resolve every operation, rather than trusting a process-local cache after a
    # database restore or an operator changing the controller location.
    return Control(client)
