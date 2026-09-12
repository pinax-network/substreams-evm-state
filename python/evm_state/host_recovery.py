"""Explicit same-machine recovery for hostname-bound prototype runs.

This cannot establish where an old hostname ran. The operator must already know
this is the original machine. It preserves every original identity/cursor/prefix
and binds the local recovery records to the persistent OS identity.
"""
from contextlib import ExitStack
import hashlib
import json
import os
from pathlib import Path

from .bootstrap import load_prefix
from .checkpoint import connect_like
from .cursor import load_progress, validate
from .files import atomic_json, file_lock
from .host import recovery_record
from .proof import VerificationError


def rebind(client, directory, previous_host):
    directory = Path(directory).resolve()
    if not previous_host or previous_host.startswith("machine-sha256:"):
        raise VerificationError("recovery requires the exact legacy hostname")
    run = json.loads((directory / "run.json").read_text())
    identity = run["identity"]
    checkpoints = connect_like(client, identity["checkpoint_database"])
    checkpoint_uuid = checkpoints.one("SELECT toString(uuid) AS id FROM system.databases WHERE name={db:String}",
                                      {"db": checkpoints.database})["id"]
    control_dir = Path(os.environ.get("EVM_STATE_HOME", "localdata/control")).resolve() / checkpoint_uuid
    with ExitStack() as locks:
        for path in [directory / "bootstrap_replay.lock", directory / "run.lock", directory / "source_readers.lock",
                     control_dir / "initialize.lock", control_dir / "writer.lock", control_dir / "readers.lock"]:
            locks.enter_context(file_lock(path, exclusive=True, blocking=False))
        # Compare both sides before creating either sidecar. Recovery is
        # repeatable if interrupted between the two atomic file writes.
        if json.loads((directory / "run.json").read_text()) != run:
            raise VerificationError("run changed during host recovery")
        owner = client.one("SELECT run_id,identity FROM _evm_state_run")
        db_uuid = client.one("SELECT toString(uuid) AS id FROM system.databases WHERE name={db:String}",
                             {"db": client.database})["id"]
        if (run.get("phase") != "prepared" or identity.get("format_version") != 3
                or identity.get("host") != previous_host or identity.get("state_directory") != str(directory)
                or identity.get("database") != client.database or identity.get("http_url") != client.url
                or run.get("database_uuid") != db_uuid or owner["run_id"] != run["run_id"]
                or json.loads(owner["identity"]) != identity):
            raise VerificationError("legacy native ownership does not match")
        if (hashlib.sha256((directory / "package.spkg").read_bytes()).hexdigest() != identity["package_sha256"]
                or (directory / "meta" / f"{client.database}_schema_hash.txt").read_text().strip() != run["schema_hash"]):
            raise VerificationError("frozen package or schema differs")
        binding = json.loads((control_dir / "binding.json").read_text())
        control_owner = checkpoints.one("SELECT control_id,binding FROM _evm_checkpoint_control")
        if (binding.get("host") != previous_host or binding.get("database_uuid") != checkpoint_uuid
                or binding.get("directory") != str(control_dir)
                or control_owner["control_id"] != binding["control_id"]
                or json.loads(control_owner["binding"]) != binding
                or (control_dir / "initialized").read_text() != binding["control_id"]):
            raise VerificationError("legacy checkpoint ownership does not match")
        progress = load_progress(client, run, directory)
        cursor = validate(client, run, (directory / "cursor.txt").read_text().strip())
        if cursor["block"]["number"] < progress["position"]["block"]["number"]:
            raise VerificationError("native cursor regressed behind durable progress")
        prefix = load_prefix(client, directory, run)
        for record, path in [(identity, directory), (binding, control_dir)]:
            target = path / "host-rebinding.json"
            expected = recovery_record(record, path)
            if target.exists() and json.loads(target.read_text()) != expected:
                raise VerificationError("existing host recovery belongs to different state or machine")
        for record, path in [(identity, directory), (binding, control_dir)]:
            target = path / "host-rebinding.json"
            if not target.exists():
                atomic_json(target, recovery_record(record, path))
        return {"run_id": run["run_id"], "rebound": True, "cursor_block": cursor["block"],
                "prefix_block": prefix["header"] if prefix else None,
                "qualification": "Operator-confirmed original machine; original identities and state preserved."}
