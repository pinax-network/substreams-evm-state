"""Bind checkpoint inputs to a prepared, locally owned native ingestion run."""
from contextlib import contextmanager
import hashlib
import json
from pathlib import Path
import re
import socket

from .files import file_lock
from .proof import VerificationError, address


def _selected(values):
    if not isinstance(values, list) or not values:
        raise VerificationError("source accounts must be a nonempty list")
    return sorted({address(value) for value in values})


def _validate(client, source, identity, run_id, directory):
    if (identity.get("format_version") != 2 or identity.get("host") != socket.gethostname()
            or identity.get("state_directory") != str(directory)):
        raise VerificationError("source run has no matching host/directory binding; use a fresh guarded continuation")
    if (identity.get("database") != client.database or identity.get("http_url") != client.url
            or identity.get("module") != "map_block_state" or identity.get("network") != "bsc"
            or identity.get("schema_version") != 1 or identity.get("final_blocks_only") is not True):
        raise VerificationError("source database identity is not the qualified finalized BSC module")
    if (source.get("final_blocks_only") is not True or source.get("module_hash") != identity["module_hash"]
            or not re.fullmatch(r"[0-9a-f]{40}", identity["module_hash"])
            or _selected(source["accounts"]) != identity["accounts"]):
        raise VerificationError("source filter, module hash or finality differs from its native run")
    start = source["start_block"]
    if isinstance(start, bool) or not isinstance(start, int) or start < identity["start_block"]:
        raise VerificationError("source range starts before its native run")
    record = json.loads((directory / "run.json").read_text())
    db_uuid = client.one("SELECT toString(uuid) AS id FROM system.databases WHERE name={db:String}",
                         {"db": client.database})["id"]
    if (record.get("phase") != "prepared" or record.get("run_id") != run_id
            or record.get("identity") != identity or record.get("database_uuid") != db_uuid):
        raise VerificationError("source database and local run metadata differ")
    if hashlib.sha256((directory / "package.spkg").read_bytes()).hexdigest() != identity["package_sha256"]:
        raise VerificationError("source frozen package is missing or changed")
    metadata = directory / "meta" / f"{client.database}_schema_hash.txt"
    if metadata.read_text().strip() != record["schema_hash"]:
        raise VerificationError("source schema metadata is missing or changed")
    verified = {"run_id": run_id, "database_uuid": db_uuid, "package_sha256": identity["package_sha256"],
                "schema_hash": record["schema_hash"], "state_directory": str(directory)}
    if any(key in source and source[key] != value for key, value in verified.items()):
        raise VerificationError("source provenance differs from its recorded native run")
    return {**source, **verified}


@contextmanager
def verified_source(client, source):
    exists = int(client.one("SELECT count() AS n FROM system.tables WHERE database={db:String} "
        "AND name='_evm_state_run'", {"db": client.database})["n"])
    if not exists:
        raise VerificationError("source has no guarded native run ownership record")
    owner = client.one("SELECT run_id,identity FROM _evm_state_run")
    try:
        identity = json.loads(owner["identity"])
        # Old prototype sources can be exported through existing ready checkpoints;
        # do not silently adopt their mutable history into newly published state.
        if not isinstance(identity, dict) or identity.get("format_version") != 2:
            raise VerificationError("source uses an old native run identity; use a fresh guarded continuation")
        directory = Path(identity["state_directory"]).resolve()
        if (identity["state_directory"] != str(directory) or not directory.is_dir()
                or identity.get("host") != socket.gethostname()):
            raise VerificationError("source run has no matching host/directory binding")
    except (KeyError, TypeError, OSError, AttributeError) as error:
        raise VerificationError("source run metadata is missing or invalid") from error
    # Source-history cleanup must take this lock exclusively, in addition to
    # excluding its native writer. Appends can continue beyond our fixed end.
    with file_lock(directory / "source_readers.lock"):
        try:
            checked = _validate(client, source, identity, owner["run_id"], directory)
        except (KeyError, TypeError, OSError, AttributeError) as error:
            raise VerificationError("source run metadata is missing or invalid") from error
        yield checked
