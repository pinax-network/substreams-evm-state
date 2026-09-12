"""Restore a portable checkpoint into a fresh immutable generation, then publish."""
import hashlib
import json
from pathlib import Path
import tempfile
import time
import uuid

from .checkpoint import setup
from .control import control
from .export import ACCOUNT_FIELDS, _check_file, _json, _page_rows, _verify
from .proof import VerificationError, unhex, verify_account, verify_complete
from .retention import require_partitioned_schema
from .triedb import TrieDB
from .capacity import check as capacity_check


def _verify_stored(client, snapshot_id, record, work_dir):
    accounts = list(client.rows("SELECT address,exists,nonce,balance,code_hash,code,storage_root,nonzero_slots "
        "FROM checkpoint_accounts FINAL WHERE snapshot_id={id:String} ORDER BY address", {"id": snapshot_id}))
    if [row["address"] for row in accounts] != record["accounts"]:
        raise VerificationError("restored account coverage differs from the checkpoint")
    digest, count = hashlib.sha256(), 0
    for metadata in accounts:
        account = metadata["address"]
        metadata["nonce"] = int(metadata["nonce"])
        metadata["nonzero_slots"] = int(metadata["nonzero_slots"])
        evidence = record["proof_bundle"]["accounts"][account]
        proven = verify_account(record["header"]["state_root"], account, evidence["proof"])
        if metadata["exists"] != proven.exists or unhex(metadata["storage_root"], 32) != proven.storage_root or metadata["code"] != evidence["code"]:
            raise VerificationError("restored metadata differs from its proven account")
        def slots():
            for row in client.rows("SELECT slot,value FROM checkpoint_storage FINAL WHERE snapshot_id={id:String} "
                    "AND address={address:String} ORDER BY slot", {"id": snapshot_id, "address": account}):
                digest.update((account + row["slot"] + row["value"]).encode())
                yield row["slot"], row["value"]
        with tempfile.TemporaryDirectory(prefix="evm-import-verify-", dir=work_dir) as directory:
            trie = TrieDB(Path(directory) / "nodes.sqlite")
            try:
                slots_count = verify_complete(proven, slots(), metadata["code"], metadata, trie)
                capacity_check(client, [work_dir, control(client).path], "import-trie")
            finally:
                trie.close()
        if slots_count != metadata["nonzero_slots"]:
            raise VerificationError("restored storage count differs from account metadata")
        count += slots_count
        digest.update(json.dumps(metadata, sort_keys=True, separators=(",", ":")).encode())
    actual = int(client.one("SELECT count() AS n FROM checkpoint_storage FINAL WHERE snapshot_id={id:String}",
                            {"id": snapshot_id})["n"])
    if actual != count or count != record["nonzero_slots"] or digest.hexdigest() != record["state_sha256"]:
        raise VerificationError("restored checkpoint contains missing, unexpected or altered state")


def import_checkpoint(client, directory, expected_hash=None, work_dir=None, budget_bytes=100_000_000_000):
    directory = Path(directory).resolve()
    # Validate before any database mutation. Validate the stored candidate again
    # after insertion, including files that could change between these two reads.
    layout = _json(directory / "manifest.json")
    _verify(directory, layout, expected_hash, work_dir, capacity_client=client)
    record = layout["checkpoint"]
    setup(client)
    require_partitioned_schema(client)
    work_dir = Path(work_dir or tempfile.gettempdir())
    work_dir.mkdir(parents=True, exist_ok=True)
    with control(client).publisher():
        capacity_paths = [directory, work_dir, control(client).path]
        capacity_check(client, capacity_paths, "import-start")
        if client.disk_usage() >= budget_bytes:
            raise VerificationError("retained-data budget already exhausted")
        snapshot_id = uuid.uuid4().hex
        for item in layout["storage_pages"]:
            client.insert("checkpoint_storage", ({"snapshot_id": snapshot_id, **row} for row in _page_rows(directory, item)), batch_size=10000)
            if client.disk_usage() >= budget_bytes:
                raise VerificationError("restore exceeds retained-data budget; candidate remains unpublished")
            capacity_check(client, capacity_paths, "import-page")
        accounts = _json(_check_file(directory, layout["account_file"]))
        if not isinstance(accounts, list) or any(not isinstance(row, dict) or set(row) != ACCOUNT_FIELDS for row in accounts):
            raise VerificationError("invalid restored account fields")
        client.insert("checkpoint_accounts", ({**row, "snapshot_id": snapshot_id, "nonce": int(row["nonce"])} for row in accounts))
        _verify_stored(client, snapshot_id, record, work_dir)
        if client.disk_usage() >= budget_bytes:
            raise VerificationError("restore exceeds retained-data budget; candidate remains unpublished")
        restored = {**record, "snapshot_id": snapshot_id, "base_snapshot": None, "sources": [], "created_at": time.time_ns(),
            "retained_budget_bytes": budget_bytes,
            "imported_from": {"snapshot_id": record["snapshot_id"], "sources": record["sources"],
                "manifest_content_sha256": hashlib.sha256(json.dumps(layout, sort_keys=True, separators=(",", ":")).encode()).hexdigest()}}
        capacity_check(client, capacity_paths, "import-publish")
        client.insert("checkpoints", [{"snapshot_id": snapshot_id, "block_number": restored["header"]["number"],
            "block_hash": restored["header"]["hash"], "created_at": restored["created_at"],
            "manifest": json.dumps(restored, sort_keys=True, separators=(",", ":"))}])
        return restored
