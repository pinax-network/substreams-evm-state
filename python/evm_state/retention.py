"""Drop whole checkpoint generations, retaining pins and every account's newest state."""
import json

from .control import control, object_id
from .proof import VerificationError
from .reader import _pin

TABLES = ("checkpoints", "checkpoint_accounts", "checkpoint_storage")


def require_partitioned_schema(client):
    rows = list(client.rows("SELECT name,partition_key FROM system.tables WHERE database={db:String} "
        "AND name IN ('checkpoints','checkpoint_accounts','checkpoint_storage')", {"db": client.database}))
    if len(rows) != 3 or any(row["partition_key"] != "snapshot_id" for row in rows):
        raise VerificationError("checkpoint tables use an old prototype schema; export and import into a fresh database before retention")


def _plan(client, owner, keep_latest):
    if isinstance(keep_latest, bool) or not isinstance(keep_latest, int) or keep_latest < 1:
        raise ValueError("keep_latest must be at least one")
    require_partitioned_schema(client)
    records = []
    for row in client.rows("SELECT snapshot_id,manifest FROM checkpoints FINAL ORDER BY block_number DESC, created_at DESC"):
        value = json.loads(row["manifest"])
        if value.get("snapshot_id") != object_id(row["snapshot_id"]) or value.get("status") != "ready":
            raise VerificationError("invalid ready manifest blocks retention")
        records.append(value)
    ready = {row["snapshot_id"] for row in records}
    keep = {row["snapshot_id"] for row in records[:keep_latest]}
    latest_for_account = {}
    for record in records:
        for account in record["accounts"]:
            if account not in latest_for_account:
                latest_for_account[account] = record["snapshot_id"]
                keep.add(record["snapshot_id"])
    pins = [_pin(owner, path.stem) for path in owner.pins.glob("*.json")]
    for pinned in pins:
        if pinned["snapshot_id"] not in ready:
            raise VerificationError("a pinned checkpoint is missing; retention stopped")
        keep.add(pinned["snapshot_id"])
    # Partition metadata avoids scanning every storage slot. For String keys,
    # system.parts.partition is the quoted value; IDs here contain only hex.
    partitions = set()
    for row in client.rows("SELECT DISTINCT partition FROM system.parts WHERE database={db:String} AND active "
            "AND table IN ('checkpoints','checkpoint_accounts','checkpoint_storage')", {"db": client.database}):
        partitions.add(object_id(row["partition"].strip("'")))
    remove = partitions - keep
    return {"keep": sorted(keep), "remove": sorted(remove), "unpublished_candidates": sorted(remove - ready),
            "pins": [{"pin_id": p["pin_id"], "snapshot_id": p["snapshot_id"]} for p in pins],
            "latest_for_account": latest_for_account, "keep_latest": keep_latest}


def plan(client, keep_latest=2):
    owner = control(client)
    with owner.retention():
        return _plan(client, owner, keep_latest)


def prune(client, keep_latest=2):
    owner = control(client)
    with owner.retention():
        result = _plan(client, owner, keep_latest)
        result["bytes_before"] = client.disk_usage()
        for snapshot_id in result["remove"]:
            # Remove the publication marker first. If interrupted, subsequent
            # cleanup identifies the remaining partitions as unpublished orphans.
            for table in TABLES:
                client.execute(f"ALTER TABLE {table} DROP PARTITION {{id:String}}", {"id": snapshot_id})
        result["bytes_after"] = client.disk_usage()
        result["applied"] = True
        result["space_reclamation"] = "ClickHouse may retain inactive parts until background cleanup; bytes_after includes them"
        return result
