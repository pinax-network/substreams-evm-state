"""Remove whole native history partitions after verified checkpoint publication.

Each source is bound to one checkpoint database. All of that database's retained
checkpoints for these accounts protect their continuation intervals. This cleans
already checkpointed history; initial-history compaction is a separate operation.
"""
import json
from pathlib import Path

from .checkpoint import _manifest, connect_like
from .control import control
from .cursor import load_progress
from .files import exclusive_lock
from .proof import VerificationError
from .source import verified_source

PARTITIONS = {"state_blocks": "toDate(_block_timestamp_)", "_blocks_": "toYYYYMM(timestamp)"}


def _plan(client, checkpoints, run, checked, snapshot_id, directory, keep_blocks):
    if isinstance(keep_blocks, bool) or not isinstance(keep_blocks, int) or keep_blocks < 1:
        raise ValueError("keep_blocks must be at least one")
    if run["identity"]["checkpoint_database"] != checkpoints.database:
        raise VerificationError("native history belongs to another checkpoint database")
    ready = _manifest(checkpoints, snapshot_id)
    covered = [source for source in ready["sources"] if source.get("run_id") == run["run_id"]]
    provenance = ["database_uuid", "package_sha256", "schema_hash", "module_hash", "accounts"]
    if len(covered) != 1 or any(covered[0].get(key) != checked.get(key) for key in provenance):
        raise VerificationError("checkpoint does not cover this exact native source")
    selected = set(run["identity"]["accounts"])
    if not selected.issubset(ready["accounts"]):
        raise VerificationError("checkpoint does not cover all source accounts")
    progress = load_progress(client, run, directory)
    tip = progress["position"]["block"]
    if (Path(directory) / "cursor.txt").read_text().strip() != progress["cursor"]:
        raise VerificationError("native cursor differs from durable progress; recover or finish the run before cleanup")
    if tip["number"] < ready["header"]["number"]:
        raise VerificationError("native progress has not reached the checkpoint")
    # Preserve updates following every retained checkpoint, including pins and
    # checkpoints imported from another source. Publication/rotation is excluded
    # while this list and the cleanup plan are in use.
    floor = ready["header"]["number"]
    protected = []
    for row in checkpoints.rows("SELECT snapshot_id,manifest FROM checkpoints FINAL"):
        record = json.loads(row["manifest"])
        if record.get("status") != "ready" or record.get("snapshot_id") != row["snapshot_id"]:
            raise VerificationError("invalid retained checkpoint prevents native history cleanup")
        if selected.intersection(record["accounts"]):
            floor = min(floor, int(record["header"]["number"]))
            protected.append(record["snapshot_id"])
    remove_before = min(floor + 1, tip["number"] - keep_blocks + 1)
    schemas = {r["name"]: r["partition_key"] for r in client.rows(
        "SELECT name,partition_key FROM system.tables WHERE database={db:String} "
        "AND name IN ('state_blocks','_blocks_')", {"db": client.database})}
    if schemas != PARTITIONS:
        raise VerificationError("native history schema does not match the qualified partition layout")
    tables = {}
    for table in PARTITIONS:
        tables[table] = list(client.rows(f"SELECT _partition_id AS id,min(number) AS first,max(number) AS last "
            f"FROM {table} GROUP BY _partition_id HAVING last < {{before:UInt64}} ORDER BY first",
            {"before": max(0, remove_before)}))
    return {"source": checked, "checkpoint": snapshot_id, "protected_checkpoints": sorted(protected),
            "remove_before": max(0, remove_before), "keep_blocks": keep_blocks,
            "durable_block": tip, "partitions": tables}


def cleanup(client, directory, snapshot_id, keep_blocks=10000, apply=False):
    directory = Path(directory).resolve()
    run = json.loads((directory / "run.json").read_text())
    identity = run["identity"]
    if identity.get("format_version") != 3:
        raise VerificationError("native cleanup requires destination-bound run identity format 3")
    checkpoints = connect_like(client, identity["checkpoint_database"])
    source = {key: identity[key] for key in ["database", "accounts", "start_block", "module_hash", "final_blocks_only"]}
    # Same order as publication. Both writer locks fail instead of deadlocking
    # with a reader/build in another process; raw SQL bypasses this protocol.
    with control(checkpoints).publisher(), exclusive_lock(directory / "run.lock"):
        with verified_source(client, source, checkpoints) as checked:
            if checked["state_directory"] != str(directory) or checked["run_id"] != run["run_id"]:
                raise VerificationError("cleanup directory does not own this native source")
        with exclusive_lock(directory / "source_readers.lock"):
            result = _plan(client, checkpoints, run, checked, snapshot_id, directory, keep_blocks)
            result["bytes_before"] = client.disk_usage()
            if apply:
                for table, partitions in result["partitions"].items():
                    for partition in partitions:
                        client.execute(f"ALTER TABLE {table} DROP PARTITION ID {{id:String}}", {"id": partition["id"]})
                # The cursor's block and marker must still be present after all
                # drops. Interrupted cleanup can recompute the plan and resume.
                load_progress(client, run, directory)
            result.update(applied=apply, bytes_after=client.disk_usage(),
                space_reclamation="inactive parts may remain until ClickHouse background cleanup; byte counts include them")
            return result
