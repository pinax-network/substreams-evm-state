"""Compact a contiguous initial replay prefix without claiming verified state.

An immutable generation contains current nonzero slots and observed account
fields. Its synced local pointer commits only after the stored generation is
checksummed. Pruning follows that commit; checkpoint publication still requires
complete account/storage proofs at the final target. Not an export/read endpoint.
"""
import hashlib
import json
from pathlib import Path
import uuid

from .checkpoint import ZERO, _observed_fields, _union_storage, connect_like, setup, validate_interval
from .control import control, object_id
from .cursor import binding, load_progress
from .files import atomic_json, exclusive_lock
from .history import partitions_before
from .proof import VerificationError, unhex
from .source import verified_source
from .capacity import check as capacity_check


def _digest(client, generation, fields, accounts):
    digest = hashlib.sha256(json.dumps(fields, sort_keys=True, separators=(",", ":")).encode())
    if set(fields) - set(accounts):
        raise VerificationError("bootstrap has account fields outside its filter")
    count, previous = 0, None
    for row in client.rows("SELECT address,slot,value FROM bootstrap_storage WHERE generation={id:String} "
                           "ORDER BY address,slot", {"id": object_id(generation)}):
        key = (row["address"], row["slot"])
        if row["address"] not in accounts or (previous is not None and key <= previous):
            raise VerificationError("bootstrap storage has unexpected accounts or duplicate keys")
        unhex(row["slot"], 32)
        if not int.from_bytes(unhex(row["value"], 32), "big"):
            raise VerificationError("bootstrap contains a noncanonical zero slot")
        digest.update((row["address"] + row["slot"] + row["value"]).encode())
        previous = key
        count += 1
    return {"state_sha256": digest.hexdigest(), "nonzero_slots": count}


def load_prefix(client, directory, run=None):
    directory = Path(directory)
    pointer = directory / "bootstrap.json"
    if not pointer.exists():
        return None
    run = run or json.loads((directory / "run.json").read_text())
    value = json.loads(pointer.read_text())
    if (value.get("format_version") != 1 or value.get("status") != "unverified-bootstrap" or value.get("binding") != binding(run)
            or value.get("start_block") != run["identity"]["start_block"]
            or value.get("accounts") != run["identity"]["accounts"]):
        raise VerificationError("bootstrap prefix belongs to another source or is corrupt")
    generation = object_id(value["generation"])
    stored = client.one("SELECT manifest FROM bootstrap_generations WHERE generation={id:String}", {"id": generation})
    if json.loads(stored["manifest"]) != value:
        raise VerificationError("bootstrap pointer and stored manifest differ")
    if value["header"]["number"] < value["start_block"]:
        raise VerificationError("bootstrap prefix range is invalid")
    measured = _digest(client, generation, value["fields"], value["accounts"])
    if any(value.get(key) != actual for key, actual in measured.items()):
        raise VerificationError("bootstrap generation checksum or count differs")
    return value


def select_prefix(client, source):
    # Never trust an accumulator or internal range supplied in sources.json.
    source = {k: v for k, v in source.items() if k not in {"bootstrap", "delta_start"}}
    prefix = load_prefix(client, source["state_directory"])
    if prefix and source["start_block"] <= prefix["header"]["number"]:
        if source["start_block"] != prefix["start_block"]:
            raise VerificationError("requested range begins inside a compacted bootstrap prefix")
        source["bootstrap"] = prefix
        source["delta_start"] = prefix["header"]["number"] + 1
    return source


def _drop_old_generations(client, keep):
    for table in ["bootstrap_generations", "bootstrap_storage"]:
        old = list(client.rows("SELECT DISTINCT generation FROM " + table + " WHERE generation != {id:String}",
                               {"id": keep}))
        for row in old:
            client.execute(f"ALTER TABLE {table} DROP PARTITION {{id:String}}", {"id": object_id(row["generation"])})


def _cleanup(client, prefix):
    # Keep the prefix's last native block, which may also be the durable cursor.
    # This can be repeated after a crash between any of the partition drops.
    partitions = partitions_before(client, prefix["header"]["number"])
    for table, values in partitions.items():
        for partition in values:
            client.execute(f"ALTER TABLE {table} DROP PARTITION ID {{id:String}}", {"id": partition["id"]})
    _drop_old_generations(client, prefix["generation"])
    return partitions


def _require_initial(checkpoints, accounts):
    # A zero omitted from initial state must never hide a clear of an old base.
    for row in checkpoints.rows("SELECT manifest FROM checkpoints FINAL"):
        if set(json.loads(row["manifest"])["accounts"]).intersection(accounts):
            raise VerificationError("accounts already have a checkpoint; use checkpoint continuation and source retention")


def compact(client, directory, end_block=None, budget_bytes=100_000_000_000):
    if isinstance(budget_bytes, bool) or not isinstance(budget_bytes, int) or budget_bytes < 1:
        raise ValueError("bootstrap budget must be a positive integer")
    directory = Path(directory).resolve()
    run = json.loads((directory / "run.json").read_text())
    identity = run["identity"]
    if identity.get("format_version") != 3:
        raise VerificationError("bootstrap compaction requires a destination-bound native run")
    checkpoints = connect_like(client, identity["checkpoint_database"])
    setup(checkpoints)
    declared = {k: identity[k] for k in ["database", "accounts", "start_block", "module_hash", "final_blocks_only"]}
    with control(checkpoints).publisher(), exclusive_lock(directory / "run.lock"):
        with verified_source(client, declared, checkpoints) as checked:
            if checked["state_directory"] != str(directory) or checked["run_id"] != run["run_id"]:
                raise VerificationError("bootstrap directory does not own this source")
        with exclusive_lock(directory / "source_readers.lock"):
            capacity_paths = [directory, control(checkpoints).path]
            capacity_check(client, capacity_paths, "bootstrap-start")
            # Initial-state compaction intentionally drops explicit zero slots.
            # It cannot compact a continuation of an already-ready account: those
            # zeros may be needed to clear values inherited from an older base.
            _require_initial(checkpoints, identity["accounts"])
            progress = load_progress(client, run, directory)
            tip = progress["position"]["block"]["number"]
            if (directory / "cursor.txt").read_text().strip() != progress["cursor"]:
                raise VerificationError("recover or complete the native cursor before compaction")
            end = tip if end_block is None else end_block
            if isinstance(end, bool) or not isinstance(end, int) or not identity["start_block"] <= end <= tip:
                raise VerificationError("bootstrap end must be within durable native progress")
            header = client.one("SELECT number,hash,parent_hash,state_root,timestamp FROM state_blocks FINAL "
                                "WHERE number={end:UInt64}", {"end": end})
            source = select_prefix(client, checked)
            previous = source.get("bootstrap")
            validate_interval(client, source, header)
            if previous and end == previous["header"]["number"]:
                removed = _cleanup(client, previous)
                load_progress(client, run, directory)
                return {**previous, "removed_partitions": removed, "already_compacted": True}
            for statement in (Path(__file__).parent / "bootstrap.sql").read_text().split(";"):
                if statement.strip(): client.execute(statement)
            # An interrupted candidate is disposable only after the complete
            # input interval and prior committed prefix have been validated.
            _drop_old_generations(client, previous["generation"] if previous else "")
            used = client.disk_usage() + (checkpoints.disk_usage() if checkpoints.database != client.database else 0)
            if used >= budget_bytes:
                raise VerificationError("bootstrap database budget already exhausted")
            generation = uuid.uuid4().hex
            params = {"id": generation, "end": end}
            union = _union_storage([source], None, client, params)
            client.execute("INSERT INTO bootstrap_storage SELECT {id:String},address,slot,argMax(value,position) AS final_value "
                f"FROM ({union}) GROUP BY address,slot HAVING final_value != '{ZERO}'", params)
            fields = _observed_fields(client, [source], None, params)
            measured = _digest(client, generation, fields, identity["accounts"])
            used = client.disk_usage() + (checkpoints.disk_usage() if checkpoints.database != client.database else 0)
            if used >= budget_bytes:
                raise VerificationError("bootstrap candidate exceeds database budget; previous prefix and source retained")
            record = {"format_version": 1, "status": "unverified-bootstrap", "binding": binding(run),
                      "generation": generation, "start_block": identity["start_block"], "header": header,
                      "accounts": identity["accounts"], "fields": fields, **measured}
            client.insert("bootstrap_generations", [{"generation": generation, "manifest": json.dumps(record, sort_keys=True)}])
            capacity_check(client, capacity_paths, "bootstrap-commit")
            atomic_json(directory / "bootstrap.json", record, overwrite=True)
            # Re-read the committed data before irreversible local history cleanup.
            load_prefix(client, directory, run)
            removed = _cleanup(client, record)
            load_progress(client, run, directory)
            return {**record, "removed_partitions": removed, "database_bytes": client.disk_usage()}


def replay(client, spkg, endpoint, accounts, start_block, directory, dsn, stop_block,
           chunk_blocks=100000, budget_bytes=100_000_000_000, max_retries=3,
           checkpoint_database=None, decode_batch_size=32, spool_max_idle_ms=1000, prometheus_addr=None):
    """Resume bounded native chunks, compacting each before starting the next.

    Stop is exclusive, as in the native CLI. This limits accumulated history to
    a chunk plus partition granularity; budget checks are database measurements,
    not a hard limit on total disk allocation or merge/verification workspace.
    """
    from .ingest import ingest, prepare
    for name, value in [("chunk blocks", chunk_blocks), ("budget", budget_bytes)]:
        if isinstance(value, bool) or not isinstance(value, int) or value < 1:
            raise ValueError(f"bootstrap {name} must be a positive integer")
    if isinstance(stop_block, bool) or not isinstance(stop_block, int) or stop_block <= start_block:
        raise ValueError("bootstrap stop block must be greater than start block (exclusive)")
    directory = Path(directory).resolve()
    run = prepare(client, spkg, endpoint, accounts, start_block, directory, dsn, checkpoint_database)
    values = (client, directory / "package.spkg", endpoint, accounts, start_block, directory, dsn)
    destination = connect_like(client, run["identity"]["checkpoint_database"])
    setup(destination)
    with exclusive_lock(directory / "bootstrap_replay.lock"):
        with control(destination).publisher():
            _require_initial(destination, run["identity"]["accounts"])
        while True:
            # Ingest is responsible for validating/recovering a cursor written
            # after the last backup. Do not guess resume position from raw rows.
            cursor = directory / "cursor.txt"
            if cursor.is_file() and cursor.read_text().strip():
                from .cursor import save_progress
                with exclusive_lock(directory / "run.lock"):
                    progress = save_progress(client, run, directory, cursor.read_text().strip())
                    next_block = progress["position"]["block"]["number"] + 1
                if next_block > stop_block:
                    raise VerificationError("bootstrap stop precedes the already ingested cursor")
                # Also finishes cleanup if the last run died after the pointer
                # commit or before compaction. No new ingestion until it succeeds.
                prefix = compact(client, directory, budget_bytes=budget_bytes)
                if next_block == stop_block:
                    return {"run_id": run["run_id"], "status": "unverified-bootstrap",
                            "generation": prefix["generation"], "header": prefix["header"],
                            "nonzero_slots": prefix["nonzero_slots"], "state_sha256": prefix["state_sha256"],
                            "source": {k: run["identity"][k] for k in
                                ["database", "accounts", "start_block", "module_hash", "final_blocks_only"]}}
            else:
                next_block = start_block
            used = client.disk_usage() + (destination.disk_usage() if destination.database != client.database else 0)
            if used >= budget_bytes:
                raise VerificationError("bootstrap database budget already exhausted")
            ingest(*values, stop_block=min(next_block + chunk_blocks, stop_block), max_retries=max_retries,
                   checkpoint_database=checkpoint_database, decode_batch_size=decode_batch_size,
                   spool_max_idle_ms=spool_max_idle_ms, prometheus_addr=prometheus_addr)
