"""Bind one finalized native sink to one immutable package, filter and database.

Cursor loss is explicit recovery work: never silently start an old account list
over a database belonging to another run. Keep this directory on durable storage.
"""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import urllib.parse
import uuid

from .checkpoint import canonical_accounts, connect_like
from .files import atomic_json, atomic_write, exclusive_lock
from .proof import VerificationError
from .ch import identifier
from .cursor import load_progress, observe, save_progress
from .capacity import check as capacity_check
from . import host

MODULE = "map_block_state"


def _native(command, dsn):
    env = dict(os.environ, SUBSTREAMS_SINK_DSN=dsn)
    result = subprocess.run(command, env=env, capture_output=True, text=True, timeout=120)
    if result.returncode:
        # DSNs can appear in third-party errors. Do not echo credentials.
        raise RuntimeError("native Substreams command failed: " + result.stderr[-4000:].replace(dsn, "[database]"))
    return result.stdout


def _identity(client, spkg, endpoint, accounts, start_block, directory, dsn, checkpoint_database=None):
    if isinstance(start_block, bool) or not isinstance(start_block, int) or start_block < 0:
        raise ValueError("start block must be an absolute nonnegative integer")
    parsed = urllib.parse.urlsplit(dsn)
    http = urllib.parse.urlsplit(client.url)
    normalize_host = lambda value: "127.0.0.1" if value == "localhost" else value
    if parsed.scheme != "clickhouse" or parsed.path != "/" + client.database or normalize_host(parsed.hostname) != normalize_host(http.hostname):
        raise ValueError("native DSN and HTTP client must name the same ClickHouse host and database")
    if http.username or http.password or http.query or http.fragment:
        raise ValueError("use CH_USER/CH_PASSWORD for HTTP credentials")
    selected = canonical_accounts(accounts)
    package = Path(spkg).read_bytes()
    info = json.loads(_native(["substreams", "info", str(spkg), "--json", "-p", MODULE + "=" + ",".join(selected)], dsn))
    if Path(spkg).read_bytes() != package:
        raise VerificationError("package changed during preparation; retry after the build finishes")
    module = next((v for v in info["modules"] if v["name"] == MODULE), {})
    if info.get("network") != "bsc" or module.get("output_type") != "proto:evm.state.v1.BlockState":
        raise VerificationError("expected the BSC native block-state package")
    local_host = host.machine_id()
    previous = directory / "run.json"
    if previous.exists():
        previous_identity = json.loads(previous.read_text())["identity"]
        if host.matches(previous_identity, directory):
            local_host = previous_identity["host"]
    return {"format_version": 3, "database": client.database, "http_url": client.url,
        "checkpoint_database": identifier(checkpoint_database or client.database),
        "state_directory": str(directory), "host": local_host,
        "native_target": f"{parsed.hostname}:{parsed.port or 9000}/{client.database}",
        "endpoint": endpoint, "accounts": selected, "start_block": start_block,
        "module": MODULE, "module_hash": module["hash"], "package_sha256": hashlib.sha256(package).hexdigest(),
        "network": "bsc", "schema_version": 1, "final_blocks_only": True}, package


def _prepare(client, spkg, endpoint, accounts, start_block, directory, dsn, checkpoint_database=None):
    identity, package = _identity(client, spkg, endpoint, accounts, start_block, directory, dsn, checkpoint_database)
    record_path = directory / "run.json"
    admin = connect_like(client, "default")
    exists = bool(int(admin.one("SELECT count() AS n FROM system.databases WHERE name={db:String}",
                                {"db": client.database})["n"]))
    if record_path.exists():
        record = json.loads(record_path.read_text())
        if record.get("phase") not in {"preparing", "prepared"}:
            raise VerificationError("invalid native run phase")
        if record["identity"] != identity:
            raise VerificationError("run identity changed; use a new isolated database and state directory")
        if record["phase"] == "prepared" and not exists:
            raise VerificationError("run database is missing; restore matching database and cursor metadata")
    else:
        if exists and int(client.one("SELECT count() AS n FROM system.tables WHERE database={db:String}",
                                      {"db": client.database})["n"]):
            raise VerificationError("database already contains tables without this run's local identity")
        if any(directory.iterdir()):
            unexpected = [p.name for p in directory.iterdir() if p.name != "run.lock"]
            if unexpected:
                raise VerificationError("state directory is not empty and has no run identity")
        record = {"run_id": uuid.uuid4().hex, "identity": identity, "phase": "preparing"}
        atomic_json(record_path, record)
    frozen = directory / "package.spkg"
    if frozen.exists():
        if hashlib.sha256(frozen.read_bytes()).hexdigest() != identity["package_sha256"]:
            raise VerificationError("frozen run package was changed")
    elif record["phase"] == "prepared":
        raise VerificationError("frozen run package is missing")
    else:
        atomic_write(frozen, package)
    admin.execute(f"CREATE DATABASE IF NOT EXISTS {client.database}")
    db_uuid = client.one("SELECT toString(uuid) AS uuid FROM system.databases WHERE name={db:String}",
                         {"db": client.database})["uuid"]
    if record.get("database_uuid", db_uuid) != db_uuid:
        raise VerificationError("database was replaced; restore matching database and run metadata")
    owner_exists = int(client.one("SELECT count() AS n FROM system.tables WHERE database={db:String} AND name='_evm_state_run'",
                                  {"db": client.database})["n"])
    if not owner_exists:
        if record["phase"] == "prepared":
            raise VerificationError("database run identity is missing")
        # No IF NOT EXISTS: two different state directories cannot both claim a DB.
        client.execute("CREATE TABLE _evm_state_run (run_id String, identity String) ENGINE=MergeTree ORDER BY run_id "
                       "SETTINGS fsync_after_insert=1, fsync_part_directory=1")
        client.insert("_evm_state_run", [{"run_id": record["run_id"], "identity": json.dumps(identity, sort_keys=True)}])
    owner = client.one("SELECT run_id,identity FROM _evm_state_run")
    if owner["run_id"] != record["run_id"] or json.loads(owner["identity"]) != identity:
        raise VerificationError("database belongs to a different native run")
    metadata = directory / "meta" / f"{client.database}_schema_hash.txt"
    if record["phase"] == "preparing":
        metadata.parent.mkdir(exist_ok=True)
        _native(["substreams", "sink", "clickhouse", "setup", str(frozen), MODULE,
            "--bytes-encoding", "0xhex", "--sink-info-folder", str(metadata.parent)], dsn)
        # One-row block envelopes are atomic; also require durable acknowledged parts.
        for table in ["state_blocks", "_blocks_"]:
            client.execute(f"ALTER TABLE {table} MODIFY SETTING fsync_after_insert=1, fsync_part_directory=1")
        with metadata.open("rb") as handle:
            os.fsync(handle.fileno())
        record.update(phase="prepared", database_uuid=db_uuid, schema_hash=metadata.read_text().strip())
        atomic_json(record_path, record, overwrite=True)
    if not metadata.is_file() or metadata.read_text().strip() != record["schema_hash"]:
        raise VerificationError("native schema metadata is missing or changed")
    return record


def prepare(client, spkg, endpoint, accounts, start_block, directory, dsn, checkpoint_database=None):
    directory = Path(directory).resolve()
    capacity_check(client, [directory], "native-prepare")
    with exclusive_lock(directory / "run.lock"):
        return _prepare(client, spkg, endpoint, accounts, start_block, directory, dsn, checkpoint_database)


def recover_cursor(client, spkg, endpoint, accounts, start_block, directory, dsn, checkpoint_database=None):
    directory = Path(directory).resolve()
    with exclusive_lock(directory / "run.lock"):
        run = _prepare(client, spkg, endpoint, accounts, start_block, directory, dsn, checkpoint_database)
        progress = load_progress(client, run, directory)
        cursor = directory / "cursor.txt"
        # Preserve the damaged cursor as evidence without treating it as trusted
        # progress. Never overwrite a directory or silently adopt an older run.
        if cursor.is_file():
            atomic_write(directory / ("cursor-before-recovery-" + uuid.uuid4().hex + ".txt"), cursor.read_bytes())
        atomic_write(cursor, progress["cursor"].encode(), overwrite=True)
        return {"run_id": run["run_id"], "recovered": True, "position": progress["position"]}


def validate_parallel_workers(value):
    if value is not None and (isinstance(value, bool) or not isinstance(value, int) or value < 1):
        raise ValueError("parallel workers must be a positive integer or omitted")


def ingest(client, spkg, endpoint, accounts, start_block, directory, dsn, stop_block=None, max_retries=3,
           checkpoint_database=None, decode_batch_size=1, spool_max_idle_ms=100, prometheus_addr=None,
           parallel_workers=None):
    validate_parallel_workers(parallel_workers)
    directory = Path(directory).resolve()
    if stop_block is not None and stop_block <= start_block:
        raise ValueError("stop block must be greater than start block (exclusive)")
    if isinstance(decode_batch_size, bool) or not isinstance(decode_batch_size, int) or decode_batch_size < 1:
        raise ValueError("decode batch size must be a positive integer")
    if isinstance(spool_max_idle_ms, bool) or not isinstance(spool_max_idle_ms, int) or spool_max_idle_ms < 1:
        raise ValueError("spool maximum idle milliseconds must be a positive integer")
    with exclusive_lock(directory / "run.lock") as run_lock:
        capacity_check(client, [directory], "native-start")
        record = _prepare(client, spkg, endpoint, accounts, start_block, directory, dsn, checkpoint_database)
        cursor = directory / "cursor.txt"
        blocks = int(client.one("SELECT count() AS n FROM state_blocks FINAL")["n"])
        if blocks and (not cursor.is_file() or not cursor.read_text().strip()):
            raise VerificationError("native cursor is missing or empty for existing data; explicit recovery is required")
        if not blocks and cursor.exists():
            raise VerificationError("cursor exists without its block data; restore matching database and metadata")
        report = {"run_id": record["run_id"], "source": {"database": client.database,
            "accounts": record["identity"]["accounts"], "start_block": start_block,
            "module_hash": record["identity"]["module_hash"], "final_blocks_only": True}}
        if blocks:
            progress = save_progress(client, record, directory, cursor.read_text().strip())
            if stop_block is not None and progress["position"]["block"]["number"] >= stop_block - 1:
                if progress["position"]["block"]["number"] != stop_block - 1:
                    raise VerificationError("requested stop precedes the already ingested cursor")
                return {**report, "already_complete": True, "position": progress["position"]}
        elif (directory / "durable_progress.json").exists():
            raise VerificationError("durable progress exists without its block data")
        command = ["substreams", "sink", "clickhouse", str(directory / "package.spkg"), MODULE,
            "-e", endpoint, "-p", MODULE + "=" + ",".join(record["identity"]["accounts"]), "-s", str(start_block),
            "--final-blocks-only", "--bytes-encoding", "0xhex", "--sink-info-folder", str(directory / "meta"),
            "--cursor-file-path", str(cursor), "--spool-dir", str(directory / "spool"), "--spool-max-size", "1GiB",
            "--spool-max-idle", f"{spool_max_idle_ms}ms", "--max-retries", str(max_retries)]
        # The pinned sink's direct-insert transition requires STEP_NEW. A
        # finalized-only follow stays STEP_NEW_IRREVERSIBLE even at the head,
        # so it needs a one-block decode batch and a short spool idle timeout.
        # Otherwise the default 32-block batch adds ~15 seconds on BSC, and
        # one-block batches alone can keep the spool open until its size target.
        command.extend(["--decode-batch-size", str(decode_batch_size)])
        if parallel_workers is not None:
            command.extend(["--header", f"X-Substreams-Parallel-Workers:{parallel_workers}"])
        if prometheus_addr is not None:
            command.extend(["--prometheus-addr", prometheus_addr])
        if stop_block is not None:
            command.extend(["-t", str(stop_block)])
        # If this wrapper is SIGKILLed, its native child may remain alive. Keep
        # the same flock open in that child so another wrapper cannot take over
        # while the orphan still writes. Normal cleanup waits for the child
        # before releasing the lock. Supported platforms are POSIX (Linux/macOS).
        process = subprocess.Popen(command, env=dict(os.environ, SUBSTREAMS_SINK_DSN=dsn),
                                   pass_fds=(run_lock.fileno(),))
        try:
            while True:
                try:
                    result = process.wait(timeout=1)
                    break
                except subprocess.TimeoutExpired:
                    observe(client, record, directory)
                    capacity_check(client, [directory], "native-progress")
            if result:
                raise RuntimeError(f"native sink exited with status {result}; retain its cursor and spool for recovery")
        finally:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    process.kill(); process.wait()
            # SIGTERM may drain a pending spool and write a newer valid cursor.
            # Back it up even when this run exits through a capacity/error path.
            progress = observe(client, record, directory)
        if progress is None:
            raise VerificationError("native sink completed without a valid cursor; recover from durable progress")
        if stop_block is not None and progress["position"]["block"]["number"] != stop_block - 1:
            raise VerificationError("native sink stopped before its requested final block")
        with cursor.open("rb") as handle:
            os.fsync(handle.fileno())
        atomic_write(directory / "last_completed_cursor.txt", progress["cursor"].encode(), overwrite=True)
        return {**report, "position": progress["position"]}
