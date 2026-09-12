"""Measure whole data directories and supervise an explicitly bounded operation.

Sampling is an operating guard, not a filesystem quota. Reports distinguish
measured peaks from unsampled intervals and include the entire ClickHouse data
disks, even when other databases share them. No source data is deleted here.
"""
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import signal
import stat
import subprocess
import time
import urllib.parse
import uuid

from .ch import ClickHouse, identifier
from .files import atomic_json
from .proof import VerificationError


def _integer(value, name, minimum=0):
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ValueError(f"{name} must be an integer >= {minimum}")
    return value


def _roots(paths):
    unique = sorted(set(paths), key=lambda p: (len(p.parts), str(p)))
    result = []
    for path in unique:
        if not any(path == parent or parent in path.parents for parent in result):
            result.append(path)
    return result


def local_usage(paths):
    """Count allocated blocks, deduplicating nested roots and hard-linked files."""
    seen, logical, allocated, files = set(), 0, 0, 0
    for root in _roots([Path(p).resolve(strict=True) for p in paths]):
        pending = [root]
        while pending:
            path = pending.pop()
            info = path.lstat()
            if stat.S_ISLNK(info.st_mode):
                raise VerificationError("capacity roots contain a symlink; declare its target as a separate root")
            key = (info.st_dev, info.st_ino)
            if key in seen:
                continue
            seen.add(key)
            allocated += info.st_blocks * 512
            if stat.S_ISDIR(info.st_mode):
                with os.scandir(path) as entries:
                    pending.extend(Path(entry.path) for entry in entries)
            elif stat.S_ISREG(info.st_mode):
                logical += info.st_size
                files += 1
            else:
                raise VerificationError("capacity roots contain an unsupported special file")
    return {"allocated_bytes": allocated, "logical_bytes": logical, "files": files}


class DockerCapacityError(RuntimeError):
    def __init__(self, operation, timed_out=False, directory_changed=False):
        self.operation = operation
        self.directory_changed = directory_changed
        outcome = "timed out" if timed_out else "failed"
        super().__init__(f"Docker capacity {operation} {outcome}; the sample is incomplete")


def _docker(arguments, timeout=30):
    operation = "container inspection" if arguments[0] == "inspect" else "data-directory scan"
    try:
        result = subprocess.run(["docker", *arguments], capture_output=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        raise DockerCapacityError(operation, timed_out=True) from None
    if result.returncode:
        # Do not echo inspect environment values or third-party command errors.
        changed = (operation == "data-directory scan" and result.returncode == 1 and
                   b"No such file or directory" in result.stderr and b"Permission denied" not in result.stderr)
        raise DockerCapacityError(operation, directory_changed=changed)
    return result.stdout


class Meter:
    def __init__(self, client, config):
        if not isinstance(config, dict) or config.get("format_version") != 1:
            raise ValueError("unsupported capacity configuration")
        allowed = {"format_version", "clickhouse_container", "local_paths", "databases", "components",
                   "budget_bytes", "headroom_bytes", "min_free_bytes"}
        if set(config) - allowed:
            raise ValueError("unknown capacity configuration option")
        # A monitored bootstrap may not have created its destination yet.
        self.client = ClickHouse("default", client.url, client.user, client.password)
        self.config = dict(config)
        self.budget = _integer(config["budget_bytes"], "budget_bytes", 1)
        self.headroom = _integer(config["headroom_bytes"], "headroom_bytes", 1)
        self.min_free = _integer(config["min_free_bytes"], "min_free_bytes")
        if self.headroom >= self.budget:
            raise ValueError("headroom must be smaller than the capacity budget")
        self.container = config["clickhouse_container"]
        if not isinstance(self.container, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", self.container):
            raise ValueError("invalid ClickHouse container name or ID")
        if not isinstance(config["local_paths"], list) or not config["local_paths"]:
            raise ValueError("declare the local run, control, verification and export directories")
        if any(not Path(path).is_absolute() for path in config["local_paths"]):
            raise ValueError("capacity local paths must be absolute")
        self.paths = _roots([Path(path).resolve(strict=True) for path in config["local_paths"]])
        if any(not path.is_dir() for path in self.paths):
            raise ValueError("capacity local paths must be existing directories")
        databases = config.get("databases", [client.database])
        if not isinstance(databases, list) or not databases:
            raise ValueError("capacity databases must be a nonempty list")
        self.databases = sorted({identifier(name) for name in databases})
        self.components = {}
        declared_components = config.get("components", {})
        if not isinstance(declared_components, dict):
            raise ValueError("capacity components must be an object")
        for name, paths in declared_components.items():
            if not re.fullmatch(r"[a-z][a-z0-9_-]{0,30}", name) or not isinstance(paths, list):
                raise ValueError("invalid capacity component declaration")
            values = []
            for value in paths:
                path = Path(value)
                if not path.is_absolute():
                    raise ValueError("capacity component paths must be absolute")
                path = path.resolve()
                if not any(path == root or root in path.parents for root in self.paths):
                    raise ValueError("capacity component is outside declared local roots")
                values.append(path)
            self.components[name] = values
        self.container_id = None
        self.inspect()

    def inspect(self):
        info = json.loads(_docker(["inspect", self.container]))[0]
        if self.container_id is not None and info["Id"] != self.container_id:
            raise VerificationError("ClickHouse capacity container was replaced")
        url = urllib.parse.urlsplit(self.client.url)
        if url.scheme != "http" or url.hostname not in {"localhost", "127.0.0.1"} or url.username or url.password:
            raise VerificationError("Docker capacity measurement requires a local published HTTP endpoint")
        port = str(url.port or 80)
        bindings = info["NetworkSettings"]["Ports"].get("8123/tcp") or []
        if (not info["State"]["Running"] or not any(binding["HostPort"] == port and
                binding["HostIp"] in {"127.0.0.1", "0.0.0.0", ""} for binding in bindings)):
            raise VerificationError("capacity container does not own the configured ClickHouse HTTP endpoint")
        self.container_id = info["Id"]
        return info

    def sample(self):
        started = time.time_ns()
        info = self.inspect()
        disks = list(self.client.rows("SELECT name,path,type,is_remote,total_space,free_space,unreserved_space "
                                      "FROM system.disks ORDER BY name"))
        if not disks or any(disk["type"] != "Local" or disk["is_remote"] for disk in disks):
            raise VerificationError("complete capacity measurement requires local ClickHouse data disks")
        if any(not int(disk["total_space"]) or any(not 0 <= int(disk[field]) <= int(disk["total_space"])
               for field in ["free_space", "unreserved_space"]) for disk in disks):
            raise VerificationError("invalid ClickHouse disk capacity counters")
        roots = _roots([PurePosixPath(disk["path"]) for disk in disks])
        mounts = [PurePosixPath(mount["Destination"]) for mount in info["Mounts"]
                  if mount["Type"] in {"bind", "volume"}]
        if any(not root.is_absolute() or ".." in root.parts or not any(
                root == mount or mount in root.parents for mount in mounts) for root in roots):
            raise VerificationError("ClickHouse data disks must be on declared persistent container mounts")
        # Whole data directories include active/inactive/detached parts, merge
        # temporary files, schema metadata and system tables. GNU du deduplicates
        # hard links across all roots in this one invocation.
        for attempt in range(5):
            try:
                output = _docker(["exec", self.container_id, "du", "-s", "-c", "-B1", "--null", "--",
                                  *map(str, roots)])
                break
            except DockerCapacityError as error:
                # Concurrent merges can invalidate more than one traversal.
                # Retry only disappearing files, with bounded backoff and a
                # fresh whole scan. No failed scan's partial total is accepted.
                if not error.directory_changed or attempt == 4:
                    raise
                time.sleep(0.1 * 2**attempt)
        total = output.rstrip(b"\0").split(b"\0")[-1].split(b"\t", 1)
        if len(total) != 2 or total[1] != b"total":
            raise VerificationError("invalid ClickHouse data-directory measurement")
        server_bytes = int(total[0])
        for attempt in range(2):
            try:
                local = local_usage(self.paths)
                break
            except FileNotFoundError:
                # Synced atomic metadata replacement can race with traversal.
                if attempt: raise
        used = server_bytes + local["allocated_bytes"]
        components = {}
        for name, paths in self.components.items():
            for attempt in range(2):
                try:
                    components[name] = local_usage([path for path in paths if path.exists()])
                    break
                except FileNotFoundError:
                    if attempt: raise
        # Reporting groups never substitute for the full directory measurement.
        parts = [row for row in self.client.rows("SELECT database,active,sum(bytes_on_disk) AS bytes "
                  "FROM system.parts GROUP BY database,active ORDER BY database,active") if row["database"] in self.databases]
        detached = [row for row in self.client.rows("SELECT database,sum(bytes_on_disk) AS bytes FROM system.detached_parts "
                     "GROUP BY database ORDER BY database") if row["database"] in self.databases]
        merges = [row for row in self.client.rows("SELECT database,total_size_bytes_compressed AS input_bytes "
                   "FROM system.merges") if row["database"] in self.databases]
        # ClickHouse reads free and unreserved counters separately. Concurrent
        # writes/merges can make unreserved briefly exceed the earlier free
        # reading (observed by 4096 bytes during real replay). Both must be
        # valid counters; use their lower value instead of assuming atomicity.
        available = min(min(int(disk["free_space"]), int(disk["unreserved_space"])) for disk in disks)
        local_disks = []
        for path in self.paths:
            fs = os.statvfs(path)
            free = fs.f_bavail * fs.f_frsize
            available = min(available, free)
            local_disks.append({"path": str(path), "available_bytes": free})
        reasons = []
        if used + self.headroom >= self.budget:
            reasons.append("budget_headroom_exhausted")
        if available < self.min_free:
            reasons.append("filesystem_free_space_below_floor")
        return {"format_version": 1, "sample_started_ns": started, "sample_finished_ns": time.time_ns(),
            "container_id": self.container_id, "server_data_roots": list(map(str, roots)),
            "server_data_allocated_bytes": server_bytes, "local_roots": list(map(str, self.paths)),
            "local": local, "local_components": components, "accounted_allocated_bytes": used, "budget_bytes": self.budget,
            "headroom_bytes": self.headroom, "available_above_headroom_bytes": self.budget - self.headroom - used,
            "min_free_bytes": self.min_free, "server_disks": disks, "local_filesystems": local_disks,
            "selected_database_parts": parts, "selected_database_detached_parts": detached,
            "selected_database_merges": merges, "admitted": not reasons, "reasons": reasons,
            "coverage": "entire local ClickHouse data disks plus declared local roots; shared data may overcount",
            "limit_kind": "sampled operating guard, not a filesystem quota"}


def check(client, required_paths=(), stage="operation"):
    """Check the frozen policy before a managed operation allocates or publishes."""
    policy = os.environ.get("EVM_STATE_CAPACITY_CONFIG")
    if not policy:
        return None
    meter = Meter(client, json.loads(Path(policy).read_text()))
    for path in required_paths:
        resolved = Path(path).resolve()
        if not any(resolved == root or root in resolved.parents for root in meter.paths):
            raise VerificationError("operation uses a directory outside the declared capacity roots")
    events = os.environ.get("EVM_STATE_CAPACITY_EVENTS")
    destination = None
    if events:
        destination = Path(events).resolve()
        if not any(destination == root or root in destination.parents for root in meter.paths):
            raise VerificationError("capacity events directory is outside declared roots")
    started = time.time_ns()
    try:
        measured = meter.sample()
    except (OSError, RuntimeError, ValueError, KeyError, subprocess.TimeoutExpired) as error:
        if destination is not None:
            # Keep a rejected event even when the scan has no trustworthy byte
            # total. Do not record third-party errors, paths or credentials.
            failure = {
                "format_version": 1, "sample_started_ns": started, "sample_finished_ns": time.time_ns(),
                "stage": stage, "admitted": False, "reasons": ["incomplete_capacity_sample"],
                "error_type": type(error).__name__}
            if isinstance(error, DockerCapacityError):
                failure["inspection_operation"] = error.operation
                failure["directory_changed"] = error.directory_changed
            atomic_json(destination / (uuid.uuid4().hex + ".json"), failure)
        raise
    if destination is not None:
        atomic_json(destination / (uuid.uuid4().hex + ".json"), {**measured, "stage": stage})
    if not measured["admitted"]:
        raise VerificationError("capacity guard rejected operation: " + ", ".join(measured["reasons"]))
    return measured


def supervise(meter, command, output, interval=1.0):
    """Own the child process group; stop the whole run on an unsafe/missing sample."""
    if not command:
        raise ValueError("capacity-run requires a command after --")
    if isinstance(interval, bool) or not isinstance(interval, (int, float)) or not 0.1 <= interval <= 60:
        raise ValueError("sample interval must be between 0.1 and 60 seconds")
    output = Path(output).resolve()
    if not any(output == root or root in output.parents for root in meter.paths):
        raise ValueError("capacity report directory must be inside a declared local root")
    output.mkdir(parents=True, exist_ok=False)
    atomic_json(output / "config.json", meter.config)
    (output / "guards").mkdir()
    started = time.time_ns()
    samples, failures, peak, max_gap, last_sample = 0, 0, 0, 0, None
    process, result, reason, termination_error = None, None, None, None
    config_hash = hashlib.sha256(json.dumps(meter.config, sort_keys=True).encode()).hexdigest()
    with (output / "samples.jsonl").open("x") as log:
        def record():
            nonlocal samples, failures, peak, max_gap, last_sample
            try:
                value = meter.sample()
                samples += 1
                peak = max(peak, value["accounted_allocated_bytes"])
                if last_sample is not None:
                    max_gap = max(max_gap, value["sample_finished_ns"] - last_sample)
                last_sample = value["sample_finished_ns"]
            except (OSError, RuntimeError, ValueError, KeyError, subprocess.TimeoutExpired) as error:
                failures += 1
                value = {"sample_finished_ns": time.time_ns(), "admitted": False,
                         "reasons": ["incomplete_capacity_sample"], "error_type": type(error).__name__}
                if isinstance(error, DockerCapacityError):
                    value["inspection_operation"] = error.operation
                    value["directory_changed"] = error.directory_changed
                if isinstance(error, VerificationError):
                    # Meter verification errors are our fixed diagnostic text;
                    # third-party/HTTP/Docker error bodies remain excluded.
                    value["error_detail"] = str(error)
            log.write(json.dumps(value, sort_keys=True) + "\n")
            log.flush()
            os.fsync(log.fileno())
            return value
        try:
            first = record()
            if not first["admitted"]:
                reason = first["reasons"]
            else:
                process = subprocess.Popen(command, start_new_session=True,
                    env=dict(os.environ, EVM_STATE_CAPACITY_CONFIG=str(output / "config.json"),
                             EVM_STATE_CAPACITY_EVENTS=str(output / "guards")))
                while True:
                    try:
                        result = process.wait(timeout=interval)
                    except subprocess.TimeoutExpired:
                        pass
                    current = record()
                    if not current["admitted"]:
                        reason = current["reasons"]
                        break
                    if result is not None:
                        break
        finally:
            if process is not None and (process.poll() is None or reason):
                # A native sink may outlive its wrapper. Signal the owned process
                # group even if its parent already exited, then give it time to
                # flush/stop before SIGKILL. Never release another run's lock.
                try:
                    os.killpg(process.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
                except PermissionError:
                    termination_error = "could_not_confirm_process_group_termination"
                try:
                    process.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    pass
                # Reaping the wrapper alone does not prove its children stopped.
                # Ensure a child that ignored SIGTERM cannot continue allocating.
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                except PermissionError:
                    # Do not claim complete termination if the OS cannot confirm
                    # it (macOS can also return EPERM for zombie process groups).
                    termination_error = "could_not_confirm_process_group_termination"
                process.wait(timeout=10)
                result = process.returncode
    guards = [json.loads(path.read_text()) for path in (output / "guards").glob("*.json")]
    peak = max([peak, *[guard["accounted_allocated_bytes"] for guard in guards
                       if "accounted_allocated_bytes" in guard]])
    rejected = [guard["stage"] for guard in guards if not guard["admitted"]]
    guard_failures = sum("incomplete_capacity_sample" in guard["reasons"] for guard in guards)
    stop_reasons = sorted(set(reason or []) | {cause for guard in guards if not guard["admitted"]
                                              for cause in guard["reasons"]})
    report = {"format_version": 1, "started_ns": started, "finished_ns": time.time_ns(),
        "config_sha256": config_hash, "config": meter.config, "samples": samples, "failed_samples": failures,
        "peak_observed_allocated_bytes": peak, "maximum_sample_gap_ns": max_gap,
        "guard_samples": len(guards), "failed_guard_samples": guard_failures, "rejected_guard_stages": rejected,
        "command_exit_code": result, "stop_reasons": stop_reasons, "termination_error": termination_error,
        "status": "completed" if result == 0 and not reason and not rejected and not termination_error else "stopped",
        "limit_kind": "sampled operating guard; excursions between samples are not bounded by this report"}
    atomic_json(output / "summary.json", report)
    return report
