# Capacity measurement and operating headroom

The default 100 GB target covers retained data and working space. A query of
`system.parts` alone cannot establish that total: merge output, detached parts,
native spool, temporary tries and portable exports also consume disk. The
prototype now provides `capacity-report` and `capacity-run` to measure them.

This implementation supports a local Docker ClickHouse server with persistent
local data disks and a published HTTP port. It verifies that the selected running
container owns the configured HTTP endpoint, rejects container replacement and
remote/object-storage disks, and checks that its data paths reside on persistent
mounts. It does not provision storage or alter quotas.

## Declare the complete data scope

Create an absolute runtime directory and place source run directories, controller
metadata, captured proofs, checkpoint manifests, verification work, exports and
local backups beneath it. Declare additional existing roots if those files live
on other volumes. Files outside declared roots are not measured. Managed commands
reject untracked run/controller/work/export paths while a policy is active.

For example, save this as `<runtime>/capacity.json`, replacing the container and
absolute path with those for the deployment:

```json
{
  "format_version": 1,
  "clickhouse_container": "substreams-evm-state-ch",
  "local_paths": ["/absolute/persistent/evm-state-runtime"],
  "databases": ["new_accounts", "checkpoints"],
  "budget_bytes": 100000000000,
  "headroom_bytes": 10000000000,
  "min_free_bytes": 1073741824
}
```

The budget is decimal bytes. This example stops admitting work at 90 GB measured
usage and also requires at least 1 GiB of unreserved/available filesystem space.
Choose both reserves for the largest expected in-flight write and merge; the
budget reserve does not create physical free space. All roots must exist. Nested
roots and hard links are deduplicated locally; internal symlinks and special
files are rejected instead of silently ignoring their targets.

`databases` selects the table-level breakdown in the report. The measured server
total deliberately includes **the entire ClickHouse data disks**, including other
databases and system tables sharing them. That conservative total covers data,
metadata, detached parts and temporary merge files without pretending the part
catalog includes every file. Local allocated filesystem blocks are added to it.
Shared mounts, reflinks or data also exposed on the client may overcount space;
this report does not subtract unproven shared allocation.

An optional `components` object maps labels such as `spool`, `verification` and
`exports` to lists of absolute paths inside the declared roots. These paths may
be created later by the workload. They provide separate measurements without
being added to the total again; overlapping component labels can overlap in size.

```bash
.venv/bin/evm-state capacity-report --config <runtime>/capacity.json
```

The report includes allocated and logical local bytes, whole server data bytes,
selected active/inactive/detached part sizes, active merge inputs, filesystem free
space and the reasons a new operation would be rejected. Merge input size is a
diagnostic field, not an estimate substituted for actual directory usage.

## Run under the monitor

Set the same ClickHouse HTTP credentials used by the guarded commands, keep
`EVM_STATE_HOME` within a declared root, and give verification commands an explicit
`--work-dir` inside that root. The usual system temporary directory is intentionally
rejected if it is outside the declared scope.

```bash
export EVM_STATE_HOME=<runtime>/control
.venv/bin/evm-state capacity-run --config <runtime>/capacity.json \
  --output <runtime>/measurements/bootstrap-1 --interval 1 -- \
  .venv/bin/evm-state --database new_accounts bootstrap-replay \
    --package spkg/evm-state-v0.1.0.spkg --accounts '<account-list>' \
    --start-block <history-start> --stop-block <target-plus-one> \
    --state-dir <runtime>/new-accounts --checkpoint-database checkpoints
```

The report directory must be new and inside a declared root. The monitor freezes
the policy there and passes it to the child through `EVM_STATE_CAPACITY_CONFIG`.
The native wrapper checks it during ingestion; bootstrap, checkpoint, export and
import commands check it before allocation and before publication. Trie checks
run before temporary files are removed, so short-lived verification allocations
also contribute to the recorded peak. Those checks persist separate guard samples
alongside periodic samples. A native stop records any valid cursor produced during
shutdown, and a retry still validates the cursor against durable database rows.

The monitor checks before launch and during the command, signals its owned process
group on exhausted headroom or an incomplete measurement, and exits nonzero for a
stopped/failed command. It does not delete state or manufacture a ready manifest.
Restore headroom deliberately, inspect retained run/cursor state, then retry into
a new measurement directory. Native cursor damage still uses `recover-cursor`.

Results are written as:

- `config.json`: the frozen scope and thresholds;
- `samples.jsonl`: periodic measurements, including failed samples;
- `guards/*.json`: checks inside the managed operations, tagged by stage;
- `summary.json`: child exit status, stop reasons, sample counts, maximum observed
  sample gap, and the largest allocation seen in either periodic or guard samples.

A missing summary means the monitor did not complete. A healthy supervisor is
required for continuous sampling. Use the guarded project commands for ingestion
and publication; an arbitrary child executable does not implement the internal
policy checks. Do not move output or work outside the declared roots mid-run.

## What a passing run proves

A completed report demonstrates that the sampled workload and publication checks
stayed within the configured operating thresholds. Directory walks and system
queries are observations over an interval, not an atomic filesystem snapshot.
They can miss excursions between samples. A scan racing with file replacement is
retried once from scratch; an incomplete scan is never accepted as zero usage.

The monitor is **not a hard quota or an allocation reservation**. Absolute limits
need filesystem/container storage quotas and sufficient merge/shutdown headroom.
Current state, pinned old checkpoints and backups can continue growing; the guard
stops work when they exhaust the selected reserve. It cannot guarantee that an
unknown customer account set will fit or meet a publication-latency target.

## Reproduce the state/retention stress fixture

With the pinned CLI, Python test dependencies and local ClickHouse running, use a
fresh prefix and a new output path under the declared runtime root:

```bash
.venv/bin/evm-state capacity-run --config <runtime>/capacity.json \
  --output <runtime>/measurements/stress-1 -- \
  .venv/bin/python scripts/qualify_capacity.py --prefix evm_capacity_sample \
    --output <runtime>/stress-1 --accounts 64 --hot-slots 100000 --quiet-slots 64
```

This creates native-generated tables with one large synthetic account and 63
smaller accounts, verifies complete storage, exports and restores it, replaces
half the large account's slots, and rotates checkpoints while testing a pinned
reader. Values are deterministic hashes to avoid unrealistically compressible
all-zero/small-integer data. It preserves its databases and evidence for review.
The result is a SQL, proof and retention stress measurement, not a BSC replay,
customer simulation or server backprocessing benchmark.

For a separate merge-space fixture, use another fresh prefix/output and add
`--merge-only-mib 256`. It stages eight parts with random 4 KiB payloads, enables
merging and runs `OPTIMIZE ... FINAL` on its own auxiliary table. A short sampling
interval such as `--interval 0.1` helps observe the merge. This fixture is limited
to at most 1,024 MiB of generated payload and leaves its data for inspection.
