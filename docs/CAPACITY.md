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

The container must provide GNU `du` and `timeout`, as the tested ClickHouse image
does. Scans have a 25-second deadline inside the container, followed by a
two-second termination grace, within the local Docker client's 30-second limit.
Killing only that client can leave its scan running inside the container; the
inner deadline also terminates the scanner. A timed-out or incomplete scan still
rejects admission, and its sample records the timeout when identified.

For directory bind mounts visible to the Rust process, the optional capacity
setting `"data_scan_mode": "host_bind"` measures allocated host inode blocks
directly. This avoids Docker Desktop's metadata-walk overhead. It requires read
access to every data directory and permission to create a small temporary probe.
The default mode remains `"container"`.

The host mode derives paths from Docker's actual mounts, then creates a random
probe in each data root and reads it through the container before and after the
walk. It also checks that the host directory was not replaced. Probe cleanup
removes only the file inode created by that measurement. Missing or mismatched
probes, unreadable directories, named volumes, file mounts and nested non-bind
mounts fail admission. No guessed host path or stale copy is accepted.

Directory symlinks are counted without following them, and hard links are
deduplicated, as with the container scan. Temporary probe files are included in
the conservative total. The report records the scan mode, verified mappings and
host free-space counters; both host and ClickHouse free-space floors apply.
Keep the mode explicit when comparing measurements. It does not change the byte
budget, headroom reserve or incomplete-sample rejection.

The [WBNB host-bind scan record](evidence/bsc-host-bind-scan-2026-09-12.json)
measured 1.16 and 0.71 seconds for verified host scans around a 9.01-second
container scan. The container's data-byte total fell between the two host
observations as background activity continued. The preceding larger replay
attempt and its scan-timeout stop are retained in that record. Resumption
compacted the saved suffix to 7,220,161 nonzero slots; this private state still
needed its final account-root proof at that measurement stage. The later
[complete Rust checkpoint](evidence/bsc-wbnb-bootstrap-rust-2026-09-13.json)
verified all 9,718,240 nonzero slots at block 121466775. Its final replay phase
completed with no rejected samples and an observed 32,947,503,104-byte peak,
including the separate retained-volume reserve. Earlier interrupted phases
remain failures in that evidence record.

## Storage placement and migration

Compose defaults to the named `ch_data` volume. For a fresh database, create a
persistent directory and set `CH_DATA_SOURCE=./localdata/clickhouse-data` in the
ignored project `.env`, then run `make ch-up`. Use an absolute path for a directory
outside the project. Keep this setting for every subsequent Compose invocation.
On Docker Desktop, a host bind can use a different filesystem from the shared
Docker VM disk. The capacity guard measures the actual selected data disk; free
space on the host does not make a full VM disk writable.

Changing this setting alone does not move an existing database. Stop every
prototype writer and publisher, record database UUIDs and validated native cursor
and compacted-prefix checksums, and gracefully stop ClickHouse. Preserve a complete
copy of its data directory **and matching native/controller/proof/spool directories
from the same stopped state** before changing the mount. Copy into an empty target,
start only ClickHouse, and verify UUIDs, local file hashes, native cursor coverage
and compacted state before resuming writers. Retain the original volume and the
matching recovery files until the new placement is qualified. Reverting only the
database after local cursors have advanced is not a valid rollback: restore the
matching runtime state at its original paths on the original machine as well.

Backups remain part of the budget. Archives under declared local roots are measured
normally. If a retained, immutable old Docker volume is no longer attached to the
measured server, measure it separately and subtract its allocated bytes from the
policy budget. Recalculate that reserve if its contents change. Do not lower the
free-space floor or delete unrelated Docker resources to bypass a capacity stop.
The [recorded local migration](evidence/storage-migration-2026-09-12.json) preserves
both stopped replay positions and distinguishes the old volume reserve from the
archives included in live measurements. Earlier performance measurements used the
previous volume placement and do not qualify host-bind throughput.

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
ClickHouse reads its free and unreserved counters separately; concurrent writes
can briefly make the latter exceed the former. The guard validates both counters
against total capacity and uses their lower value for the free-space floor. A
4 KiB disagreement was reproduced during real replay; it is not corrupt disk
accounting or permission to use the higher reading.
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
target/release/evm-state capacity-report --config <runtime>/capacity.json
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
target/release/evm-state capacity-run --config <runtime>/capacity.json \
  --output <runtime>/measurements/bootstrap-1 --interval 1 -- \
  target/release/evm-state --database new_accounts bootstrap-replay \
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

`evm-state-qualify record-growth` additionally records active and inactive part
bytes for each observed private generation. It checks that the active storage
row count matches the prefix and that exactly one active manifest row exists.
If compaction replaces the pointer during observation, it retries the newer
generation. These per-generation catalog figures exclude native history, other
generations, exports and verification work; the whole-directory capacity sample
remains the operating-budget measurement. A private prefix still needs complete
account-root verification before it can become ready state.

Incomplete internal scans also write rejected guard events with their operation
stage and error type, without a fabricated byte total or third-party error text.
`failed_samples` counts incomplete periodic scans; `failed_guard_samples` counts
incomplete internal scans. The summary includes internal rejection reasons even
when every periodic sample passed. These events cover scans after policy/target
and event-directory validation; failures that prevent initializing the meter or
writing its event still fail the command and require its local error log.

A missing summary means the monitor did not complete. A healthy supervisor is
required for continuous sampling. Use the guarded project commands for ingestion
and publication; an arbitrary child executable does not implement the internal
policy checks. Do not move output or work outside the declared roots mid-run.

## What a passing run proves

A completed report demonstrates that the sampled workload and publication checks
stayed within the configured operating thresholds. Directory walks and system
queries are observations over an interval, not an atomic filesystem snapshot.
They can miss excursions between samples. A Docker directory scan that reports
disappearing files is retried from scratch up to four times, with 0.1, 0.2, 0.4 and
0.8 second delays. Only a complete fresh traversal supplies a byte total. Permission
errors, timeouts and other Docker failures stop immediately. Local file-replacement
races allow at most five complete walks, with 100, 200, 400 and 800 ms backoff.
Only file-not-found errors are retried; permission and other errors fail
immediately. Failed samples identify the measurement stage and, when available,
the operating system error kind/code without exposing paths or error text.
Exhausted retries still reject the measurement; an
incomplete scan is never accepted as zero usage.

The [stage-diagnostic replay record](evidence/bsc-host-scan-retries-2026-09-12.json)
contains a completed five-million-block ingestion/compaction whose capacity
supervisor nevertheless stopped during cleanup: five fresh host walks encountered
missing files. The child exited zero, but the rejected sample still made the
overall result fail. Its private prefix reached 72,735,079 with 7,884,601 slots;
the compaction took 25.8 seconds and used 3.39 GB of query memory. Active parts for
that generation were 573 MB, while the observed whole-scope peak was 20.60 GB
before the 1.74 GB retained-volume reserve. These are different scopes, and none
of these private-state measurements establishes complete account-root verification.
The host walker now measures entries as they are read instead of queuing their
paths until later, shortening the race window with part cleanup. A subsequent
[five-million-block trial](evidence/bsc-streaming-walk-2026-09-12.json) completed
through 77,735,079, including compaction and cleanup: 171 periodic samples and
425 internal checks, with zero rejections. It took 16.73 minutes end to end and
peaked at 20.29 GB, or 22.04 GB including the retained-volume reserve. The new
8,165,197-slot private prefix used 594 MB of active parts; its compaction took
23.84 seconds and 3.41 GB of query memory. This qualifies that interval, while
full-target account verification and hot-state operations remain pending.
Missing-file errors still require a fresh whole walk and exhausted retries still
reject the sample.

The [bootstrap retry record](evidence/bootstrap-capacity-retries-2026-09-12.json)
preserves two stopped runs, their failed periodic samples and admitted resume
observations. Those older samples reported only `RuntimeError`, so their precise
cause cannot be established retrospectively. New Docker failures identify the
operation and whether the directory traversal reported disappearing files,
without storing third-party error bodies. Resumed historical prefixes still
require final account/storage proofs.

Subsequent runs recorded explicit Docker data-directory scan timeouts. Their
[stop/recovery record](evidence/bootstrap-scan-timeouts-2026-09-12.json) also
includes later successful single and four-way concurrent scans. The concurrent
check did not reproduce the timeout, so its root cause remains unresolved.
The timeout and capacity thresholds were retained; both bootstraps resumed only
after fresh admitted measurements, using their original source state.

The monitor is **not a hard quota or an allocation reservation**. Absolute limits
need filesystem/container storage quotas and sufficient merge/shutdown headroom.
Current state, pinned old checkpoints and backups can continue growing; the guard
stops work when they exhaust the selected reserve. It cannot guarantee that an
unknown customer account set will fit or meet a publication-latency target.

## Aggregation memory and temporary spill files

The disk policy does not bound RAM. Bootstrap compaction and checkpoint assembly
group storage by account and slot, so their query memory also needs measurement.
ClickHouse supports [external aggregation](https://clickhouse.com/docs/reference/statements/select/group-by#group-by-in-external-memory)
that spills intermediate groups to disk; the spill threshold is distinct from
the whole query's memory limit. Temporary spill paths must be inside the measured
server data disks. The qualified local server uses `/var/lib/clickhouse/tmp/`.

On ClickHouse 26.3.33.24, the observed defaults include a 0.5 external-aggregation
memory ratio and no explicit per-query memory limit. The second live million-block
WBNB compaction exercised that default spill path: 1,907,462 retained nonzero slots,
3,461,513,972 bytes of reported query memory, 30 spill parts and 15.300 seconds.
Its 281,410,897 compressed spill bytes are cumulative writes, not a temporary
allocation peak. The private prefix remains unverified against an account root.

A separate [same-state comparison](evidence/bsc-aggregation-memory-2026-09-12.json)
copied that checksummed prefix and 10,000 contiguous native blocks into a fresh
database. Both variants produced exactly 1,912,703 nonzero slots and the same
ordered state/account-field checksum:

| Query settings | Server query duration | Reported query memory |
|---|---|---|
| Observed aggregation/memory defaults | 3.932 s | 1,878,636,028 bytes |
| 256 MiB aggregation spill, 128 MiB sort spill, 2 GiB query memory limit; ratio thresholds disabled | 4.950 s | 948,184,379 bytes |

Both comparison queries also have a 120-second execution limit and 10 GiB
temporary-data limit. The explicit spill variant wrote 162 aggregation parts;
the default comparison did not spill. These are sequential queries on shared
infrastructure, not a universal memory bound or performance guarantee. Query-log
duration includes completion; client HTTP response timing is recorded separately.
The settings apply only to the measurement queries. The running bootstraps keep
their existing defaults, whose spill path was observed independently above.

The completed comparison has 13 periodic and five guard samples, with no rejected
samples. Its largest shared-server/local allocation is 10,417,532,928 bytes,
plus the separate 1,742,835,712-byte original-volume reserve. The maximum sample
gap is 22.57 seconds, so this does not bound short-lived spill allocation. Retained
comparison databases and concurrent bootstraps are included in the total.

To repeat against an owned source that has a private prefix and at least 10,000
durable subsequent blocks, select a fresh comparison database and workload path
inside the declared roots:

```bash
target/release/evm-state --database <fresh-comparison-db> capacity-run \
  --config <runtime>/capacity.json --output <runtime>/aggregation-capacity -- \
  target/release/evm-state-qualify aggregation \
    --state-dir <runtime>/source/native --database <fresh-comparison-db> \
    --output <runtime>/aggregation-work --delta-blocks 10000
```

The qualifier briefly holds the source's cleanup lock to copy a fixed prefix/suffix,
then verifies the isolated copy and compares complete ordered results. Insufficient
durable suffix data fails the attempt. It preserves all comparison data and never
publishes a checkpoint. Query-log completion is awaited because it can become
visible after the HTTP response. The evidence retains the earlier boundary,
measurement-syntax and logging-race failures as unsuccessful attempts.

## Full trie reconstruction workspace

The [real WBNB trie workspace measurement](evidence/bsc-trie-workspace-2026-09-12.json)
uses the same isolated 1,912,703-slot result from the aggregation comparison.
Before reconstruction, its storage and archive-supplied nonce/balance/code fields
reproduce the previously recorded state checksum. The complete stream also
reproduces that checksum while the original incremental `storage_root` and
`TrieDB` code construct the trie. This is the baseline for the sorted builder
now used by checkpoint, export and restore verification.

| Measurement | Observed result |
|---|---|
| Nonzero slots / stored trie nodes | 1,912,703 / 2,660,563 |
| Trie reconstruction wall time, including progress guards | 1,280.79 s (21.35 min) |
| Python process CPU time during reconstruction | 986.43 s |
| Whole measurement process peak resident memory | 578,240,512 bytes |
| Trie file length / allocated space before close | 388,513,792 / 403,693,568 bytes |
| Shared-server and declared-local-root allocated peak | 9,374,461,952 bytes |

The separate original-volume reserve is 1,742,835,712 bytes. All 116 periodic and
21 guard samples pass; the maximum periodic gap is 29.74 seconds. Progress guards
run every 100,000 slots and before closing the temporary SQLite transaction.
RSS covers the Python process, including its preliminary checksum scan, rather
than all processes on the machine. The disposable SQLite file is measured before
close; it is not a portable trie export or a retained verified checkpoint.

The reconstructed root is
`0xd672105a8c3dfc77d33324fa8d8881000bacc3e83de76c5cceced2b7edab99ca`
at block 13,120,981. **This is not an accepted account storage root:** the RPC
provider rejected `eth_getProof` because the historical block is outside its proof
window. This exercise establishes measured reconstruction cost and input parity,
not complete account-state acceptance. The long replays retain their separately
captured recent proofs as final readiness targets.

The [sorted reconstruction comparison](evidence/bsc-trie-sorted-2026-09-12.json)
repeats the complete input and matches that root and checksum. It first stages
`keccak256(slot)` and RLP-encoded nonzero values in a fresh SQLite table, then
hashes completed subtrees in key order. Duplicate keys fail even when their
values agree. The builder keeps two lookahead entries and completed child
references instead of retaining a full trie and its node reference counts.
Account-proof verification and comparison with the proven storage root remain
unchanged. The original incremental builder remains an independent test oracle.

| Same 1,912,703-slot input | Incremental baseline | Sorted builder |
|---|---:|---:|
| Wall time, including progress guards | 1,280.79 s | 200.63 s |
| Python CPU time during reconstruction | 986.43 s | 84.61 s |
| Whole measurement process peak resident memory | 578,240,512 bytes | 73,596,928 bytes |
| Allocated workspace before close | 403,693,568 bytes | 135,061,504 bytes |

The observed wall-time improvement is 6.38 times and CPU improvement is 11.66
times. Both runs use guards every 100,000 slots, but their guard durations and
shared-host load differ. The sorted run passes all 20 periodic and 21 guard
samples; its maximum periodic gap is 22.13 seconds and shared allocated peak is
8,887,144,448 bytes, plus the separate original-volume reserve. The sorting file
is committed before hashing and measurement; its entries are **hashed slots,
not trie nodes**. It remains disposable workspace, not a checkpoint export.
SQLite requests a 16 MiB page cache; this is not a hard process-memory bound.

This reduces measured reconstruction cost. Larger state, complete hot-account
proofs, checkpoint/export latency and sustained growth remain qualification
gates. Do not extrapolate a fixed per-slot memory or time bound from these
shared-machine measurements.

To reproduce on the retained isolated comparison database, provide a JSON object
mapping its account to the matching observed fields (`nonce`, decimal-string
`balance`, `code_hash` and `code`). The Rust qualifier rejects metadata or storage that
does not reproduce the comparison's recorded checksum:

```bash
target/release/evm-state --database <comparison-db> capacity-run \
  --config <runtime>/capacity.json --output <runtime>/trie-capacity --interval 5 -- \
  target/release/evm-state-qualify trie-workspace \
    --evidence docs/evidence/bsc-aggregation-memory-2026-09-12.json \
    --fields <runtime>/matching-fields.json --output <runtime>/trie-work
```

Both output directories must be new and inside the measured roots. The evidence
selects the frozen database and generation; the workload never reads or modifies
the advancing native source and never publishes a ready manifest.
To compare with a retained prior measurement, use fresh output directories and add
`--reference <previous-trie-work>/result.json`. The Rust qualifier
requires the same source identity, header, input count/checksum and reconstructed
root. Unit tests compare both assemblers on seeded random keys, deep shared
paths, full branches and the 31/32/33-byte child-reference boundary; malformed,
duplicate, zero and interrupted input cannot return an accepted root.

## Reproduce the state/retention stress fixture

With the native Rust tools, pinned CLI and local ClickHouse running, use a
fresh prefix and a new output path under the declared runtime root:

```bash
# Set NATIVE_DSN_TEMPLATE to the local native DSN containing {database}.
target/release/evm-state capacity-run --config <runtime>/capacity.json \
  --output <runtime>/measurements/stress-1 -- \
  target/release/evm-state-qualify capacity-stress --prefix evm_capacity_sample \
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
