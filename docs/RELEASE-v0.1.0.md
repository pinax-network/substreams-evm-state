# v0.1.0 release notes

First BSC prototype release. The four deliverables pass the scoped acceptance
recorded in [QUALIFICATION.md](QUALIFICATION.md). Customer-specific account,
capacity and latency conditions remain unqualified until the missing inputs are supplied.

## What this release delivers

Selective BSC account state through a native Substreams map and ClickHouse,
with complete, verified snapshots for consumers that need a fixed block end.
The ClickHouse path is `map_block_state` → `substreams sink clickhouse`; it does
not require a `DatabaseChanges` or `db_out` transformation.

- One native row contains a block's identity, parent, state root and all selected
  account changes. Storage, balance, nonce and code are resolved independently;
  explicit zero values and empty code are retained as changes.
- Immutable checkpoints become ready only after block/cursor continuity,
  complete nonzero storage, account metadata, bytecode and proofs verify against
  the selected header. A failed or interrupted candidate preserves the existing
  ready snapshot.
- Newly discovered accounts bootstrap in isolated sources, then join existing
  accounts at a common finalized cutover. Historical replay supports private
  compaction between bounded chunks; compacted state remains unverified until
  its final proof passes.
- Portable exports can be verified offline and imported into a new checkpoint
  database. Reader pins preserve a fixed generation across pagination, newer
  publication and retention cleanup.
- Complete-storage verification sorts hashed slots in temporary SQLite storage
  and builds the trie in one ordered scan. The 1,912,703-slot comparison matches
  the original verifier's root while reducing measured reconstruction time and
  memory; see [capacity evidence](CAPACITY.md#full-trie-reconstruction-workspace).
- Native ownership, frozen package/filter identity, durable cursor backups and
  local writer locks prevent accidental source replacement or competing writers.
  Cleanup preserves the native cursor and retained checkpoint continuations.
- Whole-directory capacity monitoring measures shared ClickHouse data, declared
  local runtime/work directories and temporary allocations. Guards reject
  incomplete measurements and stop work when the operating thresholds fail.
- All first-party implementation, command-line tools, qualification workloads
  and tests use Rust. Python and Go tooling and their dependency manifests have
  been removed, and CI enforces that boundary. SQL and protobuf definitions remain.

The granular `map_state_changes` module and PostgreSQL `db_out` path remain
available. The ClickHouse qualification does not establish equivalent behavior
for every PostgreSQL operation; see [POSTGRES.md](POSTGRES.md).

## Persistence and lifecycle behavior

The collector includes persistent failed-transaction gas/sender effects and
pre-execution EIP-7702 changes, while excluding reverted execution changes.
Code clearing is distinct from account deletion. A later authorization in the
same transaction can replace an earlier clear, even when execution reverts.

For BSC SELFDESTRUCT, pre-Cancun deletion and same-transaction creation/deletion
produce a transaction-end storage reset. Post-Cancun SELFDESTRUCT of an existing
account keeps its storage, nonce and code. Later recreation writes survive an
earlier deletion. Captured producer-v3/v4/v5 cases and native/archive comparisons
are linked from [LIFECYCLE.md](LIFECYCLE.md).

## Installation and operating guidance

The release asset is `evm-state-v0.1.0.spkg`. Build it from the release checkout
with `make build`, using the pinned Rust 1.88 toolchain and Substreams 1.22.0 CLI.
The tested ClickHouse version is 26.3.33.24. Build the native Rust tools with
`cargo build --locked --release -p evm-state` and follow the
[README](../README.md) for native setup, proof capture, replay,
checkpoint publication and export/restore commands.

Use finalized blocks only. `ingest` defaults to one-block decoding and a 100 ms
spool idle threshold for finalized follow. `bootstrap-replay` defaults to a
32-block decode batch and a 1,000 ms idle threshold for historical work. Worker
requests are configurable with `--parallel-workers`; the provider controls the
admitted limit. Timing, cache and worker qualifications are separate in
[THROUGHPUT.md](THROUGHPUT.md).

Keep each native database, frozen package, spool and cursor directory together
on durable storage. Restore matching database and runtime state; an older database
backup cannot be paired with a newer cursor. The [capacity guide](CAPACITY.md)
describes measured roots, reserves, incomplete-sample handling and recovery.

## Measured WBNB state

The complete [historical bootstrap](evidence/bsc-wbnb-bootstrap-rust-2026-09-13.json)
and [current-package continuation](evidence/bsc-wbnb-continuation-rust-2026-09-13.json)
pass account, storage, bytecode and encoded-header verification. The source
identities, frozen packages and earlier interrupted capacity phases remain in
those records.

| Operation | Completed measurement |
|---|---|
| Historical checkpoint at block 121466775 | 9,718,240 nonzero slots; full proof/publication in 166 s |
| Continuation through block 121549935 | 83,160 update blocks; 9,739,339 slots; full proof/publication in 132 s |
| Complete current-state read | 974 pages of at most 10,000 slots; page-call p50 154 ms and p95 226 ms; full scan, staging and proof in 404 s |
| Portable export | 974 gzip storage pages; 431,101,963 bytes including metadata and proofs |
| Fresh-database restore | Same block/hash, full storage root and state checksum; 18.6 min including file verification, durable writes, capacity guards and stored-state verification |
| Final replay phase | 13,729,778 new blocks plus saved-suffix recovery; 32.95 GB observed peak including the retained-volume reserve; no rejected samples |

These are shared local-server measurements. Native per-block ingestion and full
proof-verified checkpoint publication have different costs; this release does
not promise a newly proven WBNB snapshot on every BSC block. Page-call latency
excludes client validation, SQLite staging and final trie reconstruction.
Restore's 979 internal capacity checks consumed about 783 seconds of its elapsed
time. The sampled operating cap remained 100 GB, including a separate
1,742,835,712-byte retained-volume reserve and 10 GB of operating headroom.

The supplied customer example also passes complete bootstrap, current-package
continuation, reads and portable restore with 8,156/8,157 slots. The separate
64-account synthetic workload exercises 104,032 slots, 50,000 clears/replacements
and pin-aware retention; it does not represent the missing customer account list.
Cold, cached and finalized-follow throughput and resource-price assumptions are
kept separate in [THROUGHPUT.md](THROUGHPUT.md).

[Portable and retention acceptance](evidence/bsc-wbnb-portable-retention-rust-2026-09-13.json)
also verifies all 9,739,339 slots returned from the restored database. Its ordered
checksum and proof root match the original reader. Releasing the old pin permits
removal of that checkpoint; covered-history cleanup preserves the cursor, and a
second verified export of the newer state has an identical manifest and page
checksums. The 1,000-block recent window selected for this test retains 3,870
blocks at daily-partition granularity. It does not change the CLI default or
establish the customer's retention policy.

All 12 hot-state acceptance phases completed with 535 periodic samples and 1,008
internal guards, without rejections. Their observed peak was 17.48 GB including
the retained-volume reserve, and the final retained sample was 15.40 GB.

## Qualification and limits to retain in the published notes

- BSC mainnet and finalized blocks are the qualified chain/finality path.
  Non-finalized ClickHouse rollback and other chains need separate qualification.
- State and account proofs are cryptographically checked against the encoded
  header. Header finality is trusted to the provider or an operator-pinned hash;
  this tool does not independently verify BSC consensus.
- Locks coordinate one host and durable control directory. They are not a
  distributed multi-host writer protocol. Process-crash tests do not emulate
  physical power loss; the storage must honor acknowledged sync writes.
- System-execution SELFDESTRUCT is unsupported until its execution boundary is
  qualified. Incomplete or unsupported source data must fail readiness.
- Capacity limits are sampled operating guards, not hard filesystem quotas.
  Published measurements must distinguish native source parts, total retained
  data, merge/trie/export peaks, output bytes and billed traffic.
- The full customer 19-account/76-slot and proposed 64-account lists were not
  supplied. Named slots are samples, not complete storage. Do not present public
  or synthetic cohorts as qualification of that missing account set or its SLA.

## Validation and release assets

The suite contains **188 Rust tests**: 93 standalone native, 49 regular ClickHouse,
one separate database-crash, two PostgreSQL and 43 mapper tests. Two internal
subprocess fixtures are excluded from these counts. The native tests exercise
the pinned real Substreams sink, including direct/spooled restart, torn cursor
recovery and a hard restart of a separate disposable ClickHouse container.
Captured lifecycle and independent proof fixtures remain checked in.

The release assets are `evm-state-v0.1.0.spkg` and `SHA256SUMS`. The package is
produced by `make build` from the release checkout; the published release records
the exact commit, green CI run and final package checksum. Downloaded assets are
checked against that checksum and their package metadata before release completion.
Package SHA-256: `6a7a27737de0e4a131d4fd954748593cde4a6c5ed3664ec1e761e2b042bed7b8`.
Historical runs retain their own frozen packages. A README-only package change
changes the `.spkg` file checksum without changing the compiled module hash.
