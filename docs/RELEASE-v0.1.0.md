# v0.1.0 release notes — draft

**Do not publish yet.** Complete the gates below, replace this draft status with
the final qualification results, and build the release asset from the final
tested commit. Current acceptance evidence is in [QUALIFICATION.md](QUALIFICATION.md).

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
- Native ownership, frozen package/filter identity, durable cursor backups and
  local writer locks prevent accidental source replacement or competing writers.
  Cleanup preserves the native cursor and retained checkpoint continuations.
- Whole-directory capacity monitoring measures shared ClickHouse data, declared
  local runtime/work directories and temporary allocations. Guards reject
  incomplete measurements and stop work when the operating thresholds fail.

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
The tested ClickHouse version is 26.3.33.24. Install the Python checkpoint tooling
and follow the [README](../README.md) for native setup, proof capture, replay,
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

## Gates before publishing

- [ ] Finish and prove the complete WBNB and supplied customer-example bootstraps;
  record their source/package identities, exact block/hash, slot counts and roots.
- [ ] Advance complete state through a recent finalized continuation and record
  the current package used for that continuation.
- [ ] Qualify export/restore, publication/read cost and retained-data growth for
  the representative nonempty hot state, including temporary work and rotation.
- [ ] Update the four-deliverable acceptance table with the resulting evidence
  and any remaining customer-specific conditions, without treating narrow
  samples as a broader guarantee.
- [ ] Run the required final checks and verify green CI for the exact release
  commit. Build the `.spkg` with `make build` and record its final SHA-256.
- [ ] Publish GitHub `v0.1.0` from that commit with the `.spkg`, checksum and final
  notes. Download the attached asset and verify its checksum and package metadata.

The test totals and performance/capacity figures in the final notes must come
from the final commit and completed measurements, rather than this draft.
