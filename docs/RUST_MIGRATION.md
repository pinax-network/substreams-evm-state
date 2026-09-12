# Rust migration acceptance

The implementation, command-line tools, qualification workloads and tests will
use Rust. SQL schemas, protobuf definitions, captured binary/JSON fixtures and
documentation remain data and interface definitions. The external Substreams CLI
remains the native sink; its upstream implementation language does not add a
second implementation language to this repository.

The Cargo workspace keeps the existing WASM package isolated from the native
`evm-state` crate. Default builds still select the WASM package; native builds
select `-p evm-state`, and final test/CI commands will cover the workspace.

The migration must preserve the existing wire, SQL, cursor, controller, proof,
checkpoint and export formats. Running backfills keep their frozen package,
source identity, cursor and controller until the Rust replacement has passed
compatibility and recovery checks. Their historical replay evidence remains
useful, but Python-only acceptance is not evidence for the Rust release.

| Area | Required Rust evidence |
|---|---|
| Account/storage/header proofs | Existing inclusion, non-inclusion, malformed proof/header, metadata, complete-storage and real captured fixtures; full retained WBNB root parity |
| Files, host identity, cursors and ownership | Atomic writes, host-rebinding compatibility, public opaque cursor vectors, database/header/cursor binding and writer exclusion |
| Native ingestion and bootstrap | Same pinned native sink, finality, worker options, local spool recovery, gap/filter detection, private compaction and resumable cleanup |
| Publication and readers | Immutable ready manifests, full proofs before readiness, failed publisher preservation, pins, fixed pagination and complete returned-state verification |
| Export/import and retention | Portable/offline proof verification, corruption rejection, restored database continuity, preserved readers and account-aware rotation |
| Capacity and qualification | Whole-directory sampled guards, transient inspection handling, process-group stop, measured trie/export/merge peaks and reproducible workloads |
| PostgreSQL fallback | Coherent SQL snapshots, field/slot diagnostics and full-root verification retained in Rust |
| Test transport and fixtures | Rust fault-injection gRPC server supports the pinned sink's S2 requests; remove the Go proxy and Go fixture generator |
| Tooling and release | Rust installer/qualification commands, Cargo CI, no first-party Python/Go sources or Python dependency manifests, final `make build`, v0.1.0 package and detailed notes |

The previous green baseline is commit `587f9a0`: 329 Python and 43 Rust tests.
Nine additional read-qualification scenarios were checked locally before this
migration request. Preserve their assertions in Rust rather than adding another
Python release dependency. The migration is an additional v0.1.0 gate, alongside
the complete hot-account proofs and qualification in [RELEASE-v0.1.0.md](RELEASE-v0.1.0.md).

The initial Rust implementation covers disk-sorted storage tries, account proofs,
BSC header hashing, public native cursor decoding and durable progress, atomic
metadata and OS identity, database/controller ownership, source validation,
checkpoint reads, durable pins, pagination, generation retention, RPC proof
capture, capacity sampling/supervision, and the private-prefix growth recorder.
The native CLI exposes reads, pins, retention, capacity, proof capture, native
preparation/ingestion/cursor recovery and checkpoint publication. It validates
existing private bootstrap prefixes and supports portable export/import with
offline proof verification. Bounded bootstrap replay, private compaction, source
history retention and complete returned-page verification are implemented in Rust.
The native wrapper has not yet passed the
full fault-injection streaming qualification needed to take over the long replays.

The first checks passed locally on Rust 1.88: 29 native unit/integration scenarios
without ClickHouse, plus 9 isolated ClickHouse reader/retention/progress scenarios.
The subprocess fixture is excluded from that count. The Rust CLI also read the
existing `ce69d62957a945a6971d8aa736dfb4c8` BSC checkpoint at block 121462462
through its original controller binding. That checks compatibility of the read
path; complete returned-state qualification must still run in Rust.

CI runs `cargo test --locked --workspace` and the opt-in Rust ClickHouse suite in
addition to the existing migration baseline. Python and the Go transport helper
remain temporarily until all replacement workflows have their required evidence.

The next Rust checks cover native preparation and source ownership, explicit
cursor recovery, finalized flush/worker arguments, full checkpoint publication,
storage clears, bad metadata, missing blocks, filter drift, unexpected accounts,
and lifecycle resets against a previous ready checkpoint. That phase passed 34
standalone Rust test functions and 18 opt-in ClickHouse test functions, alongside
the existing 43 mapper tests. Several tests run multiple corruption scenarios.
HTTP tests also reject failed queries/inserts returned with HTTP 200, consistent
with ClickHouse's [HTTP response caveats](https://clickhouse.com/docs/concepts/features/interfaces/http#http-response-codes-caveats).

Portable export/import adds offline corruption checks and ClickHouse round trips,
including preserving an existing ready checkpoint after a failed restore. The
current totals are 40 standalone native Rust tests, 22 opt-in ClickHouse tests,
and 43 mapper tests. A real three-account BSC checkpoint created by the previous
implementation was exported into five gzip pages and restored into a new database
by Rust. All 46 nonzero slots, the header and state checksum matched; both runs
completed under the Rust capacity supervisor without rejected samples. See
[portable compatibility evidence](evidence/bsc-portable-rust-2026-09-12.json).

The Rust trie implementation also reconstructed the exact frozen WBNB prefix root
from 1,912,703 slots. It used 19.4 CPU seconds and 32.0 MB peak process memory;
the trie stage took 163.0 seconds including capacity checks. The prior sorted
implementation used 84.6 CPU seconds and 73.6 MB on the same input. This is prefix
parity, not proof acceptance of the final hot-account checkpoint. See
[trie evidence](evidence/bsc-trie-rust-2026-09-12.json).

Bootstrap/history tests use independent account proofs and encoded headers with
separate day/month partitions. They check repeated compaction, zero clears,
lifecycle resets, damaged prefixes, orphan candidates, partial partition cleanup,
retained checkpoint continuation, cursor binding and ownership locks. Private
prefixes remain unverified until complete checkpoint proof acceptance.

The Rust returned-page qualifier checks every account field and storage slot
against the captured proof, including absent accounts. Its negative scenarios
preserve the additional pre-migration read checks: a repeated wrong result cannot
pass using only a stable digest or matching count. Capacity failure and pin-release
failure also prevent publishing measurement output.

A CI lock failure exposed a concurrent fork retaining a copied descriptor between
fork and exec. A deterministic regression reproduced the failure before the fix.
Normal lock destruction now explicitly unlocks; a killed wrapper still leaves the
native child's inherited descriptor locked. Both paths have subprocess coverage.
The pure native test count is now 49; the ClickHouse suite has 30 test functions,
alongside 43 mapper tests. Test helpers run only as subprocess fixtures and are
excluded from these totals.

Host recovery now has Rust coverage for original-state preservation, idempotency,
partial sidecar recovery, writer exclusion and rejection of mismatched machines,
packages, prefixes and controllers. The pinned CLI installer also runs in Rust;
archive checksums, gzip integrity and layout are validated before atomically
replacing the executable. Their Python script entry points have been removed.
These add four standalone tests and two ClickHouse test functions (53 native,
32 ClickHouse and 43 mapper tests). Full native streaming fault injection,
and the remaining operational workloads still need Rust parity.

The real restored BSC checkpoint also passed complete returned-page verification
in Rust: two scans of 46 slots, ten page calls, p50 44.0 ms and p95/max 67.1 ms.
The capacity run admitted five periodic samples and four internal guards without
rejection. See [read evidence](evidence/bsc-read-rust-2026-09-12.json). This was a
shared-server run, not a comparison of language overhead or a hot-account SLA.

PostgreSQL diagnostics now use Rust for coherent SQL snapshots, account and full
storage proofs, sampled RPC checks and metadata/provenance validation. The two
Python verification script entry points are removed. Six standalone tests retain
their corruption cases, and two PostgreSQL integration tests exercise real
queries and concurrent writer consistency in isolated schemas. The native retry
CLI also retains `--max-retries -1`; a parser regression test checks this before
any ingestion starts. This brings the standalone native total to 60; CI includes
32 ClickHouse and two PostgreSQL tests alongside the 43 mapper tests.

The Rust loopback gRPC fixture now decodes the pinned sink's compressed S2
requests directly, using the package's protobuf descriptors. It retains only
the worker header under test, not provider credentials. A real sink run verified
that requests used compressed frames, preserved all block rows, and produced a
fully proven checkpoint. Chunked bootstrap resumed private prefixes with changed
worker requests. Forced SIGKILL recovery passed in both follow and spooled modes;
a data write followed by cursor-file failure replayed from the previous durable
cursor and still produced the exact complete state.

Transport tests cover compression/frame corruption and nested-array alignment.
The Go cursor generator is removed; Rust reproduces its unchanged independent
vectors byte for byte. The old Go adapter and Python server remain only for the
remaining migration baseline scenarios until those assertions have Rust coverage.
This phase passes 63 standalone native tests, 36 ClickHouse tests and the 43
mapper tests locally; the two PostgreSQL tests passed in the preceding CI run.

Native output measurement, timed throughput, evidence summaries and trie
qualification now have Rust entry points; their four Python scripts and the old
throughput tests are removed. Rust reproduces the retained 10,000-block BSC
sample's 45,533,529 protobuf bytes and ordered checksum exactly, and the complete
retained throughput summary has matching run, live-window and read results;
see the [Rust output evidence](evidence/bsc-output-rust-2026-09-12.json).
The summary additionally binds periodic capacity samples and cache-design inputs
by checksum and rejects raw lag failures hidden by a summary counter.

The timed runner keeps its child within the capacity supervisor's process group.
A real native test verifies timeout, durable progress, child reaping and a
successful restart, plus the complete timed collection path. Proof capture also
requires the RPC header to match an explicitly requested block number. This phase
passes 73 standalone native tests, 37 ClickHouse tests and 43 mapper tests.

Lifecycle diagnostics and captured-evidence tests now run in Rust. All 26
producer fixtures retain their file/header/bytecode provenance checks. The Rust
diagnostics reproduce the previous observed-field proof checks, authorization
clear/reinstall comparisons and surviving SELFDESTRUCT comparison exactly. A
fresh 145-block recreation replay through the Rust wrapper and rebuilt package
also matches all five captured comparisons, including the storage reset. See
[lifecycle parity evidence](evidence/bsc-lifecycle-rust-2026-09-12.json).
Their four Python scripts and old evidence tests are removed. Six Rust test
functions preserve the existing assertions and add corruption cases, bringing
the standalone native count to 79.
