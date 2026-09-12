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
existing private bootstrap prefixes; compaction/replay and portable export/import
still need their Rust replacements. The native wrapper has not yet passed the
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
and lifecycle resets against a previous ready checkpoint. There are now 34
standalone Rust test functions and 18 opt-in ClickHouse test functions, alongside
the existing 43 mapper tests. Several tests run multiple corruption scenarios.
HTTP tests also reject failed queries/inserts returned with HTTP 200, consistent
with ClickHouse's [HTTP response caveats](https://clickhouse.com/docs/concepts/features/interfaces/http#http-response-codes-caveats).
