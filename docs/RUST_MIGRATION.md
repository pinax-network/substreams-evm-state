# Rust implementation and qualification

All tracked implementation code, command-line tools, qualification workloads and
tests now use Rust. The Python package, Python tests and dependency manifests,
Go transport adapter and Go fixture generator have been removed. CI checks that
those source/dependency files are not reintroduced. SQL, protobuf, captured
binary/JSON fixtures and documentation remain data and interface definitions.

The external Substreams CLI remains the native sink. Its upstream implementation
language does not add another language to this repository. The Rust installer
verifies the pinned Substreams 1.22.0 release archives.

The Cargo workspace contains the existing WASM package and the native
`crates/evm-state` library, `evm-state` controller and `evm-state-qualify` workloads.
Default package builds still select WASM; `make native` builds both native
executables, and `make test` covers the workspace. `make setup`, `make dev` and
`make sink` invoke the Rust controller.

## Preserved behavior

The migration retains the wire, SQL, cursor, controller, proof, checkpoint and
export formats. Existing backfills keep their frozen package, module hash,
source identity, database UUID, cursor and controller. Source and publisher
locks, complete proofs and immutable ready manifests still govern publication.

The former baseline at `587f9a0` contained 329 parameterized Python cases and
43 mapper tests. Rust test functions often group several corruption or boundary
cases; a lower function count does not represent removed assertions. The audit
preserves the following behavior:

| Area | Rust tests and retained expectations |
|---|---|
| Proofs and headers | `proof_parity`, `trie_boundaries`, `rpc`: independent account/storage proofs, non-inclusion, all required metadata, encoded BSC headers, exact requested block, missing/extra/duplicate/zero storage rejection; 20 fixed trie-oracle cases include deep branches, inline-child boundaries, random keys and committed 4,097-slot workspaces |
| Files, host identity, cursors | `local_state`, `lock_lifecycle`, `clickhouse_state`: atomic no-overwrite writes, OS identity and legacy attestation, public opaque cursor bytes in both directions, durable block/hash/marker binding and inherited writer locks |
| Native ingestion | `native_prepare`, `bootstrap_database`, `native_transport`: real pinned CLI, S2 gRPC requests decoded in Rust, finalized flush/worker options, copied-directory rejection, direct/spooled process kills, cursor-write failure after data insertion, bounded stop and recovery |
| Bootstrap and history | `bootstrap_database`: private compaction, checksummed prefixes, missing pointers and orphan candidates, gaps and resets, raw-input preservation, continued source ownership and resumable partition cleanup |
| Publication and onboarding | `native_prepare`, `publication_parity`, `bootstrap_database`: complete proofs before readiness, immutable old readers, killed publisher, disjoint nonempty compacted cohort, replay idempotence, independent fields and combined-filter continuation |
| Lifecycle | `publication_parity`, `lifecycle_evidence` and mapper tests: successful deletion and same/next-block recreation, diagnostic signals that retain storage, 26 captured producer fixtures with header/bytecode/proof provenance |
| Portable state | `portable`, `portable_database`, `read_qualification`: full offline verification, paginated deterministic export, exact large nonces, corrupted/compressed/changed files, post-insert corruption, preserved old ready state and complete returned-page root verification |
| Capacity | `capacity`, `capacity_workload`, `bootstrap_database`: whole data directories, component totals without double counting, persistent mounts, skewed counters, bounded fresh scans, incomplete-sample rejection, process-group stop, and checkpoint/export/import/bootstrap admission boundaries |
| Database crashes | `database_crash`: a new owned ClickHouse container is killed before ready publication and again afterward; candidates stay private, the pinned reader survives, a damaged cursor is explicitly recovered and later state is proven |
| PostgreSQL fallback | `postgres_verifier`, `postgres_database`: coherent SQL snapshots, exact metadata and complete storage verification, concurrent writes, bounded diagnostics and safe subprocess errors |
| Workload evidence | `output_qualification`, `throughput_evidence`, `lifecycle_evidence`, `synthetic`: logical protobuf byte counts, cold/cached/live evidence validation, captured lifecycle comparisons and independent synthetic roots |

Fault tests stop the first ingestion case before any block arrives and the
second after a verified durable cursor. A process killed during its first data
write without any valid cursor fails closed; it is not silently replayed over
ambiguous existing rows. Tests with prior durable progress separately cover a
failure after later data insertion but before cursor persistence.

## Completed real Rust measurements

- [Customer example](evidence/bsc-customer-example-rust-2026-09-12.json): Rust
  accepted the complete historical 8,156-slot state, continued 57,867 blocks with
  the rebuilt package, proved all 8,157 returned slots and exported/restored that
  state. Historical enumeration used the previous wrapper.
- [Isolated onboarding](evidence/bsc-onboarding-rust-2026-09-12.json): imported
  that proven state, added two disjoint accounts with proven empty storage at
  121542434, survived publisher SIGKILL and continued the combined filter to
  121542817. Old pinned pages remained unchanged. This does not establish recent
  bootstrap completeness for an arbitrary nonempty account.
- [Capacity workload](evidence/capacity-rust-2026-09-12.json): 64 synthetic
  accounts, 104,032 slots and 50,000 cleared/replaced slots. Initial, restored and
  updated checksums match the previous implementation exactly. Pin-aware
  retention passed, with no rejected capacity samples. This synthetic set is
  independent of the unavailable customer list.
- [Merge workload](evidence/merge-rust-2026-09-12.json): a Rust-created 256 MiB
  incompressible fixture merged successfully, with 809,286,373 bytes observed in
  its retained part catalog. The periodic scans did not capture an active merge
  row, so that limitation is explicit in the record. A separate
  [1 GiB workload](evidence/merge-rust-large-2026-09-12.json) then captured two
  active-merge observations and completed without rejected capacity samples.
- [Aggregation](evidence/bsc-aggregation-rust-2026-09-12.json): an isolated
  WBNB prefix plus one contiguous update produced 5,696,588 slots and identical
  ordered checksums under both settings. Explicit spilling used 1.91 GB of
  ClickHouse query memory in 16.5 seconds; observed defaults used 3.45 GB in
  11.3 seconds. Failed earlier attempts are retained. Neither private result is
  a complete account-root proof.
- [Frozen trie comparison](evidence/bsc-trie-rust-2026-09-12.json),
  [portable compatibility](evidence/bsc-portable-rust-2026-09-12.json),
  [complete reads](evidence/bsc-read-rust-2026-09-12.json),
  [logical output](evidence/bsc-output-rust-2026-09-12.json) and
  [lifecycle replay](evidence/bsc-lifecycle-rust-2026-09-12.json) retain their
  individual scopes and executable provenance.

The [WBNB handoff record](evidence/wbnb-rust-handoff-2026-09-12.json) preserves
an earlier incomplete directory-scan stop. Rust resumed the unchanged source
identity and compacted its saved suffix from private-prefix block 56,645,978
through durable block 56,974,989. The Rust historical replay continues toward
121466775. No Python controller remains active for this work.

A later [Rust capacity stop and resume](evidence/wbnb-rust-capacity-resume-2026-09-12.json)
preserves a timed-out disk scan at block 60,974,989. The compacted prefix and
durable cursor agree at that block, with 5,916,295 nonzero slots. A fresh full
measurement passed; the unchanged frozen binaries and source identity resumed
under the same capacity policy. This remains private, unverified state.

The [current-package continuation interval](evidence/bsc-wbnb-continuation-staged-rust-2026-09-12.json)
is staged separately: 83,160 contiguous blocks from 121466776 through 121549935,
with captured target proofs, validated native progress and encoded header, and
288,345,987 logical protobuf output bytes. All three capacity phases completed
without rejected samples. Complete-state publication still requires the pending
historical base proof.

Growth recording now matches the destination generation at the beginning of a
logged compaction query. The previous substring lookup also counted a later
compaction that read the same generation and stopped the recorder. A real
ClickHouse regression covers that sequence, wrong row counts and duplicate
writes to one destination. This changes measurement selection, not replay state.

## Release acceptance

The Rust recovery phase passed CI at `b63b602`, including its dedicated hard
ClickHouse restart test. The Rust-only source-removal commit `eb3c0f4` also
[passed CI](https://github.com/pinax-network/substreams-evm-state/actions/runs/34726918252).
Reproduce with `make test`, `make test-integration` and
`make test-postgres`; integration tests use disposable databases, and the crash
test needs space for a separate disposable Docker container.
The suite contains 91 standalone native tests, 49 regular ClickHouse tests,
one separate database-crash test, two PostgreSQL tests and 43 mapper tests;
two internal subprocess fixtures are excluded from those counts.
The host-bind scanner's [CI run](https://github.com/pinax-network/substreams-evm-state/actions/runs/34731177435)
passed all 186 tests at `380c9f7`. Its new cases cover fresh physical-mount
challenges, copied or replaced directories, nested mounts and filesystem aliases.

Full WBNB account-root acceptance, current-package continuation, hot-state
export/restore and sustained-growth qualification remain release gates. The
customer's actual 19-account/76-slot list and proposed 64-account set are still
unavailable. Keep those limits explicit in the final four-deliverable table and
[release notes](RELEASE-v0.1.0.md). Publish v0.1.0 only after those gates and the
final package-producing `make build` have passed.
