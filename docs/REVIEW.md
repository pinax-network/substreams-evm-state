# Prototype handoff review — 2026-09-11

Reviewed repository `8433af7` and the current upstream native SQL sink at
[`1b7d09c`](https://github.com/streamingfast/substreams/tree/1b7d09c2de7b2f8c3c4083b590476f34d74c293a/sink/sql/db_proto).
The local installed CLI reports a development build at `be35ad3`; the relevant
upstream versioning/DDL behavior was also checked at the newer commit.
The revised delivery plan is [SCOPE.md](SCOPE.md).

## Confirmed baseline and handoff fix

- Working checkout: `cargo test` passes all seven tests (three filter, four
  persistence). These do not cover database projection, proof verification,
  onboarding, reorgs or restart behavior.
- Clean `git archive HEAD` checkout: `cargo test --locked --offline` fails because
  `src/pb/mod.rs` includes `evm.state.v1.rs`, which was ignored and untracked.
  The handoff fix includes that generated binding in source control so normal
  build/test does not depend on a prior developer's generated files. Regenerate
  deliberately with `make protogen` after proto changes.
- The shared collector already allows adding a native protobuf projection
  without making `db_out` an intermediate dependency.
- With the binding included, an archive of the staged source passes
  `cargo test --locked --offline` (seven tests), `make pack` (WASM build and
  package creation), and `substreams info` for both expected output modules.
  A GitHub Actions workflow now runs locked tests and the release WASM build
  from a fresh checkout.

## Findings that remain open

### 1. Root comparison is not the complete verification contract

**Resolved 2026-09-12:** the legacy scripts now use one coherent SQL snapshot and
the shared account/storage proof verifier. Unknown or wrong metadata fails,
`--block` must equal the current head, and sampled results cannot claim complete
storage. The finding below records the reviewed prototype's original behavior;
see [current verification commands](POSTGRES.md#verification).

The former `verify_storage_root.py` prototype recomputed a storage
trie and compared it with `eth_getProof.storageHash`. It did not validate
`accountProof` against the exact block header's state root. Its separate SQL
queries could observe different heads while ingestion ran, and `--block N` changed
the RPC block without selecting historical DB state.

It also exits successfully when nonce/balance/code-hash comparisons print
`MISMATCH`, provided the storage roots match. Reproduced with an empty storage
trie, DB nonce 1 and RPC nonce 2: output reports the nonce mismatch and exit status
is 0. Missing metadata is skipped. The RPC spot checker defaults to 1,000 slots,
not every slot, and similarly reads current tables rather than an as-of snapshot.

Before readiness can depend on these checks: capture one consistent database
snapshot and exact header, validate account inclusion/non-inclusion and metadata,
hash bytecode, fail on missing required fields or any mismatch, and distinguish
trusted-header proof verification from matching a provider-reported root.
See [EIP-1186](https://eips.ethereum.org/EIPS/eip-1186).

### 2. Account onboarding into a live database is not implemented

[`db_out.rs`](../src/db_out.rs) upserts state without a condition rejecting older
block numbers. A historical replay that overlaps existing keys can overwrite
newer values. A single shared cursor also resumes from its existing position;
changing start flags does not create a separate bootstrap stream. A newer block
marker does not prove a newly added account was bootstrapped.

The prior instruction to replay new accounts into the same database is unsafe as
a general runbook. Use isolated staging/cursors, verify and catch up, then promote
at a common finalized boundary. A module-hash warning override does not fill gaps.

### 3. Complete lifecycle and input validation are unqualified

[`persist.rs`](../src/persist.rs) visits the four explicit change collections but
has no separate account deletion/storage-reset handling; there are no lifecycle
fixtures proving the producer supplies every necessary effect. Treat this as an
unverified prerequisite, not a demonstrated producer defect. SELFDESTRUCT rules
vary by fork and creation context ([EIP-6780](https://eips.ethereum.org/EIPS/eip-6780)).
Sparse historical Extended samples do not prove all historical semantics.

[`collect`](../src/lib.rs) does not reject non-Extended input and defaults missing
header fields to empty/zero. An unsuitable source can therefore look like a valid
empty state update. Define supported model/version boundaries and fail closed.

### 4. Retention is not yet a bounded-state product

Event output is always on. There is no implemented `include_events` or
`include_code` flag. Partitions are prepared manually; dropping their tables is
manual retention, and out-of-range events fall into a DEFAULT partition.
Storage retains cleared slots; blocks, bytecode and historical slot keys also
grow. Neither a 100 GB nor a 300 GB footprint has been demonstrated for the
customer's account set. Transport savings are not database-size measurements.

## Native ClickHouse: source-backed constraints

| Observation | Consequence |
|---|---|
| [Generated engine is `ReplacingMergeTree(_version_, _deleted_)`](https://github.com/streamingfast/substreams/blob/1b7d09c2de7b2f8c3c4083b590476f34d74c293a/sink/sql/db_proto/sql/click_house/dialect.go#L174) | Native protobuf ingestion can reconcile replacement rows, but key and query design determine which rows replace one another |
| [`_version_` is `time.Now().UnixNano()`](https://github.com/streamingfast/substreams/blob/1b7d09c2de7b2f8c3c4083b590476f34d74c293a/sink/sql/db_proto/sql/database.go#L178) | Later ingestion is not necessarily later chain state; isolate backfills or retain block versions |
| [Default ORDER BY and partition construction](https://github.com/streamingfast/substreams/blob/1b7d09c2de7b2f8c3c4083b590476f34d74c293a/sink/sql/db_proto/sql/click_house/dialect.go#L348) | Default ordering keeps block/row identity; partition construction adds month unless a timestamp field is specified. Explicit keys alone do not ensure physical compaction across months |
| [Undo inserts deletion versions for orphaned blocks](https://github.com/streamingfast/substreams/blob/1b7d09c2de7b2f8c3c4083b590476f34d74c293a/sink/sql/db_proto/sql/click_house/database.go#L578) | Inference: collapsing different blocks onto a single state key can lose predecessor state or let an orphan tombstone hide it. Qualify finalized block versions first |
| [Transaction methods do nothing](https://github.com/streamingfast/substreams/blob/1b7d09c2de7b2f8c3c4083b590476f34d74c293a/sink/sql/db_proto/sql/click_house/database.go#L416), tables flush separately and cursor persists to a file | A blocks row is not an atomic multi-table checkpoint. Explicit publication and restart tests are necessary |
| [Postgres native row insertion](https://github.com/streamingfast/substreams/blob/1b7d09c2de7b2f8c3c4083b590476f34d74c293a/sink/sql/db_proto/sql/postgres/row_inserter.go#L61) only upserts the internal cursor in this inserter | PostgreSQL itself supports upserts; this native mapping path does not expose state upserts. Keep `db_out` for the current PostgreSQL design |

ClickHouse replacement is eventual and uses the sorting key. Correct reads need
`FINAL` or equivalent reconciliation; merges are not a correctness barrier.
See [ReplacingMergeTree documentation](https://clickhouse.com/docs/engines/table-engines/mergetree-family/replacingmergetree).
The proposed native route needs a small real-sink qualification, not simply
annotations on the existing event messages.

## Measurement audit

The previous agent's local scratch evidence was inspected without copying raw
logs or credentials into this repository. `noop_50k.log` covers blocks
121000000–121049999 and runs from 16:36:24 to 16:38:02 EDT on 2026-09-10:
about 98 seconds. It requests 100 workers, reports at most 50 active jobs, and
ends with 50,000 processed blocks but only 1,000 received messages. This supports
the wall-time observation and the warning about noop counters. It does not prove
sustained 100-worker scaling or independently prove all segments were uncached.

The README's other prior measurements remain attributed to that run; no new
paid/full-history stream or live database qualification was performed in this
review. The three-account filter differs from the customer's 19-account probe.

Corrections for estimates:

- Published [Substreams pricing](https://pinax.network/pricing/substreams) and
  [Firehose pricing](https://pinax.network/pricing/firehose) are $150/**TiB** and
  $1.75/million processed blocks, not $150/decimal TB.
- At 500 blocks/s, 121M blocks take about **67 hours**; 34 hours assumes an
  unmeasured sustained 1,000 blocks/s. Even the nominal 1,000 blocks/s model
  would take roughly 22 minutes to cover a week at 0.45 s/block, not seconds.
- Historical BSC block intervals differ. Use the exact sampled interval for a
  measurement and state the block/month assumption for a projection.
- The 9 KB state-only figure is an estimate, not a measured supported mode.
  Native ClickHouse output must be measured separately.
- Noop warm-up output, cached delivery and end-to-end sink ingestion are different
  operations. Generated cache bytes are not automatically client-billed egress.
  Confirm actual usage accounting before quoting a bootstrap bill.
- The apparent monthly savings are a scenario comparison across different filters,
  not a qualified customer-specific multiplier. Include compute on Firehose too.

## Handoff decision

Proceed with a finalized native ClickHouse qualification and keep the Postgres
baseline. Gate a customer-ready release on fixed-block proof verification,
isolated onboarding, coherent publication, lifecycle coverage and measured
retention. The existing prototype is not yet a complete selective state service.
