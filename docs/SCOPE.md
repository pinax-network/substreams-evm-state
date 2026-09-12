# EVM selective state: qualification scope

Updated 2026-09-11. The review of prototype `8433af7` is in [REVIEW.md](REVIEW.md).
The ClickHouse implementation now exists; acceptance evidence and remaining
release gates are tracked in [QUALIFICATION.md](QUALIFICATION.md).

## Customer requirements and boundaries

The target is selective BSC state for local execution of router, token and hook
code. The pilot covers 19 accounts / 76 named slots; the next qualification is
up to 64 accounts. The account set grows as dependencies are discovered.
The named slots are validation samples, **not** a bound on the full storage of
those accounts. Shared contracts may dominate retained state.

Required outcomes:

- Complete nonzero storage and account metadata/code at a recent, exact finalized
  block/hash, followed by a complete update interval.
- Updates keyed by the account actually changed, including persistent failed
  transaction, authorization, system and lifecycle effects.
- Coherent block-end publication and continuity through empty blocks, restarts
  and the selected finality policy.
- A repeatable way to initialize newly discovered accounts before marking them ready.
- Retained local data within an agreed limit in the 100–300 GB range, including
  indexes, history, temporary merge space, checkpoints and sink metadata.
- A clear distinction between root-verified state and provider-attested state.

Engineering preference: use a native protobuf map output with ClickHouse if
qualification passes; keep PostgreSQL + `db_out` as the existing fallback.
Database choice is not itself the customer's acceptance criterion.

Excluded from this qualification: a full node, an all-account chain replica,
arbitrary historical snapshot service, an unbounded event archive, a general
Parquet export product, and commercial/subscription changes. Other chains and
non-finalized ClickHouse state require separate qualification. Team suggestions
in the customer conversation are not additional delivery commitments.

## Implemented baseline

```text
Extended Block + account params ──> db_out ─────────────> PostgreSQL DatabaseChanges
                              └──> map_state_changes ─> evm.state.v1.StateChanges
```

Both modules call the same collector independently. `db_out` has no intermediate
module dependency. Params are an address list; empty means all accounts.
`include_events` and `include_code` options do not exist. Events are always emitted.

PostgreSQL maintains current storage and independently updated account fields,
deduplicated bytecode, block markers, and partitioned event tables. State starts
partial: an unobserved field is unknown, not zero. Zeroed storage rows are kept,
so even current storage grows with slots ever touched, not only live nonzero slots.
The current tables do not support arbitrary historical reads.

The original PostgreSQL tests and measurements are baseline evidence, not full
customer qualification. Native ClickHouse output, guarded ingestion, immutable
checkpoint publication and account/header/storage proof verification are now
implemented. Portable exports, verified restores, persistent reader pins and
whole-checkpoint cleanup are also implemented. Cursor validation/backup/recovery
and native history partition cleanup are implemented. Initial replay can compact
private state between bounded chunks; peak disk accounting and representative
capacity qualification remain unfinished.

## Native ClickHouse route

```text
Extended Block + account params
  └──> map_block_state (shares persistence collector)
         └──> annotated protobuf
                └──> substreams sink clickhouse
                       └──> block-end versions + published checkpoint/read views
```

There is no `DatabaseChanges`/`db_out` step on this path. A native SQL sink is
still needed to write the map output. Preserve the existing granular
`map_state_changes` stream for consumers needing individual changes.

One physical `state_blocks` row contains the complete block's changes as inline
Nested arrays, with one final value per changed key **per block**. Balance, nonce
and code are independent groups; a balance-only change does not replace the other
fields. This avoids the native sink's lack of transactions across multiple tables.

| Envelope group | Logical identity | Value |
|---|---|---|
| Block metadata | block number/hash | Parent, timestamp, state root, filter/schema identity |
| Storage version | address, slot, block identity | Final value and ordinal, including zero |
| Balance version | address, block identity | Final balance |
| Nonce version | address, block identity | Final nonce |
| Code version | address, block identity | Final code hash, explicit empty code |
| Lifecycle | address, ordinal | Confirmed transaction-end storage reset; diagnostic SELFDESTRUCT, nonce reset and code-clear signals |

The native generated DDL uses `ORDER BY (number, hash)`, a `number` primary key,
and a daily partition. Integration tests create their tables with the real
native CLI from the built package, including the inline Nested arrays.

Why keep block versions for qualification:

- The native sink generates `_version_` from ingestion time, not block/ordinal.
  Versioning by block identity keeps late historical delivery from replacing
  another block's value. Read logic selects the greatest blockchain position.
- Its default sorting includes block/row identifiers and its partition builder
  adds a time partition. Merely marking an account key does not create a
  compact current-state table spanning all history.
- Reorg tombstones target rows from orphaned blocks. Collapsing all blocks onto
  one state key can discard the predecessor that undo would need to restore.
- Retained block versions allow readers to exclude partially ingested newer
  blocks while serving a previously published checkpoint.

This design still needs bounded retention. It is not permission to accumulate
all historical state changes indefinitely.

### Read and publication contract

Start with finalized blocks only. Deduplicate native sink retries (`FINAL` or
equivalent), then select the latest version at or below an explicitly published
block/hash. Apply the zero-slot filter **after** selecting the latest version,
otherwise old nonzero values can reappear. Resolve account fields independently.
The native mapper applies BSC's SELFDESTRUCT fork/creation-context rules. A
confirmed deletion removes all earlier slot versions, including inherited
checkpoint storage, while later recreation writes survive. Code clearing alone
does not erase storage. See [lifecycle evidence and remaining coverage](LIFECYCLE.md).

The one-row block envelope supplies block-level atomicity. Checkpoint candidates
are built separately and receive a ready manifest only after complete storage,
metadata, code, account proofs and interval continuity verify. A failed candidate
cannot change previously published checkpoints. Native internal `_blocks_` rows
are not publication signals. Tests interrupt publication after storage and after
account insertion, and fail native cursor writes after block data insertion.

The runner binds database ownership, filter, package/module identity and local
schema metadata, locks out competing local writers, and rejects cursor loss or
database replacement. Keep the cursor, frozen package and spool on a durable
volume. A checked, synced cursor backup supports explicit torn-cursor recovery;
publication requires durable native progress covering its target. A source binds
to one checkpoint destination so history cleanup can preserve all of its retained
checkpoint continuation intervals. Tests kill the native and database processes;
physical host power loss is not emulated and storage must honor sync writes.
Initial replay compaction has adversarial and native-sink tests; representative
initial replay capacity and peak disk usage still require qualification.

## Bootstrap and growing account sets

Replay is a candidate initial-state source when history and lifecycle semantics
are sufficient. A creation-block replay alone does not prove unchanged account
metadata, pre-creation balances, genesis/predeployed state, or deletion/recreation
handling. Qualify those cases or mark the account unsupported.

Required onboarding sequence:

1. Record chain, normalized account set, package/module hash, source history
   start and target finalized block/hash for an isolated bootstrap job.
2. Replay new accounts into separate staging tables/database and cursor state.
   Do not restart the existing live sink at an older block or let staging
   overwrite shared live head/state rows.
3. Fill and verify all account metadata/code at the target; enumerate all nonzero
   storage. A list of named slots is not a complete account export.
4. Recompute the storage trie and verify the account proof against the chosen
   header's state root. Verify nonce, balance and bytecode hash as well. Record
   the trust source of the header. Keep provider attestation a separate outcome.
5. Catch up through a common finalized cutover block; establish a complete
   continuation interval and promote the new accounts as one ready generation.
   Only then change the live filter/cursor configuration deliberately.

Each distinct params string changes module identity/cache. Replaying only new
accounts avoids redoing their peers' bootstrap, but changing the shared live
filter still changes its module hash. Silencing that mismatch is not onboarding.

Export a checkpoint manifest with chain, block/hash/state root, account list,
schema/module version, row counts, checksums, verification status and continuation
position. Require metadata presence for readiness; never silently treat NULL as zero.

## Ordered delivery and acceptance

| Milestone | Deliverables | Acceptance |
|---|---|---|
| 0. Reproducible baseline | Fresh-checkout build; accurate docs; pinned package and sink versions | Locked build, existing tests and package validation pass |
| 1. Native ClickHouse proof of concept | Block-end map/proto, local setup, durable sink metadata, read/publication queries | Real sink path; repeated key in/between blocks, partial account fields, zero/empty code, replay and mid-flush restart all produce the expected published state |
| 2. Bootstrap and completeness | Isolated onboarding, fixed checkpoint, account proof verification, export manifest | New account catches up without changing existing ready state; missing/tampered state, metadata or proof fails readiness |
| 3. Customer qualification | Actual pilot filter, then bounded 64-account sample; recent and old/shared contract bootstrap | Endpoint parity and lifecycle fixtures pass; empty-block continuity and read latency meet agreed targets |
| 4. Capacity and release | State/event retention, checkpoint rotation, merge headroom, repeatable benchmarks, CI, versioned package and runbook | Measured retained footprint fits the selected cap; cold/cached/live results reproducible; package installs from clean checkout |

Use the existing PostgreSQL route if native ClickHouse cannot meet coherent reads,
bootstrap correctness or storage limits without excessive custom work.
Non-finalized ClickHouse rollback is a later milestone with explicit adversarial
tests; existing PostgreSQL undo claims also need an integration test before handoff.

Qualification must cover successful/reverted nested calls, failed sender effects,
7702 self/multiple/discarded authorizations and code clearing, block/system effects,
CREATE/CREATE2, SELFDESTRUCT across applicable fork rules, deletion/recreation,
and older producer versions. Reject unsupported/incomplete source blocks instead
of publishing an apparently valid empty result. Sampling historical blocks does
not establish complete history or lifecycle support.

## Measurements and remaining inputs

Re-run cold build, cached delivery and sustained live follow separately, using
the actual output type and customer filter. Record requested and active workers,
exact block range and timestamps, package/module hashes, decoded protobuf bytes,
rows, database/merge disk usage, cursor lag and publication/read latency.
Noop message counters are not block counts. Cache warmth must be established,
not inferred solely from a new range.

The prior three-account PostgreSQL results are illustrative. They do not justify
a 19/64-account price, a universal per-account bound, linear worker scaling,
or a complete-history SLA. Current published rates use **TiB**, not decimal TB;
module output volume, billed traffic and retained database size are separate.

Inputs for the qualification run: actual account/slot sample and known creation
blocks; selected retained-data cap; acceptable finalized lag and publication/read
latency; checkpoint/update retention interval; required trust model for the header
and proof. These do not block the repo review or local prototype.
