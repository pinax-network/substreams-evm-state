> Legacy PostgreSQL baseline. The primary prototype now uses ClickHouse; see [the main README](../README.md).
> Historical measurements below do not qualify the native ClickHouse route or the customer account set.
> Legacy RPC/root scripts are diagnostic only and must not be used to mark an account ready.

# substreams-evm-state

Account-keyed **EVM state projection** for Firehose *Extended* blocks, sunk into
**PostgreSQL** with the `substreams sink postgres` command of the
[`substreams` CLI](https://github.com/streamingfast/substreams).

Given a list of accounts (or none = every account), it streams every
**persisted** storage / balance / nonce / code change of those accounts and
maintains their current state plus an event log, block by block, with reorg
handling delegated to the sink.

> Status: **prototype (stage 2)**. Verified on BSC against `bsc.rpc.pinax.network`
> (see [Verification](#verification)). Network defaults to `bsc`; other EVM
> Extended chains require separate qualification. Historical BSC samples include
> Extended block 1 (`ver=3`), with `ver=5` on the tested current blocks.
> See the [handoff review](REVIEW.md) and [revised scope](SCOPE.md):
> native ClickHouse ingestion is proposed, while complete bootstrap, lifecycle
> coverage, proof-backed readiness and bounded retention remain open.

## What it covers

| Change source | Included | Notes |
|---------------|----------|-------|
| Successful transactions | ✅ | every call with `state_reverted == false` |
| Failed / reverted transactions | ✅ | only what persists: `GAS_BUY`, `GAS_REFUND`, `REWARD_TRANSACTION_FEE` balance changes and the sender nonce |
| EIP-7702 `SET_CODE` transactions | ✅ | authority nonce + delegation code persist even when the tx fails; authorization list stored separately |
| `Block.system_calls` | ✅ | e.g. EIP-2935 history contract `0x0000f908…2935`, written every BSC block |
| `Block.balance_changes` / `Block.code_changes` | ✅ | validator fee rewards, fork upgrades |
| Empty / non-matching blocks | ✅ | a `blocks` row is written for every block, so cursor and snapshot continuity is preserved |
| Reorgs | ◐ (sink) | `--final-blocks-only`, or PostgreSQL undo via `substreams_history`; rollback still needs an integration test |
| Initial state bootstrap | ◐ | this streams *changes*; state columns are `NULL` until first observed. Replay from creation is a candidate bootstrap, subject to lifecycle/history coverage and fixed-block verification (see [Bootstrap](#bootstrap)) |

Filtering is on the **address of the changed account** in each state-change
record, never on call-to / `tx.to`. A filtered run is an exact subset of an
unfiltered one.

## Modules

```
sf.ethereum.type.v2.Block ──► db_out(params) ──────────► DatabaseChanges   (the sink module)
                          └─► map_state_changes(params) ► evm.state.v1.StateChanges   (optional, gRPC consumers)
```

`db_out` reads the Firehose block directly and applies the persistence rules
itself, so the sink executes exactly one module and nothing intermediate is
cached server-side. `map_state_changes` shares the same code and filter but is
not a dependency of `db_out`; it is only executed if you request it.

### Params: the account filter

Both modules take one string param: a comma (or whitespace) separated list of
20-byte addresses, `0x` optional, case-insensitive. **Empty = all accounts**
(see [Sizing](#sizing) before doing that).

```bash
-p db_out=0x32c59d556b16db81dfc32525efb3cb257f7e493d,0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c
```

The params value is part of the module hash. Changing the list changes the
hash. The Makefile currently sets `--on-module-hash-mismatch=warn`; this bypass
does not initialize a new account or prove continuity. Bootstrap new accounts
with separate staging data and cursor state, verify and catch up, then deliberately
cut over the live filter. Do not replay an older range over live state: current
upserts do not reject older block numbers. See [Bootstrap](#bootstrap).

## PostgreSQL schema

`postgres/schema.*.sql` layers, concatenated by `make schema` into
`postgres/schema.sql` (the file the sink applies on `setup`).

**Current state** (block-end values, last write by ordinal wins):

| Table | Key | Columns |
|-------|-----|---------|
| `accounts` | `address` | `balance` (wei), `nonce`, `code_hash`, `block_num`, `balance_block_num`, `nonce_block_num`, `code_block_num` |
| `storage` | `(address, slot)` | `value`, `block_num`, `ordinal` — slots cleared to zero keep a row with `0x00…00` |
| `code` | `code_hash` | `code` (`BYTEA`), `size`, `first_block_num` — deduplicated bytecode |
| `blocks` | `block_num` | `block_hash`, `parent_hash`, `timestamp`, `state_root`, `coinbase`, `transaction_count`, per-type change counts |

**Event log** (one row per persisted change, keyed `(block_num, ordinal)`,
`PARTITION BY RANGE (block_num)`): `storage_changes`, `balance_changes`
(with `reason`), `nonce_changes`, `code_changes`, `set_code_authorizations`.
Each carries `scope ∈ {tx, tx_failed_persistent, tx_7702, system_call, block}`,
`tx_hash`, `tx_index`, `tx_status`, `call_index`.

Partitions must exist before rows arrive. `make pg-setup` creates them for
`[PARTITION_FROM, PARTITION_TO)` in `PARTITION_STEP` blocks via the SQL
function `create_event_partitions(from, to, step)`; a `DEFAULT` partition
catches anything outside. Retention is `DROP TABLE storage_changes_p<from>`.

**Views**: `storage_nonzero`, `account_state` (accounts ⋈ code, bytecode as hex), `head`.

Encoding: `0x`-prefixed lower-case hex `TEXT` for addresses, hashes, slots and
values; `BYTEA` for bytecode; `NUMERIC` for wei; `BIGINT` for nonces and
blocks; `TIMESTAMP` (UTC).

## Quick start

Prerequisites: Rust 1.88 + `wasm32-unknown-unknown` (via `rust-toolchain.toml`),
[`substreams`](https://github.com/streamingfast/substreams/releases) CLI
≥ v1.20.2 (ships `substreams sink postgres`), Docker, `psql`, Python 3 with
`pycryptodome` for the storage-root check.

Generated Rust protobuf bindings are committed so a fresh checkout builds
directly. Run `make protogen` when changing the protobuf schema.

```bash
export SUBSTREAMS_API_KEY=...         # never commit keys; see .env.example

make pg-up                            # local Postgres 16 on :5432 (docker compose)
make pg-setup                            # build wasm, pack spkg, create tables + partitions
make pg-dev                              # stream the default 32-block window into Postgres
make psql                             # poke around
```

Defaults (override on the command line):

| Var | Default | Meaning |
|-----|---------|---------|
| `ENDPOINT` | `bsc.substreams.pinax.network:443` | Substreams endpoint |
| `START_BLOCK` / `STOP_BLOCK` | `120140091` / `120140123` | block range for `make pg-dev` |
| `ACCOUNTS` | sample contract + WBNB + EIP-2935 contract | params filter |
| `PG_DSN` / `PG_URL` | local docker DSN | Postgres (sink DSN / psql URL) |
| `PARTITION_FROM` / `PARTITION_TO` / `PARTITION_STEP` | `120000000` / `130000000` / `1000000` | event-log partitions created by `make pg-setup` |

```bash
make pg-dev  ACCOUNTS=0xabc…,0xdef… START_BLOCK=121114100 STOP_BLOCK=121114161
make pg-sink START_BLOCK=121114100       # follow head, final blocks only
make verify                           # RPC cross-check (RPC_API_KEY in env)
make verify-root ADDRESS=0x…          # storage trie root vs eth_getProof
```

`make pg-dev` runs `substreams sink postgres … --development-mode --undo-buffer-size 0`
and flushes every block. `make pg-sink` adds `--final-blocks-only`. The sink
resumes from the cursor stored in the `cursors` table; to re-run a different
range, use a fresh database (`make pg-down && make pg-up && make pg-setup`).

### Example queries

```sql
SELECT * FROM head;                                            -- latest committed block
SELECT * FROM account_state WHERE address = '0xbb4c…';         -- balance / nonce / code
SELECT slot, value FROM storage_nonzero WHERE address = '0x32c5…' ORDER BY slot;
SELECT block_num, scope, address, old_value, new_value          -- who paid gas on failed txs
  FROM balance_changes WHERE scope = 'tx_failed_persistent' ORDER BY block_num, ordinal;
SELECT * FROM nonce_changes WHERE scope = 'tx_7702';             -- 7702 authority nonces
```

## Semantics

Rules applied by `src/persist.rs` (from the `sf.ethereum.type.v2` proto docs,
confirmed on BSC Firehose):

1. **`SUCCEEDED` tx** — record every change of every call with `state_reverted == false`.
2. **`FAILED` / `REVERTED` tx** — consult only the root call. Keep balance
   changes with reason `GAS_BUY`, `GAS_REFUND`, `REWARD_TRANSACTION_FEE`
   (`scope = tx_failed_persistent`) and the transaction sender's nonce increment,
   matched by sender, transaction nonce and nonzero ordinal. Everything else is dropped.
3. **EIP-7702** (`TRX_TYPE_SET_CODE`) — for each authorization with
   `discarded == false`, the `authority`'s nonce and code changes persist even
   when the tx fails (`scope = tx_7702`), provided their nonzero ordinal precedes
   root-call execution. Later reverted execution changes to the same authority
   are excluded. Missing execution boundaries fail closed. On BSC the root call
   carries both sender and authority nonce effects; self-authorization does not
   duplicate the sender increment. See [persistence evidence](LIFECYCLE.md).
4. **`Block.system_calls`** — calls with `state_reverted == false` (`scope = system_call`).
5. **`Block.balance_changes`, `Block.code_changes`** — always (`scope = block`).
6. No-op records (`old == new`) are dropped.
7. Within a block, state tables receive the **last change by ordinal** per key;
   the event log keeps every change.

Reprocessing the same block over the same state is idempotent. This does not make
out-of-order replay safe: an older upsert can replace a newer current-state value.
Lifecycle completeness (including deletion/recreation) and unsupported-source
rejection still require qualification; see [the review](REVIEW.md).

## Verification

`scripts/verify_rpc.py` compares up to 1,000 `storage` slots by default (override
with `--limit`) and observed `accounts`
fields (balance, nonce, code) with `eth_getStorageAt` / `eth_getBalance` /
`eth_getTransactionCount` / `eth_getCode` at the DB head block, plus the block
hash and `state_root` against `eth_getBlockByNumber`.

`scripts/verify_storage_root.py 0x<addr>` recomputes the account's storage
trie root (secure Merkle Patricia Trie over `storage_nonzero`) and compares it
with `eth_getProof(addr, [], block).storageHash`. A match checks completeness
relative to the **RPC-reported root**; the script does not verify `accountProof`
against the header's state root. It can also exit successfully despite metadata
mismatches. Neither script reads a consistent database snapshot while writes run,
and `--block` does not provide historical DB state. Pause ingestion at the target
for diagnostic checks; these scripts are not a production readiness gate.
The previously tested endpoint had a short recent-proof window. Full fixed-block
proof verification is part of the [remaining scope](SCOPE.md).

Results reported by the prior prototype run on BSC (2026-09-10,
`bsc.rpc.pinax.network`; not re-run during the 2026-09-11 review):

| Test | Blocks | Filter | Checked | Mismatches |
|------|--------|--------|---------|------------|
| Contracts | 120140091–120140122 | sample contract, WBNB, EIP-2935 | 392 slots, 1 balance, hash, state_root | 0 |
| EOAs, failed 7702 txs | 121114100–121114160 | 3 EOAs + EIP-2935 | 62 slots, 3 nonces, 2 balances, 1 code (BYTEA), hash, state_root | 0 |
| Live follow | 120140123–120155883 (15,763 blocks) | 7702 bot EOA | nonce | 0 |
| Storage root | 121114203–121122203 (creation → head, 8,001 blocks) | `0x98dd05…5ffff` | 46-slot trie root vs `eth_getProof.storageHash`, nonce, code_hash | 0 |

Unit tests for the persistence rules: `make test`.

## Sizing

Prior BSC samples below. The daily estimate uses the older 0.75 s/block
assumption (≈ 115k blocks/day), not the 0.45 s scenario used for monthly costs:

| Run | Rows / block (event log) | DB growth |
|-----|--------------------------|-----------|
| Unfiltered, 10 blocks | ≈ 1,540 storage + 435 balance + 113 nonce | ≈ 2.3 MB/block ⇒ **≈ 250 GB/day** |
| 3 contracts (incl. WBNB), 32 blocks | ≈ 90 storage + 17 balance | small |
| Filtered live follow, per-block flush | — | ≈ 56 blocks/s throughput |

Unfiltered event logging is not viable for a bounded local budget; the
current storage grows with slots **ever touched**, including cleared slots.
Blocks and bytecode also accumulate. Event retention currently requires manual
partition drops; a state-only mode would require code/schema changes, not just
a run flag. The customer's retained-data budget has not yet been qualified.

## Throughput and cost

Prior measurements from 2026-09-10 on `bsc.substreams.pinax.network`, using the
three-account sample filter (sample contract, WBNB, EIP-2935). This differs from
the customer's 19-account filter; it is not a per-account upper bound.

| Measurement | Prior result |
|-------------|--------------|
| `db_out` output | ≈ 27 KB/block (≈ 165 rows/block; estimated 2/3 event bytes, 1/3 state bytes) |
| Reported uncached backprocessing, 20 workers, 10,000 blocks | 87 s ⇒ ≈ 115 blocks/s |
| 100 workers requested, 50,000 blocks | ≈ 98 s ⇒ ≈ 500 blocks/s; recovered log reports at most 50 active jobs |
| Reported cached delivery of the same 50,000 blocks | 14 s ⇒ ≈ 3,500 blocks/s |
| Live follow, per-block flush | ≈ 56 blocks/s |

The recovered 50,000-block log supports its wall time, not sustained linear
worker scaling or an independently verified cold cache. At 500 blocks/s,
121M blocks would take about **67 hours**. The former 34-hour estimate assumed
an unmeasured 1,000 blocks/s. Historical ranges, producer versions, account
activity and worker availability need representative measurement before an SLA.
Recent creation narrows the range; it does not guarantee a seconds-long bootstrap.

Illustrative usage costs using the published
[Substreams](https://pinax.network/pricing/substreams) and
[Firehose](https://pinax.network/pricing/firehose) rates checked 2026-09-11:
**$150/TiB + $1.75/million processed blocks**, USD. Assume 5.76M blocks per
30 days (0.45 s/block), decimal KB/GB for the output estimates, and convert bytes
to TiB (`2^40`) for billing. Hosting, retention and other services are excluded.

| Scenario | Assumed decoded output | Approximate usage cost |
|----------|------------------------|------------------------|
| Three-account `db_out`, state + events | 27 KB/block ⇒ 155.5 GB/month | $31/month |
| Hypothetical state-only output (not an implemented mode) | 9 KB/block ⇒ 51.8 GB/month | $17/month |
| Firehose at the customer's probe rate (different filter) | 55,549,962 bytes / 32 blocks ⇒ 10,000 GB/month | $1,374/month |
| One full 121M-block delivery at 27 KB/block | 3,267 GB | $657 once |

The last row models **delivered output**, not a measured noop cache-build bill.
Confirm actual billed bytes and processed blocks for warm-up and subsequent
replay before quoting a bootstrap total. Compute still depends on the scanned
block range; output depends on tracked account activity. These examples do not
establish a customer-specific savings multiplier, retained database size, or
native ClickHouse cost. See [the measurement audit](REVIEW.md).

## Bootstrap

This package does not export a complete initial state checkpoint. Candidate paths:

* **Replay from creation into isolated staging.** The prior run replayed
  `0x98dd051fe7d43b2943b1245ca26e8c565dc5ffff` from block 121114203 through
  121122203 (8,001 blocks), reportedly in about 30 seconds with 50 workers.
  Its 46-slot storage trie matched the RPC-reported `storageHash`. This is one
  successful storage sample, not a general lifecycle or account-completeness
  proof. Older, hotter contracts require representative replay measurements.
* **External snapshot.** A compatible complete snapshot at block/hash N can
  seed staging before incremental updates at N+1. No supported snapshot importer
  or export service is included here.

For a growing live filter, keep bootstrap data and cursors separate, verify
storage **and** metadata/code at a fixed finalized block/hash, catch up to a common
cutover point, then promote. Do not replay old blocks directly into the live
state tables. A creation-block replay can leave unchanged metadata unknown and
needs qualified source history and deletion/recreation semantics. Proof-backed
readiness and checkpoint publication remain in the [delivery scope](SCOPE.md).

## Layout

```
substreams.yaml            # db_out (sink) + map_state_changes (optional)
proto/evm/state/v1/        # StateChanges proto
src/lib.rs                 # collect(): shared core; both handlers
src/persist.rs             # persistence rules (unit-tested)
src/params.rs              # account filter
src/db_out.rs              # Tables projection
postgres/schema.*.sql      # numbered layers → schema.sql (generated)
scripts/verify_rpc.py      # RPC cross-check
scripts/verify_storage_root.py  # MPT storage root vs eth_getProof
docker-compose.yml         # local Postgres 16
docs/SCOPE.md              # scoping notes and open questions
docs/REVIEW.md             # handoff findings and native ClickHouse constraints
```

## Sink modes and the proposed ClickHouse route

`substreams sink postgres` auto-detects its mode from the output module type.
A `DatabaseChanges` output runs in *database-changes* mode (create / update /
upsert / delete, delta ops, undo via `substreams_history`). Any other protobuf
with `schema.table` / `schema.field` annotations runs in *relational
mappings* mode. On **PostgreSQL**, tables are inferred from the proto and data
uses insert/COPY paths without state upserts. PostgreSQL itself supports upserts;
this native mapping path is the limitation. The current PostgreSQL state design
therefore keeps `db_out`.

On **ClickHouse**, the native mapping path creates
`ReplacingMergeTree(_version_, _deleted_)`, so a protobuf map can provide
replacement rows without `DatabaseChanges`. This is not a drop-in equivalent of
PostgreSQL upserts: replacement follows the sorting key, `_version_` is assigned
at ingestion time, and table writes are not one atomic block transaction.

The proposed next milestone is a native `map_block_state` output with finalized
block-end versions, independent account fields, retry-safe reads, and an explicit
publication/checkpoint boundary. It is **not implemented yet**. See
[scope and acceptance tests](SCOPE.md) and [upstream source review](REVIEW.md).

## Known issues / follow-ups

* Do **not** import the `substreams-sink-sql-protodefs` spkg in `substreams.yaml`:
  current CLI builds embed those protos and the import causes
  `name conflict over sf.substreams.sink.sql.v1.Service` in `substreams run`,
  `gui` and `protogen`. The `sink:` section works without it.
* `substreams-sink-sql` (standalone binary) is deprecated but still works with
  this package and the same database; the Makefile uses the CLI.
* `substreams sink noop` is a cache warm-up: the server runs in noop mode and
  only sends sparse progress messages (one per 1,000-block segment on
  protocol v3, none on a fully cached range), so its `msg/s` and `total`
  counters are not block counts and `last_block_seen` may lag or be `None`.
  Judge a warm-up by wall time, then confirm with
  `substreams run --production-mode -o clock` over the same range (cached
  delivery ≈ 3,500 blocks/s).
