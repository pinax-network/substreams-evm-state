# substreams-evm-state

Account-keyed **EVM state projection** for Firehose *Extended* blocks, sunk into
**PostgreSQL** with the `substreams sink postgres` command of the
[`substreams` CLI](https://github.com/streamingfast/substreams).

Given a list of accounts (or none = every account), it streams every
**persisted** storage / balance / nonce / code change of those accounts and
maintains their current state plus an event log, block by block, with reorg
handling delegated to the sink.

> Status: **prototype (stage 2)**. Verified on BSC against `bsc.rpc.pinax.network`
> (see [Verification](#verification)). Network defaults to `bsc`; any chain
> served as `sf.ethereum.type.v2.Block` Extended works. BSC Firehose is
> Extended from block 1 (`ver=3` historically, `ver=5` on current blocks).

## What it covers

| Change source | Included | Notes |
|---------------|----------|-------|
| Successful transactions | ✅ | every call with `state_reverted == false` |
| Failed / reverted transactions | ✅ | only what persists: `GAS_BUY`, `GAS_REFUND`, `REWARD_TRANSACTION_FEE` balance changes and the sender nonce |
| EIP-7702 `SET_CODE` transactions | ✅ | authority nonce + delegation code persist even when the tx fails; authorization list stored separately |
| `Block.system_calls` | ✅ | e.g. EIP-2935 history contract `0x0000f908…2935`, written every BSC block |
| `Block.balance_changes` / `Block.code_changes` | ✅ | validator fee rewards, fork upgrades |
| Empty / non-matching blocks | ✅ | a `blocks` row is written for every block, so cursor and snapshot continuity is preserved |
| Reorgs | ✅ (sink) | `--final-blocks-only`, or undo via the sink's `substreams_history` table |
| Initial state bootstrap | ◐ | this streams *changes*; state columns are `NULL` until first observed. Replaying from a contract's creation block reconstructs its full storage, verifiable with `scripts/verify_storage_root.py` (see [Bootstrap](#bootstrap)) |

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
hash, so the sink warns about a cursor hash mismatch on restart; run with
`--on-module-hash-mismatch=warn` (the Makefile does). Adding an account
later: restart with the new list from the block you want it tracked from, or
replay it from its creation block into the same database (see
[Bootstrap](#bootstrap)).

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

Partitions must exist before rows arrive. `make setup` creates them for
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

```bash
export SUBSTREAMS_API_KEY=...         # never commit keys; see .env.example

make pg-up                            # local Postgres 16 on :5432 (docker compose)
make setup                            # build wasm, pack spkg, create tables + partitions
make dev                              # stream the default 32-block window into Postgres
make psql                             # poke around
```

Defaults (override on the command line):

| Var | Default | Meaning |
|-----|---------|---------|
| `ENDPOINT` | `bsc.substreams.pinax.network:443` | Substreams endpoint |
| `START_BLOCK` / `STOP_BLOCK` | `120140091` / `120140123` | block range for `make dev` |
| `ACCOUNTS` | sample contract + WBNB + EIP-2935 contract | params filter |
| `PG_DSN` / `PG_URL` | local docker DSN | Postgres (sink DSN / psql URL) |
| `PARTITION_FROM` / `PARTITION_TO` / `PARTITION_STEP` | `120000000` / `130000000` / `1000000` | event-log partitions created by `make setup` |

```bash
make dev  ACCOUNTS=0xabc…,0xdef… START_BLOCK=121114100 STOP_BLOCK=121114161
make sink START_BLOCK=121114100       # follow head, final blocks only
make verify                           # RPC cross-check (RPC_API_KEY in env)
make verify-root ADDRESS=0x…          # storage trie root vs eth_getProof
```

`make dev` runs `substreams sink postgres … --development-mode --undo-buffer-size 0`
and flushes every block. `make sink` adds `--final-blocks-only`. The sink
resumes from the cursor stored in the `cursors` table; to re-run a different
range, use a fresh database (`make pg-down && make pg-up && make setup`).

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
   (`scope = tx_failed_persistent`) and the smallest-ordinal nonce change (the
   sender). Everything else is dropped.
3. **EIP-7702** (`TRX_TYPE_SET_CODE`) — for each authorization with
   `discarded == false`, the `authority`'s nonce and code changes persist even
   when the tx fails (`scope = tx_7702`). On BSC the root call of a failed
   set-code tx carries both the sender and the authority nonce; both are kept.
4. **`Block.system_calls`** — calls with `state_reverted == false` (`scope = system_call`).
5. **`Block.balance_changes`, `Block.code_changes`** — always (`scope = block`).
6. No-op records (`old == new`) are dropped.
7. Within a block, state tables receive the **last change by ordinal** per key;
   the event log keeps every change.

Every row is an upsert, so re-processing a block is idempotent.

## Verification

`scripts/verify_rpc.py` compares every `storage` slot and every `accounts`
field (balance, nonce, code) with `eth_getStorageAt` / `eth_getBalance` /
`eth_getTransactionCount` / `eth_getCode` at the DB head block, plus the block
hash and `state_root` against `eth_getBlockByNumber`.

`scripts/verify_storage_root.py 0x<addr>` recomputes the account's storage
trie root (secure Merkle Patricia Trie over `storage_nonzero`) and compares it
with `eth_getProof(addr, [], block).storageHash`. A match is a proof that the
`storage` table holds **every** non-zero slot of that account at that block.
Most RPC nodes only serve `eth_getProof` within a few hundred blocks of head,
so run it while the sink is live.

Results on BSC (2026-09-10, `bsc.rpc.pinax.network`):

| Test | Blocks | Filter | Checked | Mismatches |
|------|--------|--------|---------|------------|
| Contracts | 120140091–120140122 | sample contract, WBNB, EIP-2935 | 392 slots, 1 balance, hash, state_root | 0 |
| EOAs, failed 7702 txs | 121114100–121114160 | 3 EOAs + EIP-2935 | 62 slots, 3 nonces, 2 balances, 1 code (BYTEA), hash, state_root | 0 |
| Live follow | 120140123–120155883 (15,763 blocks) | 7702 bot EOA | nonce | 0 |
| Storage root | 121114203–121122203 (creation → head, 8,001 blocks) | `0x98dd05…5ffff` | 46-slot trie root vs `eth_getProof.storageHash`, nonce, code_hash | 0 |

Unit tests for the persistence rules: `make test`.

## Sizing

Measured on BSC (block time ≈ 0.75 s, ≈ 115k blocks/day):

| Run | Rows / block (event log) | DB growth |
|-----|--------------------------|-----------|
| Unfiltered, 10 blocks | ≈ 1,540 storage + 435 balance + 113 nonce | ≈ 2.3 MB/block ⇒ **≈ 250 GB/day** |
| 3 contracts (incl. WBNB), 32 blocks | ≈ 90 storage + 17 balance | small |
| Filtered live follow, per-block flush | — | ≈ 56 blocks/s throughput |

Unfiltered event logging is not viable for a bounded local budget; the
current-state tables alone are bounded by the number of live slots. Keep the
event log short with partition drops, or remove layer
`postgres/schema.2.events.sql` and the corresponding rows in `src/db_out.rs`.

## Throughput and cost

Measured 2026-09-10 on `bsc.substreams.pinax.network` with the default
3-account filter (sample contract, WBNB, EIP-2935 contract). WBNB is one of
the hottest contracts on BSC, so this is a pessimistic per-account profile.

| Measurement | Value |
|-------------|-------|
| `db_out` output | ≈ 27 KB/block (≈ 165 rows/block; event log ≈ 2/3 of the bytes, state tables ≈ 1/3) |
| Uncached backprocessing, 20 parallel workers, 10,000 blocks | 87 s ⇒ ≈ 115 blocks/s (≈ 10 blocks/s per worker, one 1,000-block segment per worker) |
| Uncached backprocessing, 100 workers, 50,000 blocks | ≈ 98 s ⇒ ≈ 500 blocks/s (only 50 segments to run) |
| Cached delivery of the same 50,000 blocks | 14 s ⇒ ≈ 3,500 blocks/s |
| Live follow, per-block flush | ≈ 56 blocks/s, well above the 2.2 blocks/s BSC produces |

Cache build (first backprocessing of a new params value) scales with
`workers × ≈10 blocks/s`. Full BSC history (≈ 121M blocks) is ≈ 34 h at 100
workers or ≈ 3 days at 50; a contract created in 2025 (block ≈ 47M+) is about
60 % of that; a contract created last week is seconds. The params value is
part of the module hash, so **each distinct account list is its own cache**.
Add accounts by replaying only the new ones from their creation blocks
(§ Bootstrap) instead of rebuilding the whole list.

Cost at Pinax list prices ($150/TB of module output + $1.75 per 1M blocks;
BSC ≈ 5.76M blocks/month at 0.45 s):

| Scenario | Output | Monthly |
|----------|--------|---------|
| This package, 3 accounts incl. WBNB, state + event log | ≈ 27 KB/block ⇒ ≈ 155 GB/month | ≈ $23 + $10 = **≈ $33** |
| Same, state tables only (drop the event log) | ≈ 9 KB/block ⇒ ≈ 52 GB/month | ≈ $8 + $10 = **≈ $18** |
| Raw Firehose Extended blocks with CombinedFilter (customer's probe: 1.7 MB/block) | ≈ 9.8 TB/month | **≈ $1,480** |
| One-time full-history cache build for the 3-account filter | ≈ 3.3 TB + 121M blocks | ≈ $490 + $212 ≈ **$700** |

Costs scale with the number and activity of tracked accounts, not with
chain size; a quiet contract adds almost nothing.

## Bootstrap

This package does not export initial state. Two options:

* **Replay from creation.** Run with `ACCOUNTS=<addr>` from the contract's
  creation block: every slot it ever wrote is reconstructed, so `storage_nonzero`
  is complete by construction. Prove it with `make verify-root ADDRESS=<addr>`
  while the sink is at head. Tested on BSC: contract
  `0x98dd051fe7d43b2943b1245ca26e8c565dc5ffff` (created at block 121114203)
  replayed 8,001 blocks to head in ≈ 30 s in production mode (50 parallel
  workers), 46 non-zero slots, recomputed root `0x979eec…180f` equal to the
  RPC `storageHash`. Older, hotter contracts (WBNB, 2020) mean tens of millions
  of blocks; measure before promising a turnaround time.
* **External snapshot.** Load `accounts` / `storage` / `code` from another
  source at block *N*, then start the sink at *N+1*.

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
```

## Sink modes and why `db_out` stays

`substreams sink postgres` auto-detects its mode from the output module type.
A `DatabaseChanges` output runs in *database-changes* mode (create / update /
upsert / delete, delta ops, undo via `substreams_history`). Any other protobuf
with `schema.table` / `schema.field` annotations runs in *relational
mappings* mode: tables inferred from the proto, bulk `COPY` loads, but
**insert-only**. The current-state tables (`accounts`, `storage`, `code`) are
upserts, so this package uses `db_out`. An event-log-only deployment could
annotate `evm.state.v1.StateChanges` and drop `db_out`.

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
