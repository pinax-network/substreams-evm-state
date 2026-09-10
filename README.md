# substreams-evm-state

Account-keyed **EVM state projection** for Firehose *Extended* blocks, sunk into
**PostgreSQL** with [`substreams-sink-sql`](https://github.com/streamingfast/substreams-sink-sql).

Given a list of accounts (or none = every account), it streams every
**persisted** storage / balance / nonce / code change of those accounts and
maintains their current state plus an event log, block by block, with reorg
handling delegated to the sink.

> Status: **prototype (stage 1)**. Verified on BSC against `bsc.rpc.pinax.network`
> (see [Verification](#verification)). Network defaults to `bsc`; any chain
> served as `sf.ethereum.type.v2.Block` Extended works.

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
| Initial state bootstrap | ❌ | this streams *changes*. State columns are `NULL` until first observed. Replaying from a contract's creation block reconstructs its full storage (see [Bootstrap](#bootstrap)) |

Filtering is on the **address of the changed account** in each state-change
record, never on call-to / `tx.to`. A filtered run is an exact subset of an
unfiltered one.

## Modules

```
sf.ethereum.type.v2.Block ─► map_state_changes ─► evm.state.v1.StateChanges ─► db_out ─► DatabaseChanges
                              (params: accounts)                                (Clock)
```

| Module | Output | Use |
|--------|--------|-----|
| `map_state_changes` | `evm.state.v1.StateChanges` | consume directly over gRPC; every record carries `ordinal`, `scope`, `tx_hash`, `tx_index`, `tx_status`, `call_index` |
| `db_out` | `sf.substreams.sink.database.v1.DatabaseChanges` | feed `substreams-sink-sql` (Postgres) |

### Params: the account filter

`map_state_changes` takes one string param: a comma (or whitespace) separated
list of 20-byte addresses, `0x` optional, case-insensitive. **Empty = all
accounts** (see [Sizing](#sizing) before doing that).

```bash
# CLI / sink flag
-p "map_state_changes=0x32c59d556b16db81dfc32525efb3cb257f7e493d,0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c"

# or in substreams.yaml
params:
  map_state_changes: "0x32c59d…,0xbb4cdb…"
```

Adding an account later: restart the sink with the new list from the block you
want it tracked from. Its state columns fill in as changes arrive (or replay
from its creation block, see [Bootstrap](#bootstrap)).

## PostgreSQL schema

`postgres/schema.*.sql` layers, concatenated by `make schema` into
`postgres/schema.sql` (the file the sink applies).

**Current state** (block-end values, last write by ordinal wins):

| Table | Key | Columns |
|-------|-----|---------|
| `accounts` | `address` | `balance` (wei), `nonce`, `code_hash`, `block_num`, `balance_block_num`, `nonce_block_num`, `code_block_num` |
| `storage` | `(address, slot)` | `value`, `block_num`, `ordinal` — slots cleared to zero keep a row with `0x00…00` |
| `code` | `code_hash` | `code` (hex), `size`, `first_block_num` — deduplicated bytecode |
| `blocks` | `block_num` | `block_hash`, `parent_hash`, `timestamp`, `state_root`, `coinbase`, `transaction_count`, per-type change counts |

**Event log** (one row per persisted change, keyed `(block_num, ordinal)`):
`storage_changes`, `balance_changes` (with `reason`), `nonce_changes`,
`code_changes`, `set_code_authorizations`. Each carries
`scope ∈ {tx, tx_failed_persistent, tx_7702, system_call, block}`, `tx_hash`,
`tx_index`, `tx_status`, `call_index`.

**Views**: `storage_nonzero`, `account_state` (accounts ⋈ code), `head`.

Encoding: `0x`-prefixed lower-case hex `TEXT` for addresses, hashes, slots,
values and bytecode; `NUMERIC` for wei; `BIGINT` for nonces and blocks.
Timestamps are `TIMESTAMP` (UTC).

## Quick start

Prerequisites: Rust 1.88 + `wasm32-unknown-unknown` (via `rust-toolchain.toml`),
[`substreams`](https://github.com/streamingfast/substreams/releases) CLI,
[`substreams-sink-sql`](https://github.com/streamingfast/substreams-sink-sql/releases)
≥ v4.12.0, Docker, `psql`.

```bash
# auth for the Pinax endpoint (never commit keys; see .env.example)
export SUBSTREAMS_API_KEY=...        # or SUBSTREAMS_API_TOKEN=...

make pg-up                            # local Postgres 16 on :5432 (docker compose)
make setup                            # build wasm, pack spkg, create tables
make dev                              # stream the default 32-block window into Postgres
make psql                             # poke around
```

Defaults (override on the command line):

| Var | Default | Meaning |
|-----|---------|---------|
| `ENDPOINT` | `bsc.substreams.pinax.network:443` | Substreams endpoint |
| `START_BLOCK` / `STOP_BLOCK` | `120140091` / `120140123` | block range for `make dev` |
| `ACCOUNTS` | sample contract + WBNB + EIP-2935 contract | params filter |
| `PG_DSN` | local docker DSN | Postgres |

```bash
make dev ACCOUNTS=0xabc…,0xdef… START_BLOCK=121114100 STOP_BLOCK=121114161
make sink START_BLOCK=121114100       # follow head, final blocks only
```

`make dev` uses `--development-mode --undo-buffer-size 0` and flushes every
block. `make sink` uses `--final-blocks-only --infinite-retry`. The sink
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
confirmed on BSC Firehose `ver=5` blocks):

1. **`SUCCEEDED` tx** — record every change of every call with `state_reverted == false`.
2. **`FAILED` / `REVERTED` tx** — consult only the root call. Keep balance
   changes with reason `GAS_BUY`, `GAS_REFUND`, `REWARD_TRANSACTION_FEE`
   (`scope = tx_failed_persistent`) and the smallest-ordinal nonce change (the
   sender). Everything else is dropped.
3. **EIP-7702** (`TRX_TYPE_SET_CODE`) — for each authorization with
   `discarded == false`, the `authority`'s nonce and code changes persist even
   when the tx fails (`scope = tx_7702`).
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

```bash
RPC_API_KEY=... python3 scripts/verify_rpc.py                 # whole DB at head
RPC_API_KEY=... python3 scripts/verify_rpc.py --address 0x… --block 120140122
```

Results on BSC (2026-09-10, `bsc.rpc.pinax.network`):

| Test | Blocks | Filter | Checked | Mismatches |
|------|--------|--------|---------|------------|
| Contracts | 120140091–120140122 | sample contract, WBNB, EIP-2935 | 392 slots, 1 balance, hash, state_root | 0 |
| EOAs, failed 7702 txs | 121114100–121114160 | 3 EOAs | 3 nonces, 2 balances, 1 code, 1 slot | 0 |
| Live follow | 120140123–120155883 (15,763 blocks) | 7702 bot EOA | nonce | 0 |

Failed-tx and 7702 paths were exercised on real data: txs
`0x506ed5…` (REVERTED) and `0x9929e0…` (FAILED) at blocks 121114122 / 121114153
produced exactly one `tx_failed_persistent` sender nonce and one `tx_7702`
authority nonce each, and nothing else.

Unit tests for the persistence rules: `make test`.

## Sizing

Measured on BSC (block time ≈ 0.75 s, ≈ 115k blocks/day):

| Run | Rows / block (event log) | DB growth |
|-----|--------------------------|-----------|
| Unfiltered, 10 blocks | ≈ 1,540 storage + 435 balance + 113 nonce | ≈ 2.3 MB/block ⇒ **≈ 250 GB/day** |
| 3 contracts (incl. WBNB), 32 blocks | ≈ 90 storage + 17 balance | small |
| Filtered live follow, per-block flush | — | ≈ 56 blocks/s throughput |

Unfiltered event logging is not viable for a bounded local budget; the
current-state tables alone are bounded by the number of live slots. If the
event log is not needed, drop layer `postgres/schema.2.events.sql` and the
corresponding rows in `src/db_out.rs` (or truncate on a schedule).

## Bootstrap

This package does not export initial state. Two options:

* **Replay from creation.** Run with `ACCOUNTS=<addr>` from the contract's
  creation block: every slot it ever wrote is reconstructed, so `storage_nonzero`
  is complete by construction. Completeness can be verified by recomputing the
  storage trie root from `storage_nonzero` and comparing to
  `eth_getProof(addr, [], block).storageHash` (not yet scripted).
* **External snapshot.** Load `accounts` / `storage` / `code` from another
  source at block *N*, then start the sink at *N+1*.

## Layout

```
substreams.yaml            # map_state_changes + db_out + sink (postgres)
proto/evm/state/v1/        # StateChanges proto
src/persist.rs             # persistence rules (unit-tested)
src/params.rs              # account filter
src/db_out.rs              # Tables projection
postgres/schema.*.sql      # numbered layers → schema.sql (generated)
scripts/verify_rpc.py      # RPC cross-check
docker-compose.yml         # local Postgres 16
docs/SCOPE.md              # scoping notes and open questions
```

## Known issues / follow-ups

* Do **not** import the `substreams-sink-sql-protodefs` spkg in `substreams.yaml`:
  current CLI builds embed those protos and the import causes
  `name conflict over sf.substreams.sink.sql.v1.Service` in `substreams run`,
  `gui` and `protogen`. The `sink:` section works without it.
* Bytecode is stored as hex `TEXT`; `BYTEA` would halve the size of `code`.
* Event-log tables are not partitioned yet; add `PARTITION BY RANGE (block_num)`
  before running unfiltered or long-lived.
* Storage-root completeness check against `eth_getProof` is not scripted yet.
