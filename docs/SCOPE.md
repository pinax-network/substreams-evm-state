# substreams-evm-state — scoping

Account-keyed EVM state projection (storage / balance / nonce / code) from the
Firehose **Extended** `sf.ethereum.type.v2.Block`, sunk into **PostgreSQL**
with `substreams-sink-sql` (`engine: postgres`).

Structured like [`substreams-sec`](https://github.com/pinax-network/substreams-sec):
one package, one `db_out` map emitting `DatabaseChanges`, numbered plain-SQL
schema layers concatenated by `make schema`, Makefile-driven `setup` / `dev`.

## 1. Problem statement (from the customer thread)

The customer (BSC, router/token/V4-hook local execution) needs:

| Need | Status |
|------|--------|
| Incremental updates projected by the **account actually changed**, not by call-to address | **This package** |
| System / lifecycle effects (block rewards, system calls, withdrawals) | This package |
| Persistent effects of **failed transactions** (gas, sender nonce) and **EIP-7702** (authorization nonce/code) | This package |
| Coherent **block-end snapshots** with empty-block and reorg continuity | This package (`blocks` row every block + sink cursor / undo) |
| Complete initial storage export at a fixed block hash | **Out of scope** for Substreams alone. Partial answer in §8 (replay-from-creation) |
| Retained local data ≤ 100–300 GB | Drives the schema choice in §4 (state tables primary, event log optional/partitioned) |

Pinax has already confirmed to the customer that CombinedFilter on call-to
addresses is insufficient and that the right approach is keying on the
state-change records themselves. This package is that recommendation, made
concrete.

## 2. Reference facts (verified against `sf/ethereum/type/v2/type.proto`)

Where state changes live in an Extended block:

| Location | Fields | Notes |
|----------|--------|-------|
| `Block.balance_changes` | `BalanceChange` | Block-level: mining/validator rewards, withdrawals, BSC system reward, fee reset |
| `Block.code_changes` | `CodeChange` | Block-level code changes (hard-fork upgrades, system contract deploys) |
| `Block.system_calls[]` | `Call` with `storage_changes`, `balance_changes`, `nonce_changes`, `code_changes` | System calls outside a tx (EIP-4788 beacon root, EIP-2935 history, chain-specific) |
| `TransactionTrace.calls[]` | same four repeated fields per `Call` | Per-call; `state_reverted`, `status_failed`, `status_reverted` flags |
| `TransactionTrace.set_code_authorizations[]` | `SetCodeAuthorization` (`authority`, `address`, `nonce`, `discarded`) | EIP-7702, only when `type == TRX_TYPE_SET_CODE` |

Record shapes:

- `StorageChange { address, key, old_value, new_value, ordinal }`
- `BalanceChange { address, old_value: BigInt?, new_value: BigInt?, reason, ordinal }` — `None` means 0
- `NonceChange   { address, old_value, new_value, ordinal }`
- `CodeChange    { address, old_hash, old_code, new_hash, new_code, ordinal }`

Ordinals are block-global and give exact execution order, **except** for
elements attached to a call with `state_reverted == true` (may be 0). Those
elements are discarded anyway (see §3), so last-write-wins by ordinal is safe.

Persistence rules stated by the proto (`TransactionTrace.status` docs and the
Block header docs):

- **SUCCEEDED** tx: record every change from every call where
  `state_reverted == false`.
- **FAILED / REVERTED** tx: look only at the **root call** (`calls[0]`).
  Keep balance changes whose reason is `GAS_BUY`, `GAS_REFUND`, or
  `REWARD_TRANSACTION_FEE`; keep the nonce change with the **smallest ordinal**.
  Drop everything else.
- **EIP-7702 SetCode tx** (any status): nonce + code changes for each
  **non-discarded** authorization are recorded in state even if the tx fails.
  The sender's nonce change appears first, without a code change.
- Skip no-op records (`old_value == new_value`).

## 3. Module design

Two modules, so the projection is reusable without Postgres (the customer
also consumes gRPC directly):

```
sf.ethereum.type.v2.Block ─► map_state_changes ─► evm.state.v1.StateChanges
                                                          │
                            sf.substreams.v1.Clock ──────►│
                                                          ▼
                                                        db_out ─► DatabaseChanges
```

### `map_state_changes` (params: string)

Applies the persistence rules from §2 and emits one flat, ordinal-sorted list
per change type. Every record carries provenance:

```
block_num, ordinal, tx_hash?, tx_index?, tx_status, call_index?, scope
scope ∈ { tx, tx_failed_persistent, tx_7702, system_call, block }
```

Params (comma-separated `key=value`, all optional):

| Param | Default | Purpose |
|-------|---------|---------|
| `accounts=0x..,0x..` | empty = all | Filter on the **record's own `address`** (never call-to). Lower-cased hex. |
| `include_code=true` | `true` | Emit full bytecode in `CodeChange`; `false` emits hashes only |
| `include_events=true` | `true` | Emit per-change rows (event log tables) in addition to state upserts |

Filtering happens **after** the persistence rules, so a filtered run is exactly
a subset of the unfiltered run — no semantic difference.

Even when nothing matches, the module returns a (possibly empty)
`StateChanges` so the block is still streamed and the sink still advances its
cursor and writes the `blocks` row. This is the empty-block continuity the
customer asked for.

### `db_out` (Clock + StateChanges → DatabaseChanges)

Same shape as `substreams-sec::db_out`: one `Tables`, `blocks` row every
block, then fan-out. Uses `substreams-database-change` **v4** so we get
`upsert_row` / `set` / `set_if_null` delta ops on Postgres (needs
`substreams-sink-sql ≥ v4.12.0`; local is a dev build newer than v4.13.1).

Within a block, the same `(address, key)` can change many times. `db_out`
collapses to the **last change by ordinal** per key before writing the state
row, so the state tables only ever see the block-end value. The event-log
tables keep every intermediate change.

## 4. Postgres schema (layers, `postgres/schema.*.sql`)

Encoding: `0x`-prefixed lower-case hex `TEXT` for addresses, hashes, slots
and 32-byte values (RPC-comparable, same as `substreams-evm`); `NUMERIC` for
wei balances; `BIGINT` for nonces and block numbers. Bytecode as `BYTEA`.

### Layer 0 — foundation

```sql
-- one row per block, including empty ones (continuity / snapshot marker)
blocks (
  block_num   BIGINT PRIMARY KEY,
  block_hash  TEXT NOT NULL,
  parent_hash TEXT NOT NULL,
  timestamp   TIMESTAMP NOT NULL,
  state_root  TEXT NOT NULL,          -- header.state_root, for the customer's own checks
  storage_changes INT, balance_changes INT, nonce_changes INT, code_changes INT
)
```

### Layer 1 — current state (the primary product, bounded size)

```sql
accounts (
  address     TEXT PRIMARY KEY,
  balance     NUMERIC,                 -- NULL = never seen a balance change
  nonce       BIGINT,
  code_hash   TEXT,                    -- NULL = never seen a code change
  block_num   BIGINT NOT NULL,         -- last block that touched any field
  balance_block_num BIGINT, nonce_block_num BIGINT, code_block_num BIGINT
)

storage (
  address     TEXT NOT NULL,
  slot        TEXT NOT NULL,           -- 32-byte key, hex
  value       TEXT NOT NULL,           -- 32-byte value, hex; zero rows are kept, see note
  block_num   BIGINT NOT NULL,
  ordinal     BIGINT NOT NULL,
  PRIMARY KEY (address, slot)
)

code (
  code_hash   TEXT PRIMARY KEY,
  code        BYTEA NOT NULL,
  first_block_num BIGINT NOT NULL      -- dedup: WBNB-style shared bytecode stored once
)
```

Note on zero values: a slot written to `0x00…00` is kept as a row with the
zero value rather than deleted. This preserves "we saw this slot cleared at
block N" for the customer's snapshot semantics; a view `storage_nonzero`
hides them. Deleting instead is a one-line change if they prefer.

### Layer 2 — event log (optional, `include_events`)

```sql
storage_changes (block_num, ordinal, address, slot, old_value, new_value,
                 tx_hash, tx_index, call_index, scope,   PRIMARY KEY (block_num, ordinal))
balance_changes (block_num, ordinal, address, old_value, new_value, reason,
                 tx_hash, tx_index, call_index, scope,   PRIMARY KEY (block_num, ordinal))
nonce_changes   (block_num, ordinal, address, old_value, new_value, ...)
code_changes    (block_num, ordinal, address, old_hash, new_hash, ...)
set_code_authorizations (block_num, tx_hash, auth_index, authority, delegate, nonce, discarded)
```

These are `PARTITION BY RANGE (block_num)` so retention is `DROP PARTITION`.
Sizing (from the customer's own probe, 19 accounts): 12,172 persisted storage
changes over 32 blocks ≈ 380 rows/block. At BSC's ~0.75 s blocks that is on
the order of 40 M `storage_changes` rows/day, i.e. several GB/day. **The
event log will not fit a 100–300 GB budget unfiltered for hot shared
contracts.** Default recommendation to the customer: state tables always on,
event log on with a short retention (hours–days) or off.

### Layer 3 — views

`storage_nonzero`, `account_state_at_head` (join `accounts` + `code`), and a
`snapshot_ready(block_num)` view: `true` when `blocks.block_num` exists, which
is the customer's "block-end snapshot is coherent" signal since the sink
commits a block's rows and its cursor in one transaction.

## 5. Reorg handling

`substreams-sink-sql` owns cursors and undo. Two supported operating modes,
chosen at run time not in the package:

| Mode | Flags | Trade-off |
|------|-------|-----------|
| Final only | `--final-blocks-only` | Simplest; BSC fast-finality lag is small. Recommended default for the customer. |
| Live + undo | `--undo-buffer-size N` (Postgres history table) | Lower latency; sink reverts rows on fork via its history log |

Because state rows are pure upserts keyed by `(address[, slot])`, undo through
the sink's history table restores the previous value correctly; no
package-side reorg logic is needed. The `blocks` row for the orphaned block is
removed by the same mechanism.

## 6. Verification plan

Against `bsc.rpc.pinax.network` at the same block (the customer already did
92 spot checks this way):

1. `eth_getStorageAt(address, slot, block)` vs `storage.value` for every
   `(address, slot)` touched in a sample range.
2. `eth_getBalance` / `eth_getTransactionCount` / `eth_getCode` vs `accounts`.
3. Failed-tx sample: pick reverted txs in range, assert sender nonce and
   gas-related balances moved and nothing else did.
4. EIP-7702 sample (BSC Pascal fork is live): assert `authority` nonce and
   delegation code (`0xef0100 || address`) rows exist for non-discarded auths.
5. Root check (see §8): recompute the storage trie root from all
   `storage_nonzero` rows for one contract and compare with
   `eth_getProof(address, [], block).storageHash`.

Item 5 is what makes "complete" a verifiable claim rather than an attestation,
which is the distinction the customer drew.

## 7. Repo layout and build

```
substreams-evm-state/
├── substreams.yaml            # map_state_changes + db_out + sink (postgres)
├── Cargo.toml / Makefile      # make schema | build | pack | setup | dev
├── proto/evm/state/v1/state.proto
├── src/
│   ├── lib.rs                 # handlers
│   ├── persist.rs             # §2 persistence rules (unit-tested)
│   ├── filter.rs              # params parsing + account filter
│   ├── db_out.rs              # Tables fan-out, last-write-by-ordinal collapse
│   └── pb/
├── postgres/
│   ├── schema.0.blocks.sql
│   ├── schema.1.state.sql
│   ├── schema.2.events.sql
│   ├── schema.3.views.sql
│   └── schema.sql             # generated (gitignored)
├── spkg/
└── docs/SCOPE.md
```

Dependencies (current as of 2026-09-10):

| Dep | Version |
|-----|---------|
| `substreams` | 0.7.x |
| `substreams-ethereum` | 0.11.1 |
| `substreams-database-change` | 4.0.0 (spkg `substreams-sink-database-changes-v4.0.0.spkg`) |
| `substreams-sink-sql` protodefs | v1.0.7 |
| `substreams-sink-sql` CLI | ≥ v4.12.0 for delta ops (latest release v4.13.1) |
| Rust toolchain | 1.88 + `wasm32-unknown-unknown` (matches `substreams-evm`) |

Endpoint for dev: `bsc.substreams.pinax.network:443`, `network: bsc`.
Test range: the customer's 120140091–120140122 window so numbers are directly
comparable to their probe (12,172 persisted storage changes, 19 accounts).

## 8. Bootstrap: what Substreams can and cannot do

Pinax has told the customer there is no scoped state-export product today.
That stays true. But this package gives a **partial, verifiable** bootstrap
path worth offering:

- For a contract created at block C, running `map_state_changes` with
  `accounts=<addr>` from C to head reconstructs **every** slot the contract
  has ever written. The resulting `storage_nonzero` set is complete by
  construction, and §6 item 5 verifies it against `eth_getProof.storageHash`.
- Cost is the Substreams backprocessing of the range (parallel on tier2),
  not RPC pagination. For 2020-era shared contracts (WBNB, PancakeSwap
  routers) that is tens of millions of blocks; needs a measured estimate
  before we quote it. Egress is small because the filtered module output is
  small; compute is the cost.
- Fits the customer's "repeatable initialization of newly discovered
  contracts" requirement: one filtered run per new address, promote to ready
  once the root check passes, then switch to the shared incremental stream.

What it does not solve: EOAs and contracts whose history predates the
Firehose Extended range on BSC, and any account where replay cost is
unacceptable. Those still need the Parquet/export roadmap.

## 9. Open questions — status after the stage-1 prototype (2026-09-10)

1. **BSC `system_calls` content.** Resolved. Every BSC block carries exactly one
   system call: `0xffff…fffe → 0x0000f90827f1c53a10cb7a02335b175320002935`
   (EIP-2935 history contract) writing one slot. Emitted with `scope = system_call`.
   Block-level `balance_changes` are two `REWARD_TRANSACTION_FEE` records per
   block (system address → coinbase). No block-level `code_changes` observed.
2. **Extended range on BSC.** Still open — ask infra for the first `ver=5` block.
3. **Zero-slot policy.** Implemented as *keep row with zero value*; `storage_nonzero` view hides them.
4. **Event log default.** Implemented as always-on (no partitioning yet). Unfiltered
   measured at ≈ 2.3 MB/block ⇒ ≈ 250 GB/day on BSC; filtered to a few contracts
   it is small. Decision on retention/partitioning deferred to the customer.
5. **Encoding.** Hex `TEXT` everywhere, including bytecode. `BYTEA` is a follow-up.
6. **7702 backfill note.** Observed on current BSC blocks that `SetCodeAuthorization.address`
   is populated. Code changes for the authority are recorded only when the
   delegate actually changes (a bot re-authorizing the same delegate produces
   nonce changes but no code change) — consistent with "no-op changes dropped".
7. **Failed 7702 txs.** Confirmed on real data: the root call carries the sender
   nonce *and* the authority nonce; the proto's "smallest ordinal" rule alone
   would drop the authority nonce. The mapper keeps both (`tx_failed_persistent`
   + `tx_7702`) and both verified against RPC.
8. **Sink resume.** `substreams-sink-sql` resumes from the `cursors` table even in
   development mode; a different block range needs a fresh database.

## 10. Milestones

| # | Deliverable | Verifies |
|---|-------------|----------|
| 1 | ✅ Package skeleton, proto, `map_state_changes` with persistence rules + unit tests | §2 rules |
| 2 | ✅ `db_out` + Postgres schema layers 0–3, `make setup`/`make dev` against the customer's 32-block window | WBNB storage count matches raw Firehose exactly |
| 3 | ✅ RPC cross-check script (`scripts/verify_rpc.py`, §6 items 1–4) | 0 mismatches |
| 4 | ◐ Event log layer 2 (no partitioning yet), sizing measured | Budget fit |
| 5 | Storage-root check (§6 item 5) on one contract via replay-from-creation | §8 bootstrap claim |
| 6 | README, publish spkg, hand the customer the params + run flags | Ship |
