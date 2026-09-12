# substreams-evm-state

Selective BSC account state using a native Substreams map and ClickHouse.
Reconstruct complete storage, code, balance and nonce at a fixed finalized block,
verify the result, and publish an immutable checkpoint for local execution.

**Prototype; v0.1.0 is not released yet.** Native ingestion, isolated checkpoint
construction, proof verification, portable exports/restores, reader pins,
cursor recovery and checkpoint/native-history cleanup are implemented and tested.
Initial replay supports private compaction between bounded native chunks. Capacity
monitoring covers data directories and temporary work. Cold/cached ingestion,
15-minute finalized follow and a real interrupted three-account cutover are now
measured; broader historical lifecycle and account-set qualification remain unfinished.
The customer's actual 19/64-account lists have not been supplied, so their
capacity, latency and cost are not qualified. See [scope](docs/SCOPE.md) and
[current evidence](docs/QUALIFICATION.md).

## Architecture

```text
BSC Extended blocks + explicit selected accounts
  └─ map_block_state: annotated protobuf, directly from the Block
       └─ native `substreams sink clickhouse`
            └─ state_blocks: one complete block per physical row
                 └─ verified immutable checkpoint + one ready manifest
```

This path has no `db_out` transformation. The native sink still writes the map
output to ClickHouse. `db_out` and the granular `map_state_changes` module remain
available; the [PostgreSQL baseline](docs/POSTGRES.md) is documented separately.

Each native row contains block number/hash/parent/state root and inline arrays of
storage, balance, nonce, code and lifecycle changes. The mapper keeps the final
value per changed key in each block. These arrays share one physical row, so a
reader cannot see half a block's account changes. Empty blocks also produce rows.

ClickHouse replacement is asynchronous. Reads deduplicate retries with `FINAL`,
then choose the greatest blockchain position, not ingestion time. Account fields
are resolved independently: a balance update cannot erase a known nonce or code.
Zero storage clears are applied **before** filtering the final nonzero state.

## Local setup

Requirements: Rust 1.88 with the WASM target (see `rust-toolchain.toml`),
Python 3.10+, Docker, and Substreams **1.22.0**. Native recovery tests additionally
use Go 1.26.5 for a local transport adapter. Linux/WSL and macOS are supported.

```bash
make python-deps
python3 scripts/install_substreams.py
export PATH="$PWD/localdata/toolchain/bin:$PATH"
export SUBSTREAMS_API_KEY=...  # use your environment; do not commit credentials

make ch-up
make build                   # builds WASM and spkg/evm-state-v0.1.0.spkg
make dev                     # guarded native ingestion of the 32-block sample
```

Finalized follow uses one-block decoding and a 100 ms spool idle threshold.
The pinned native sink continues spooling finalized blocks even at the chain
head; its `is_live=false` log field is not a lag indicator. Bounded bootstrap
keeps larger batches. See [measured throughput and operating settings](docs/THROUGHPUT.md).

The installer checks pinned archive hashes from the upstream v1.22.0 release.
ClickHouse is pinned to `26.3.33.24`. Local development ports are `18123` for HTTP
and `19000` for the native protocol, bound to loopback. Docker volumes retain data.

| Setting | Default |
|---|---|
| `CH_DATABASE` | `evm_native` |
| `CH_CHECKPOINT_DATABASE` | same as `CH_DATABASE`; immutable publication destination |
| `CH_STATE` | `localdata/evm_native` |
| `START_BLOCK` / `STOP_BLOCK` | `120140091` / `120140123` (stop is exclusive) |
| `ACCOUNTS` | sample contract, WBNB, EIP-2935 history contract |
| `ENDPOINT` | `bsc.substreams.pinax.network:443` |

**This short sample is an update interval, not a complete initial account state.**
Use the bootstrap workflow below to establish completeness.

The Python HTTP client accepts `CH_HTTP_URL`, `CH_USER`, and `CH_PASSWORD`.
The native CLI receives its connection through `SUBSTREAMS_SINK_DSN`; the Makefile
sets that from `CH_DSN`. Custom connections must point to the same host/database.

## Run identity and restarts

`make setup`, `make dev` and `make sink` use the guarded Python runner. It binds
the normalized account list, package checksum, module hash, endpoint, starting
block and database identity to a durable state directory and host. A database ownership
record and local process lock reject competing runs, including copied directories.
The native child inherits the lock, so it continues to exclude another writer if
its wrapper is killed. The package is copied into
that directory so an in-progress run cannot change when the repository is rebuilt.
Operate one runner on one host per source database. Do not copy a run directory
to another host and start a second writer; distributed writer leases are not implemented.

Keep the entire directory, including native schema metadata, cursor and spool,
on a persistent volume. Repeat the same command to resume. A different filter or
package requires a new database and state directory. Use the frozen package path
to resume an existing run after changing the repository package.

Missing/empty cursors, missing schema metadata and replaced databases fail closed.
Do not delete a cursor to force a replay over existing data. Restore matching
database and run metadata, or use `recover-cursor` with the matching run arguments
to repair a damaged cursor from `durable_progress.json`. The runner periodically
checks the cursor's finalized block/hash against both native data and block-marker
rows before saving that backup with atomic replacement and filesystem sync.
Recovery rechecks those rows and the run binding; an absent or mismatched backup
is insufficient. Preserve the spool during an interrupted run. A cleanly completed
run also saves `last_completed_cursor.txt`, but recovery uses the checked backup.
See the [recovery commands](docs/CHECKPOINTS.md#native-cursor-recovery).

Run identity format 3 binds the host/directory and a single checkpoint destination.
Set `CH_CHECKPOINT_DATABASE` (or `--checkpoint-database`) when publishing into a
separate database. Older prototype runs fail the new guard; preserve them and
export a verified checkpoint, then start a fresh guarded continuation from that
checkpoint in a new database/directory.

A bounded run succeeds only when its validated cursor reaches `STOP_BLOCK - 1`.
Repeating that completed range returns its checked position without replaying it.
Publication also requires durable progress covering the chosen checkpoint target.
Local tests exercise database-process SIGKILL and torn cursor recovery; they do
not emulate a physical host power failure. Durable storage must honor sync writes.

The mapper rejects empty account filters, non-Extended blocks, unsupported
producer versions, invalid block identity and incomplete transaction traces.
The native runner always requests finalized blocks. Other chains and
non-finalized rollback are outside this qualification.

## Verified bootstrap and onboarding

1. Choose explicit accounts and sufficient history for them. Creation-block
   replay is a candidate source; proof verification decides whether it was complete.
2. Capture a recent finalized header, account proofs and code **before** replay,
   while the RPC provider can still serve proofs at that block.
3. Replay into a fresh native database through the captured block, inclusive.
4. Build the checkpoint from that source interval. It is published only after
   storage, account metadata, bytecode and proofs all verify.

```bash
.venv/bin/evm-state capture-proofs \
  --accounts 0x98dd051fe7d43b2943b1245ca26e8c565dc5ffff \
  --output localdata/bootstrap/proofs.json
```

Set `STOP_BLOCK` to the captured `header.number + 1`, then ingest that account
from its known creation block `121114203`. Use a fresh cohort identity:

```bash
make dev CH_DATABASE=bootstrap_example CH_STATE=localdata/bootstrap/native \
  ACCOUNTS=0x98dd051fe7d43b2943b1245ca26e8c565dc5ffff \
  START_BLOCK=121114203 STOP_BLOCK=<captured-block-plus-one>
```

Create `localdata/bootstrap/sources.json` as an array of source records. Use the
database, accounts, start block and module hash recorded in the run's `run.json`:

```json
[
  {
    "database": "bootstrap_example",
    "start_block": 121114203,
    "accounts": ["0x98dd051fe7d43b2943b1245ca26e8c565dc5ffff"],
    "module_hash": "<hash-from-run.json>",
    "final_blocks_only": true
  }
]
```

The checkpoint destination is created when needed. It must match the destination
bound at source preparation. It may be the same database as its source;
checkpoint tables use separate names.

```bash
.venv/bin/evm-state --database bootstrap_example checkpoint \
  --proofs localdata/bootstrap/proofs.json \
  --sources localdata/bootstrap/sources.json \
  --work-dir localdata/bootstrap/verification \
  --output localdata/bootstrap/checkpoint.json

.venv/bin/evm-state --database bootstrap_example show <snapshot-id> \
  --address 0x98dd051fe7d43b2943b1245ca26e8c565dc5ffff
```

For subsequent checkpoints, pass `--base <snapshot-id>`. Existing accounts must
continue at exactly `base.header.number + 1`. Newly discovered accounts use a
separate source cohort and cursor. All cohorts must reach the same finalized
block/hash before publication; cohorts cannot overlap. Existing checkpoints
remain readable while onboarding or verification is in progress.

Publication checks each source against its database ownership record and local
prepared run, including the frozen package checksum, module hash, account filter,
schema metadata, host/directory and database UUID. The requested interval may
begin later than the run's original start, but cannot precede it. The manifest
records that checked provenance. An arbitrary `sources.json` cannot claim a
native run identity on its own.

The checkpoint manifest carries block identity, accounts, source records, counts,
state checksum and proof evidence. A row in `checkpoints` is the readiness signal;
raw source rows and failed candidate rows are not ready.

## Export, restore and reader retention

The [checkpoint operations guide](docs/CHECKPOINTS.md) covers pagination,
portable files, recovery, onboarding and cleanup. A portable export contains an
encoded header, account proofs, complete metadata/code and checksummed gzip
storage pages. `manifest.json` is written last, after all exported state verifies.

```bash
.venv/bin/evm-state --database bootstrap_example export <snapshot-id> \
  --output localdata/exports/checkpoint-1
.venv/bin/evm-state verify-export localdata/exports/checkpoint-1 \
  --expected-hash <independently-trusted-block-hash>
.venv/bin/evm-state --database restored_state import-export \
  localdata/exports/checkpoint-1 --expected-hash <independently-trusted-block-hash>
```

Offline verification requires no database, RPC connection or credentials. An
import verifies the files first, then verifies the stored candidate before
publishing a new snapshot ID. It preserves the original block and state checksum.

Use `pin`, `page` and `unpin` for multi-request reads. Pins persist until explicitly
released; `pins` lists them. `retention-plan` previews cleanup and
`prune-checkpoints` removes whole old or failed generations, preserving every pin,
the newest checkpoint for every account, and the requested recent generations.

All checkpoint clients for a database must run on the same host and share the
same durable `EVM_STATE_HOME` (default `localdata/control`). The database is bound
to that directory; a second controller cannot bypass existing reader pins.
Exports and in-progress builds exclude cleanup. Direct SQL readers must arrange
their own pin through this interface. Distributed reader/writer coordination is
not implemented. Older, unpartitioned prototype tables require export/import
into a fresh database before checkpoint cleanup can be enabled.

After publishing a verified checkpoint, `source-retention-plan` and `prune-source`
can remove covered native history in whole daily data/monthly marker partitions.
Stop that source's writer first. Cleanup preserves the cursor block, the requested
recent block window and the update interval after every retained checkpoint for
the source's accounts. Pinned older checkpoints can therefore retain more history.
See [native history cleanup](docs/CHECKPOINTS.md#native-history-cleanup).

For a new account cohort, `bootstrap-replay` runs bounded native chunks and folds
each completed interval into a private current-state generation before continuing.
It reclaims older history partitions without publishing unverified state. The
normal `checkpoint` command still requires complete storage/account proofs at the
final target. See [initial replay compaction](docs/CHECKPOINTS.md#initial-replay-compaction)
for the command, recovery behavior and capacity limits.

## Verification and limits

The verifier reconstructs the trie from **all nonzero slots**, verifies the
account proof against the header state root, and checks nonce, balance and
bytecode hash. Named validation slots alone cannot establish completeness.
Observed field mismatches fail verification; only unobserved bootstrap metadata
may be filled from proven account values.

Captured headers are RLP-encoded and hashed to bind the state root to the block
hash. By default, finality is trusted to the RPC provider. `capture-proofs
--expected-hash` binds the result to an operator-supplied hash; it does not verify
BSC consensus. Older prototype provider-trusted bundles without the encoded
header remain readable, but cannot claim operator-pinned hash verification.

The native mapper applies BSC's SELFDESTRUCT fork rules at transaction end,
clears inherited storage on deletion and preserves later recreation writes.
Failed EIP-7702 handling separates authorization changes from reverted execution
using the root-call boundary. Synthetic regression cases pass; the full captured
producer/fork/7702 matrix remains unqualified. See [lifecycle semantics and
evidence](docs/LIFECYCLE.md). A mismatch leaves the candidate unpublished.

The default checkpoint table budget is 100,000,000,000 bytes. Use
[`capacity-run`](docs/CAPACITY.md) to additionally measure whole ClickHouse data
directories, pending spool, temporary trie work and exports. It records periodic
and publication/trie guard samples, enforces configured operating headroom and
rejects untracked work directories. A capacity-stopped native run remains resumable.
It is a sampled guard, **not a hard filesystem quota**; shared server data is
included conservatively and excursions between samples remain possible.

Bootstrap compaction limits replay history to a chunk plus partition granularity
and private state generations. Complete current state, pinned checkpoints and
backups can still grow. The recorded synthetic and public BSC workloads fit the
100 GB target, but the customer's account set and sustained growth are not yet
qualified. See [capacity methodology](docs/CAPACITY.md) and [evidence](docs/QUALIFICATION.md).

## Tests and release

```bash
make test                  # Rust and offline Python tests
make test-integration      # local ClickHouse, published CLI, fault injection
```

The integration suite uses uniquely named disposable databases. It tests native
row encoding, direct/spooled restarts, a cursor-write failure after data insertion,
coherent checkpoint publication, zero clears, account onboarding, proof failures,
run-identity guards, export tampering, restore failures, pinned pagination and
interrupted checkpoint/native-history cleanup. A separate disposable ClickHouse
container is killed and restarted before and after publication to test database
recovery; the configured development database is never restarted by that test.
The suite makes no provider requests and needs no API keys.
The Go adapter only translates the native CLI's S2 gRPC compression for the local
Python test server; it is not part of the production data path.

CI builds the package from a clean checkout and runs the same local integration
suite. The first GitHub release will be **v0.1.0**, with the `.spkg` and detailed
qualification notes, once all four deliverables pass.
