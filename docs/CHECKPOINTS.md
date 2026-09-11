# Checkpoint operations

This prototype supports one checkpoint controller host per ClickHouse database.
Use the pinned dependencies and native CLI from the [README](../README.md).
The commands below operate on ready, immutable checkpoints; a candidate without
a row in `checkpoints` must never be served to consumers.

## Durable controller and readers

Choose an absolute `EVM_STATE_HOME` on persistent local storage before the first
checkpoint operation. All processes that build, read, export, import or prune
that database must use that same location on the same host:

```bash
export EVM_STATE_HOME=/absolute/persistent/path/evm-checkpoint-control
```

The controller binds the database's UUID to this host and directory. Back up the
whole controller directory with the matching database. Changing the variable,
losing metadata or copying it to a different host does not establish a new owner;
operations fail closed. Portable export/import into a fresh database is the
supported way to move a verified checkpoint to another host.

For a read spanning multiple requests, first protect the selected checkpoint:

```bash
.venv/bin/evm-state --database checkpoints pin <snapshot-id> --purpose local-executor
.venv/bin/evm-state --database checkpoints page <pin-id> --address <account> --limit 1000
.venv/bin/evm-state --database checkpoints page <pin-id> --address <account> \
  --limit 1000 --cursor '<next-cursor>'
.venv/bin/evm-state --database checkpoints unpin <pin-id>
```

Use the returned `pin_id`. Repeat with `next_cursor` until it is null; null marks
the end of that account's nonzero storage. Each page carries the same snapshot,
block/hash and account metadata. A cursor is valid only for its checkpoint and
account. Nonces and balances in page/account export metadata are exact decimal
strings, avoiding Node.js `Number` rounding. Slots and storage values are
32-byte hex strings. Block numbers in this BSC version are JSON integers.

Pins have no automatic expiry. A stopped or crashed reader retains its pin;
`evm-state --database checkpoints pins` lists it for explicit release once that
consumer no longer needs the checkpoint. New checkpoint construction can proceed
while older checkpoints are being read. Direct SQL access bypasses these locks;
SQL consumers must keep a pin for the full lifetime of their read.

## Portable export and offline verification

```bash
.venv/bin/evm-state --database checkpoints export <snapshot-id> \
  --output localdata/exports/checkpoint-1 --page-size 10000 \
  --work-dir localdata/verification

.venv/bin/evm-state verify-export localdata/exports/checkpoint-1 \
  --expected-hash <trusted-block-hash> --work-dir localdata/verification
```

An export directory contains:

| File | Contents |
|---|---|
| `accounts.json` | Complete code, nonce, balance, existence and storage-root metadata |
| `storage-NNNNNN.jsonl.gz` | Ordered nonzero slots; at most the chosen page size, one account per page |
| `manifest.json` | Format version, exact header, account proofs, counts, per-file checksums and logical state checksum |

The exporter holds a reader lock and verifies all written data before writing
`manifest.json` last. The directory must be new. An interrupted export is left
without a manifest and fails verification; retry into a new directory. Remove
an incomplete directory only after confirming its exporter has stopped.

Offline verification requires only the exported files and the Python tool. It
checks file structure and checksums, recomputes every complete storage root,
verifies account inclusion/non-inclusion and metadata/code against the state
root, and hashes the encoded header. Missing or altered state is rejected even
if someone rewrites the file checksums. An independently trusted
`--expected-hash` binds that result to the consumer's chosen block. Without it,
the export's recorded provider/producer header trust remains the starting point.
Neither mode independently verifies BSC consensus or finality.

Do not modify files while exporting, verifying, transferring or importing them.
The importer rechecks the stored candidate before publication, but portable
directories are not a mutable synchronization interface.

## Restore and continue

Verify and restore into a fresh checkpoint database:

```bash
.venv/bin/evm-state --database restored_checkpoints import-export \
  localdata/exports/checkpoint-1 --expected-hash <trusted-block-hash> \
  --work-dir localdata/verification --budget-bytes 100000000000
```

The command verifies the export before database writes and verifies the stored
candidate before publishing. It returns a **new snapshot ID** with the same
block/hash and logical state checksum. Save that ID as the next build's base.
Source run metadata and sink cursors are not imported from the donor database.

To advance it:

1. Capture a new finalized proof bundle for all accounts before replay, while the
   RPC can serve proofs at that block.
2. Start a fresh, guarded native source database at exactly
   `restored.header.number + 1`. Replay through the new captured target inclusive
   (`--stop-block` is target plus one). The frozen account filter must cover the
   existing accounts. Bind `--checkpoint-database restored_checkpoints` at source
   preparation, or set `CH_CHECKPOINT_DATABASE=restored_checkpoints` with Make.
3. Create `sources.json` from the new run's recorded account list, module hash,
   database and continuation start, as shown in the README. The publisher checks
   this against the database's native ownership row, prepared local run, frozen
   package and schema metadata. Keep those source directories accessible on this
   host; source data alone is insufficient to publish a new checkpoint.
4. Run `checkpoint` against `restored_checkpoints` with `--base <restored-id>`,
   the new proof bundle and source records. The first source block must point to
   the restored header hash; every block through the target must be present.
5. Publish the returned new snapshot ID to consumers only after the command
   succeeds. A failed candidate leaves the previous ready checkpoint intact.

For new-account onboarding, use a separate source cohort from enough history to
reconstruct the new accounts. Capture one proof bundle covering the entire
combined account set at a common target. Existing-account cohorts continue from
the base plus one; cohorts must not overlap. Build and verify the combined
checkpoint before consumers switch. A source package/filter change is a new
native run identity, not a reason to ignore module-hash mismatches.

Native identity format 3 binds the run's host, absolute directory and sole
checkpoint destination. All cohorts for one publication use that same destination.
Earlier
prototype identities cannot be used for new publication or resumed by the new
guard. Keep their existing ready checkpoint, export/import it if needed, and
continue in a fresh guarded source. Do not rewrite old ownership rows to force
an upgrade. The source reader lock is held through publication; source-history
cleanup must acquire it exclusively as well as excluding the native writer.

The real BSC export/restore/continuation exercise is recorded in
[qualification evidence](QUALIFICATION.md). Synthetic tests also change and
clear storage after a restore and inject failures before publication.

## Native cursor recovery

The runner periodically saves `durable_progress.json` with an atomic, synced write.
It decodes the pinned upstream cursor format and verifies its finalized block/hash
against both `state_blocks FINAL` and `_blocks_ FINAL`, binding the result to the
run ID, full run identity and database UUID. Checkpoint publication requires this
checked progress to cover its target. The cursor encoding is public obfuscation;
the cursor by itself is neither authentication nor proof of complete account state.

If `cursor.txt` is missing, empty or torn after an interrupted run, stop its native
writer and recover with the original immutable arguments and frozen package:

```bash
SUBSTREAMS_SINK_DSN='<native-source-dsn>' .venv/bin/evm-state --database <source-db> \
  recover-cursor --package <state-dir>/package.spkg --state-dir <state-dir> \
  --endpoint <original-endpoint> --accounts <original-account-list> \
  --start-block <original-start> --checkpoint-database <checkpoint-db>
```

The command rechecks database/run/schema identity and the backup's block data,
preserves the damaged file when present, then restores the checked cursor. Resume
the same `ingest` command afterward and retain its spool. Do not substitute a
copied `last_completed_cursor.txt` or invent a cursor to bypass validation. Without
a matching backup and database, restore matching metadata or start a fresh source
from a verified exported checkpoint.

Tests cover native direct/spooled process kills, a torn cursor, and a separate
ClickHouse server process killed before and after checkpoint publication. They
verify that incomplete candidates stay unpublished and a published checkpoint
survives restart. They do not emulate physical power loss; the storage system must
honor the configured filesystem sync operations.

## Checkpoint cleanup

```bash
.venv/bin/evm-state --database checkpoints retention-plan --keep-latest 2
.venv/bin/evm-state --database checkpoints prune-checkpoints --keep-latest 2
```

Both commands exclude active readers/exporters and publishers; they fail instead
of waiting behind a long operation. The plan lists generations to keep and remove.
Cleanup preserves every persistent pin, the latest checkpoint for each account,
and at least the requested number of recent checkpoints. It also removes failed,
unpublished candidate partitions. Therefore the retained count may exceed
`--keep-latest` when consumers are pinned or account sets differ.

Cleanup removes a generation's ready manifest before its state partitions. An
interruption leaves unpublished remnants that the next cleanup can collect.
ClickHouse can retain inactive parts until background cleanup, so logical
deletion is not a promise of immediate physical space reclamation. The reported
database byte totals include active and inactive parts.

Checkpoint cleanup requires tables partitioned by `snapshot_id`. Existing
unpartitioned prototype databases remain readable and exportable; migrate using
export/import into a fresh database before cleanup. A portable export also
requires a captured encoded header, which the earliest prototype bundles lack.

## Native history cleanup

Once a ready checkpoint covers every account of an exact native source, stop
that source writer and preview cleanup:

```bash
.venv/bin/evm-state --database <source-db> source-retention-plan <snapshot-id> \
  --state-dir <state-dir> --keep-blocks 10000
.venv/bin/evm-state --database <source-db> prune-source <snapshot-id> \
  --state-dir <state-dir> --keep-blocks 10000
```

The snapshot must be in the source's bound checkpoint database and its manifest
must record that exact run, package, schema, database UUID and account set. The
current cursor must match its checked durable backup and cover the checkpoint.
Cleanup excludes the native writer, source readers and checkpoint publishers.

It preserves the update interval after **every retained checkpoint** containing
any source account, including imported checkpoints and persistent pins. Rotate
unneeded checkpoints first if those intervals no longer need to be retained.
The native cursor block and at least `--keep-blocks` recent blocks are preserved.
Only whole daily `state_blocks` and monthly `_blocks_` partitions whose maximum
height precedes the cutoff are removed, so actual retained history can be larger
than the requested minimum. Interrupted cleanup can be repeated; it recomputes
the plan and verifies the cursor still has its data and marker afterward.

This operation bounds already-checkpointed history at partition granularity; it
does **not** bound the initial bootstrap before its first verified checkpoint.
Spool, exports, temporary verification files and peak merge space need separate
accounting. A proven 100 GB operating cap remains an open release gate.
