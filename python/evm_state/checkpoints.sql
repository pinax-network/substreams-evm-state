-- Immutable candidate snapshots are invisible until their manifest is inserted.
-- A reader pins a snapshot_id. It never joins mutable "latest" fields separately.
CREATE TABLE IF NOT EXISTS checkpoint_storage (
    snapshot_id String, address String, slot String, value String
) ENGINE = ReplacingMergeTree ORDER BY (snapshot_id, address, slot)
SETTINGS fsync_after_insert=1, fsync_part_directory=1;

CREATE TABLE IF NOT EXISTS checkpoint_accounts (
    snapshot_id String, address String, exists Bool, nonce UInt64, balance String,
    code_hash String, code String, storage_root String, nonzero_slots UInt64
) ENGINE = ReplacingMergeTree ORDER BY (snapshot_id, address)
SETTINGS fsync_after_insert=1, fsync_part_directory=1;

CREATE TABLE IF NOT EXISTS checkpoints (
    snapshot_id String, block_number UInt64, block_hash String, created_at UInt64,
    manifest String
) ENGINE = ReplacingMergeTree ORDER BY snapshot_id
SETTINGS fsync_after_insert=1, fsync_part_directory=1;

CREATE VIEW IF NOT EXISTS ready_checkpoints AS
SELECT snapshot_id, block_number, block_hash, created_at, manifest
FROM checkpoints FINAL ORDER BY block_number DESC, created_at DESC;
