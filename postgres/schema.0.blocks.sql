-- One row per streamed block, including blocks with no matching changes.
-- Presence of a row = the block-end snapshot for all tracked accounts is
-- committed (the sink writes a block's rows and its cursor in one transaction).
CREATE TABLE IF NOT EXISTS blocks (
    block_num           BIGINT PRIMARY KEY,
    block_hash          TEXT NOT NULL,
    parent_hash         TEXT NOT NULL,
    timestamp           TIMESTAMP NOT NULL,
    state_root          TEXT NOT NULL,
    coinbase            TEXT NOT NULL,
    transaction_count   INTEGER NOT NULL,
    -- number of persisted, filter-matching changes in this block
    storage_changes     INTEGER NOT NULL DEFAULT 0,
    balance_changes     INTEGER NOT NULL DEFAULT 0,
    nonce_changes       INTEGER NOT NULL DEFAULT 0,
    code_changes        INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_blocks_hash ON blocks (block_hash);
CREATE INDEX IF NOT EXISTS idx_blocks_timestamp ON blocks (timestamp);
