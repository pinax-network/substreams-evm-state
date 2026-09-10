-- Current state of tracked accounts (block-end values, last write by ordinal).
-- Columns are NULL until the first change of that kind is observed: this
-- package streams *changes*, it does not bootstrap initial state.

CREATE TABLE IF NOT EXISTS accounts (
    address             TEXT PRIMARY KEY,
    balance             NUMERIC,            -- wei
    nonce               BIGINT,
    code_hash           TEXT,               -- FK-ish into code.code_hash
    block_num           BIGINT NOT NULL,    -- last block touching any field
    balance_block_num   BIGINT,
    nonce_block_num     BIGINT,
    code_block_num      BIGINT
);
CREATE INDEX IF NOT EXISTS idx_accounts_block_num ON accounts (block_num);
CREATE INDEX IF NOT EXISTS idx_accounts_code_hash ON accounts (code_hash);

-- One row per (account, slot) ever written. A slot cleared to zero keeps its
-- row with value 0x00..00 (see view storage_nonzero).
CREATE TABLE IF NOT EXISTS storage (
    address             TEXT NOT NULL,
    slot                TEXT NOT NULL,      -- 32-byte hex
    value               TEXT NOT NULL,      -- 32-byte hex
    block_num           BIGINT NOT NULL,
    ordinal             BIGINT NOT NULL,
    PRIMARY KEY (address, slot)
);
CREATE INDEX IF NOT EXISTS idx_storage_block_num ON storage (block_num);

-- Deduplicated bytecode.
CREATE TABLE IF NOT EXISTS code (
    code_hash           TEXT PRIMARY KEY,
    code                BYTEA NOT NULL,     -- raw bytecode; encode(code, 'hex') for text
    size                INTEGER NOT NULL,
    first_block_num     BIGINT NOT NULL
);
