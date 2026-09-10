-- Event log: one row per persisted change. Keyed by (block_num, ordinal);
-- ordinals are unique within a block. `scope` says where the change came
-- from: tx | tx_failed_persistent | tx_7702 | system_call | block.
--
-- All event tables are PARTITION BY RANGE (block_num). Partitions must exist
-- before rows arrive: run `SELECT create_event_partitions(from, to, step)`
-- (make partitions) ahead of the range you sink, and DROP old partitions for
-- retention. A DEFAULT partition catches anything outside created ranges so
-- the sink never fails; move/drop it before creating a range it overlaps.

CREATE TABLE IF NOT EXISTS storage_changes (
    block_num           BIGINT NOT NULL,
    ordinal             BIGINT NOT NULL,
    block_hash          TEXT NOT NULL,
    timestamp           TIMESTAMP NOT NULL,
    address             TEXT NOT NULL,
    slot                TEXT NOT NULL,
    old_value           TEXT NOT NULL,
    new_value           TEXT NOT NULL,
    scope               TEXT NOT NULL,
    tx_hash             TEXT NOT NULL,      -- '' for system_call / block scope
    tx_index            INTEGER NOT NULL,
    tx_status           INTEGER NOT NULL,   -- sf.ethereum.type.v2.TransactionTraceStatus
    call_index          INTEGER NOT NULL,
    PRIMARY KEY (block_num, ordinal)
) PARTITION BY RANGE (block_num);
CREATE INDEX IF NOT EXISTS idx_storage_changes_address_slot ON storage_changes (address, slot, block_num);
CREATE INDEX IF NOT EXISTS idx_storage_changes_tx_hash ON storage_changes (tx_hash);
CREATE TABLE IF NOT EXISTS storage_changes_default PARTITION OF storage_changes DEFAULT;

CREATE TABLE IF NOT EXISTS balance_changes (
    block_num           BIGINT NOT NULL,
    ordinal             BIGINT NOT NULL,
    block_hash          TEXT NOT NULL,
    timestamp           TIMESTAMP NOT NULL,
    address             TEXT NOT NULL,
    old_value           NUMERIC NOT NULL,
    new_value           NUMERIC NOT NULL,
    reason              INTEGER NOT NULL,   -- sf.ethereum.type.v2.BalanceChange.Reason
    scope               TEXT NOT NULL,
    tx_hash             TEXT NOT NULL,
    tx_index            INTEGER NOT NULL,
    tx_status           INTEGER NOT NULL,
    call_index          INTEGER NOT NULL,
    PRIMARY KEY (block_num, ordinal)
) PARTITION BY RANGE (block_num);
CREATE INDEX IF NOT EXISTS idx_balance_changes_address ON balance_changes (address, block_num);
CREATE INDEX IF NOT EXISTS idx_balance_changes_tx_hash ON balance_changes (tx_hash);
CREATE TABLE IF NOT EXISTS balance_changes_default PARTITION OF balance_changes DEFAULT;

CREATE TABLE IF NOT EXISTS nonce_changes (
    block_num           BIGINT NOT NULL,
    ordinal             BIGINT NOT NULL,
    block_hash          TEXT NOT NULL,
    timestamp           TIMESTAMP NOT NULL,
    address             TEXT NOT NULL,
    old_value           BIGINT NOT NULL,
    new_value           BIGINT NOT NULL,
    scope               TEXT NOT NULL,
    tx_hash             TEXT NOT NULL,
    tx_index            INTEGER NOT NULL,
    tx_status           INTEGER NOT NULL,
    call_index          INTEGER NOT NULL,
    PRIMARY KEY (block_num, ordinal)
) PARTITION BY RANGE (block_num);
CREATE INDEX IF NOT EXISTS idx_nonce_changes_address ON nonce_changes (address, block_num);
CREATE TABLE IF NOT EXISTS nonce_changes_default PARTITION OF nonce_changes DEFAULT;

CREATE TABLE IF NOT EXISTS code_changes (
    block_num           BIGINT NOT NULL,
    ordinal             BIGINT NOT NULL,
    block_hash          TEXT NOT NULL,
    timestamp           TIMESTAMP NOT NULL,
    address             TEXT NOT NULL,
    old_hash            TEXT NOT NULL,
    new_hash            TEXT NOT NULL,      -- bytecode in code.code_hash
    scope               TEXT NOT NULL,
    tx_hash             TEXT NOT NULL,
    tx_index            INTEGER NOT NULL,
    tx_status           INTEGER NOT NULL,
    call_index          INTEGER NOT NULL,
    PRIMARY KEY (block_num, ordinal)
) PARTITION BY RANGE (block_num);
CREATE INDEX IF NOT EXISTS idx_code_changes_address ON code_changes (address, block_num);
CREATE TABLE IF NOT EXISTS code_changes_default PARTITION OF code_changes DEFAULT;

-- EIP-7702 authorization lists (informational; persisted effects are in
-- nonce_changes / code_changes with scope tx or tx_7702).
CREATE TABLE IF NOT EXISTS set_code_authorizations (
    block_num           BIGINT NOT NULL,
    tx_hash             TEXT NOT NULL,
    auth_index          INTEGER NOT NULL,
    block_hash          TEXT NOT NULL,
    timestamp           TIMESTAMP NOT NULL,
    tx_index            INTEGER NOT NULL,
    tx_status           INTEGER NOT NULL,
    authority           TEXT NOT NULL,      -- '0x' when not recoverable
    delegate            TEXT NOT NULL,      -- '0x' on early BSC blocks (pending Firehose backfill)
    nonce               BIGINT NOT NULL,
    discarded           BOOLEAN NOT NULL,
    PRIMARY KEY (block_num, tx_hash, auth_index)
) PARTITION BY RANGE (block_num);
CREATE INDEX IF NOT EXISTS idx_set_code_authorizations_authority ON set_code_authorizations (authority, block_num);
CREATE TABLE IF NOT EXISTS set_code_authorizations_default PARTITION OF set_code_authorizations DEFAULT;

-- Create range partitions <table>_p<from> of `step` blocks covering
-- [from_block, to_block) for every event table. Idempotent.
CREATE OR REPLACE FUNCTION create_event_partitions(from_block BIGINT, to_block BIGINT, step BIGINT)
RETURNS INTEGER LANGUAGE plpgsql AS $$
DECLARE
    t TEXT; lo BIGINT; created INTEGER := 0;
BEGIN
    FOREACH t IN ARRAY ARRAY['storage_changes','balance_changes','nonce_changes','code_changes','set_code_authorizations'] LOOP
        lo := from_block - (from_block % step);
        WHILE lo < to_block LOOP
            IF to_regclass(format('%I_p%s', t, lo)) IS NULL THEN
                EXECUTE format('CREATE TABLE %I_p%s PARTITION OF %I FOR VALUES FROM (%s) TO (%s)', t, lo, t, lo, lo + step);
                created := created + 1;
            END IF;
            lo := lo + step;
        END LOOP;
    END LOOP;
    RETURN created;
END $$;
