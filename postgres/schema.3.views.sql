-- Read views.

-- Non-zero slots only (what an RPC storage dump would return).
CREATE OR REPLACE VIEW storage_nonzero AS
SELECT address, slot, value, block_num, ordinal
FROM storage
WHERE value <> '0x0000000000000000000000000000000000000000000000000000000000000000';

-- Account head state joined with bytecode.
CREATE OR REPLACE VIEW account_state AS
SELECT a.address, a.balance, a.nonce, a.code_hash, '0x' || encode(c.code, 'hex') AS code, c.size AS code_size,
       a.block_num, a.balance_block_num, a.nonce_block_num, a.code_block_num
FROM accounts a
LEFT JOIN code c ON c.code_hash = a.code_hash;

-- Latest committed block (snapshot head).
CREATE OR REPLACE VIEW head AS
SELECT block_num, block_hash, timestamp, state_root
FROM blocks
ORDER BY block_num DESC
LIMIT 1;
