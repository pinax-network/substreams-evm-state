# BSC persistence and account lifecycle

This describes the native `map_block_state` path and its checkpoint consumer.
The granular `map_state_changes` and PostgreSQL output share transaction-change
selection, but do not emit the native account-wide `storage_reset` marker.
The release qualification remains in [QUALIFICATION.md](QUALIFICATION.md).

## Failed transactions and EIP-7702

The sender nonce is selected by transaction sender, old transaction nonce and
the increment to nonce plus one, with a nonzero execution ordinal. Picking the
smallest root-call ordinal across all accounts is unsafe: reverted records can
have ordinal zero. Gas purchase, refund and transaction-fee balance changes keep
their existing persistence rules.

Accepted EIP-7702 authorization nonce/code changes occur before root-call
execution. A failed transaction may later change the same authority's nonce
again while executing delegated code, so authority address membership alone
does not imply persistence. The collector retains accepted authority changes
with `0 < ordinal < root.begin_ordinal`. It emits the sender increment only once
when the sender also authorizes delegation. Missing root execution boundaries
on failed transactions with accepted authorizations fail closed.

Evidence:

- The [upstream protobuf semantics](https://github.com/streamingfast/firehose-ethereum/blob/develop/proto/sf/ethereum/type/v2/type.proto)
  document persistent sender/authorization changes and unreliable ordinals on
  reverted records.
- The pinned [BSC Reth inspector](https://github.com/streamingfast/bnb-reth/blob/f7e29525db3235e202a4f4caa3440e90de0906bc/crates/firehose/src/inspector.rs#L618)
  records the sender increment, then authorizations before root call entry. Its
  authorization replay also handles repeated authorities and sender delegation.
- The [Reth-BSC producer snapshot](https://github.com/streamingfast/battlefield-ethereum/blob/b4dde9af73c6e5e2857994b788f2ecca70a32c40/test/snapshots/prague/fh3.0/v5/reth-bsc-dev/setcode_set_delegations.expected.json)
  shows those pre-execution ordinal groups. The upstream snapshot normalizes
  addresses and some numeric values; it is ordering evidence, not an unmodified
  mainnet account proof or end-to-end execution fixture.

Rust regression tests cover sender selection with reverted zero ordinals, self
authorization, repeated authority changes, discarded authorizations, explicit
delegation clearing, later reverted execution and missing boundaries. Two
unmodified BSC v5 TransactionTrace messages are also committed as
[fixtures with provenance](../tests/fixtures/bsc-failed-setcode.json): one REVERTED
transaction at 121114122 and one FAILED transaction at 121114153. Both retain
sender and accepted-authority nonce changes before root execution; neither
changes code. Their block hashes and failure status were cross-checked with RPC.
The broader captured self/multiple/discarded/code-clear matrix is still required.

The rebuilt native mapper also replayed 121114100–121114160 for their sender
and authority. All 61 block identities/parents were checked, and observed sender
balance/nonce plus authority nonce matched archive RPC at 121114160. This
[parity record](evidence/bsc-lifecycle-parity-2026-09-12.json) qualifies those
observed fields; it is not a complete-account bootstrap or root proof.

## SELFDESTRUCT and recreation

The native module targets BSC mainnet. Its Cancun/Haber activation timestamp is
`1718863500`, from [BSC chain configuration](https://github.com/bnb-chain/bsc/blob/c5533ab5b7244dc474add10740834417a2c605d7/params/config.go#L214).
[EIP-6780](https://eips.ethereum.org/EIPS/eip-6780) determines deletion:

- Before activation, a committed SELFDESTRUCT deletes the account.
- At/after activation, deletion happens only when the executing account was
  created in the same transaction. Earlier creation in the same block is not
  sufficient. The model represents both CREATE and CREATE2 using `CallType.CREATE`.
- A failed transaction or reverted call does not schedule deletion.
- DELEGATECALL/CALLCODE execute in their parent's account context. The target
  code address must not be mistaken for the account being deleted. Missing or
  cyclic ancestry is rejected.

Deletion takes effect at the transaction-end ordinal, after any later calls in
that transaction. The mapper emits `storage_reset` at that position and sets
balance, nonce and code to zero/empty unless a later transaction already supplies
a newer value. It removes earlier storage patches in that block. Writes from a
later recreation survive.

The checkpoint consumer excludes all slot versions at or before the latest
`storage_reset` for that account, including untouched slots from a base checkpoint.
It then resolves newer writes and zero clears before verifying complete state
against the target proof. `selfdestruct`, `nonce_reset` and `code_cleared` alone
are diagnostic signals, not permission to erase storage; in particular, clearing
an EIP-7702 delegation does not delete the account's storage.

The [Reth-BSC creation/destruction snapshot](https://github.com/streamingfast/battlefield-ethereum/blob/b4dde9af73c6e5e2857994b788f2ecca70a32c40/test/snapshots/suicide/fh3.0/v5/reth-bsc-dev/create_contract_to_fixed_address_kill_it.expected.json)
shows a slot written during execution and nonce/code cleanup after root-call exit
without a corresponding per-slot clear. Merely replaying storage-change records
therefore leaves stale storage. This shape informed the regression scenarios;
the tests use synthetic state with real account/storage trie proofs.

Current tests exercise the fork boundary with producer version fields 3, 4 and 5,
delegated execution, same-block recreation, previous-transaction creation and
reverted destruction. Version-field permutations do not establish full historical
producer coverage. SELFDESTRUCT in system execution is explicitly unsupported
until its execution boundary is qualified. Full producer fixtures, historical
parity and representative-account replay remain release requirements.
