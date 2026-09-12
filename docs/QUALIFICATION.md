# ClickHouse qualification evidence

Status: prototype, 2026-09-12. **Do not publish v0.1.0 yet.** This records current
evidence and preserves the four original deliverables; partial coverage does not
make an entire deliverable complete.

| Deliverable | Evidence in this prototype | Remaining acceptance work |
|---|---|---|
| Native finalized projection and coherent reads/restarts | One physical block envelope; generated native Nested schema; direct and spooled process-kill recovery; failure after block insertion before cursor write; frozen package/filter and database ownership guards; copied-directory/host rejection; inherited native writer lock; checked atomic cursor backup and explicit torn-cursor recovery; database-process SIGKILL before/after publication | Sustained follow and agreed read/publication latency; storage must honor sync writes (physical host power loss is not emulated) |
| Verified isolated bootstrap and onboarding | Real 180,090-block BSC replay; complete storage/account proof/code verification; immutable ready manifests; synthetic new-account catch-up; portable export and verified import; real restore followed by 9,427 BSC blocks and another 2,305-block continuation; 195,056-block replay with forced kill, cursor recovery and root verification; publication requires durable cursor coverage | Exercise representative multi-account onboarding and interrupted operational cutover |
| Completeness/lifecycle/proof tests | Wrong/missing proofs, missing/extra slots, bad metadata/code, wrong header commitments, gaps/forks/filter changes all fail; zero storage and proven non-inclusion pass; synthetic failed sender/self/multiple/discarded 7702 cases; BSC fork-aware SELFDESTRUCT and delegated execution; deletion/recreation across native patches and inherited checkpoint storage; legacy verifiers use consistent snapshots and fail on unknown/wrong metadata | Captured producer/fork fixtures and full 7702 matrix, historical CREATE/CREATE2 parity, representative lifecycle replay |
| Retention/cost qualification and release | Local sample disk/throughput evidence; early database-budget rejection; checkpoint/candidate cleanup preserving readers and latest account state; native history partition cleanup preserving every retained checkpoint's continuation and the durable cursor; initial replay compaction between bounded chunks; pinned toolchain; package-producing `make build`; CI integration job | Peak merge/spool/trie/export space accounting, representative hot/old/growing-account measurements, actual customer set when available, exact-head CI and v0.1.0 assets/notes |

## Reproducible local checks

`make test` runs the Rust tests and offline Python tests. With local ClickHouse
running, `make test-integration` also exercises the **published Substreams
1.22.0 binary**, not an emulated SQL writer. The tested release commit is
`be35ad36f63a52ff49d3e15cf993de4cad6bfbd9`; ClickHouse is `26.3.33.24`.

The Python suite currently contains 209 tests (81 offline, 126 ClickHouse
integration and two PostgreSQL integration).
Integration databases have random `evm_test_` names and are deleted afterward.
One fault test creates its own `evm-crash-` Docker container, kills/restarts that
database process and removes the container afterward. It never restarts the
configured development database. The 208-test baseline passed locally in 84.84
seconds, and all 19 Rust tests passed. A process-kill test now waits for the killed
native child to release its inherited writer lock before recovery; reaping its
wrapper alone was a timing race. The earlier 159-test lifecycle baseline also
passed on GitHub CI at `41bc101`.
The native fixtures serve real packaged protobuf types over local gRPC. The test
transport adapter translates the CLI's S2 request compression using its upstream
library. These tests cover sink behavior; they do not execute the WASM mapper.
Rust projection tests and the separately measured BSC replay cover that path.

CI installs the Python package as a regular wheel and checks its dependency
consistency before running the suite. A separate fresh virtual environment also
installed the wheel and verified the real 46-slot BSC export with provider and
database credentials removed. This exercises the packaged proof code without
relying on an editable checkout or database access.

Native failure tests prove:

- killing a process after committed data/cursor resumes at the saved cursor;
- both direct insertion and spooled backfill survive that restart;
- a failed cursor-file write after block data insertion can recover from the
  previous saved cursor without missing or partially published block state;
- retried native rows read correctly with `FINAL`, including empty blocks.
- a surviving native child retains the inherited writer lock after its wrapper
  is killed; copied directories and changed host identities cannot resume a run.
- checked progress decodes upstream Go-generated cursor vectors and binds the
  finalized block/hash to both native block data and marker rows;
- missing, empty or torn cursor files recover only from a matching atomic backup;
  wrong run/position/token, missing block data and missing markers fail closed;
- direct and spooled native replay can resume after process kill and explicit
  cursor repair, then publish a root-verified checkpoint;
- a hard database-process restart before publication leaves a collectible,
  unpublished candidate; restart after publication preserves the old snapshot,
  followed by repaired-cursor replay and a separately verified new snapshot.

These are process-crash and torn-file tests, not physical whole-host power-loss
tests. Acknowledged ClickHouse parts use filesystem sync settings and local
metadata uses synced atomic replacement; durable storage must honor those writes.

Publication rejects missing or changed source ownership, mismatched declared
filter/module/finality, changed package/schema metadata, replaced database identity,
unsupported producer versions and source ranges before the native run began.
It also rejects a different checkpoint destination or durable progress short of
the requested target. Format 3 binds a source to its sole publication database.
Synthetic checkpoint fixtures explicitly model these ownership records. Native
preparation and the real BSC continuation separately exercise actual package hashes.

The guarded `make dev` quick start was also run against BSC using the published
CLI. It ingested all 32 expected blocks (120140091–120140122), 1,293 storage
updates and 32 balance updates, recorded database ownership and saved its cursor.
This is a partial update sample, not a complete bootstrap of those three accounts.

Checkpoint failure tests prove that readers retain the old snapshot when a new
candidate has a gap, fork, parent mismatch, wrong filter/schema, invalid proof,
incorrect nonce/code, missing slot or unexpected account. Fault injection after
storage insertion and after account insertion leaves no ready candidate manifest.
Onboarding tests add a separate cohort at the same target and preserve the old
checkpoint, while balance-only changes leave nonce/code intact and zero clears
cannot resurrect old nonzero storage.

Export and retention tests additionally cover omitted/altered pages even after
file checksums are rewritten, code/nonce/proof/header corruption, exact uint64
nonce encoding for JSON consumers, path traversal, interrupted exports, changed
files during import and verification of the stored candidate before publication.
Pins survive newer publications, prevent checkpoint deletion between page calls,
and reject cursors for another account or checkpoint. Cleanup preserves the latest
state of quiet accounts and resumes removal of unpublished parts after interruption.
Missing or mismatched controller metadata fails closed. These locks are local to
one host and durable control directory, not a distributed coordination protocol.

Native retention tests span daily data and monthly block-marker partitions,
preserve a pinned older checkpoint and its continuation, build a newer checkpoint
after cleanup, then rotate the old checkpoint and reclaim more history. Active
source writers/readers exclude cleanup. A failure between data and marker
partition drops can resume without deleting the durable cursor's own rows.
Initial replay now has a separate private-compaction path. Twenty-one tests cover
repeated compaction and exact state-checksum parity with a full replay, zero
clears, deletion/recreation, proof rejection, damaged/missing prefixes, interrupted
pointer/partition writes, candidate-budget failure, broken suffixes, locks and
disjoint-cohort onboarding. A real native-sink test replays three bounded chunks,
resumes from compacted cursors and publishes only after final root verification.
These tests do not establish a peak 100 GB operating cap.

The legacy PostgreSQL diagnostics now use one consistent SQL snapshot and shared
account/storage proof verification. Twenty-seven offline regressions reject unknown
or mismatched metadata, incomplete storage, invalid proofs, inconsistent heads,
unsafe SQL inputs and misleading historical `--block` requests; query timeouts
never print database credentials. Two PostgreSQL
tests use disposable schemas: one verifies complete state and rejects a wrong
nonce, while the other checks coherent reads during concurrent committed updates.
The sampled RPC command labels its result as incomplete storage coverage; neither
legacy command publishes a ready checkpoint. The old custom trie implementation
and success-on-metadata-mismatch behavior have been removed.

The [lifecycle implementation and source references](LIFECYCLE.md) distinguish
account-wide deletion from code clearing and post-Cancun SELFDESTRUCT that keeps
storage. Checkpoint tests remove untouched inherited slots, preserve later
recreation and retain the old immutable checkpoint. Rust tests cover BSC's fork
boundary, delegated account context and failed authorization/execution separation.
These synthetic fixtures are not a substitute for producer-captured historical
qualification or customer execution parity.

Two captured BSC v5 failed/reverted EIP-7702 transactions now supplement the
synthetic cases, with immutable protobuf fixtures, block/transaction hashes,
checksums and RPC failure-status confirmation. A fresh native replay of the
61-block interval containing both transactions matched the observed sender
balance/nonce and authority nonce against RPC at block 121114160. See the
[parity evidence](evidence/bsc-lifecycle-parity-2026-09-12.json). Neither captured
transaction changed code; this does not complete the broader authorization or
historical lifecycle matrix, and the parity check is not a complete storage proof.

## BSC evidence

The [compacted bootstrap record](evidence/bsc-compacted-bootstrap-2026-09-12.json)
replays the quiet public account below from creation through **121309258**:
195,056 blocks in four native chunks of at most 50,000 blocks. Each chunk's
private state retained 46 nonzero slots. The final checkpoint independently
verified all storage, account metadata/code and the account proof, with the exact
same state checksum as the earlier uncompacted replay at that block.

Replay plus four compactions took 182.37 seconds; final checkpoint verification
took 0.135 seconds. Cache warmth was not established, so this is not a cold or
sustained-live throughput claim. The first compaction removed 48,092 old daily
block envelopes, reducing sampled source parts from 14,725,118 to 4,297,811 bytes.
Later chunks remained in the same daily partition and retained 146,964 native
rows at the end. Final source parts were 74,597,702 bytes, including inactive
parts at that instant; checkpoint parts were 17,273 bytes. These measurements
illustrate partition granularity and background merge variability, not a hot
contract bound or total peak disk usage. No ready state existed before the final
proof verification.

The [machine-readable record](evidence/bsc-bootstrap-2026-09-11.json) describes
the small public contract `0x98dd051fe7d43b2943b1245ca26e8c565dc5ffff` replayed
from creation block **121114203** through **121294292**, inclusive:

- 180,090 contiguous block envelopes; 46 nonzero storage slots at the target;
- every account field and bytecode verified against its proven account leaf;
- all nonzero storage reconstructs the proven storage root;
- the encoded header hashes to
  `0xd0987f468e3b66fa2593eb2df41924fed5fd563c697828916416de436784bd90`;
- checkpoint state checksum
  `388627901da595ca5d1719fbfe5263674aaa6b94c68aa5544680518ef45ef330`;
- source table parts occupied 52,398,130 bytes at the recorded measurement;
  a repeated checkpoint verification took 1.848 seconds on this machine.

The replay used a local build of the v1.22.0 source before the run-ownership
wrapper was added. The checkpoint was reverified after adding encoded-header
verification. The immutable header fixture is in
`tests/fixtures/bsc-121294292-header.json`.

This is a quiet, small account. It does **not** qualify WBNB, the customer's
19/64-account set, all contract lifecycles, peak disk usage or sustained live
operation. Header finality is trusted to the RPC provider, not independently
verified BSC consensus. The cache state of the original replay was not separately
established, so its duration is not labeled cold-build or cached throughput.

The [portable checkpoint record](evidence/bsc-portable-checkpoint-2026-09-11.json)
then exercises this same account's export, restore and continuation:

- The checkpoint at 121294292 exported to three storage pages and 15,857 total
  bytes; offline header/account/storage/code verification passed.
- Import into a fresh partitioned database preserved all 46 slots and the exact
  state checksum, verifying the stored candidate before publishing it.
- The published Substreams 1.22.0 binary ingested 9,427 contiguous blocks from
  121294293 through 121303719 in 36.035 seconds. Cache warmth is unknown.
- A new checkpoint verified in 0.331 seconds. Its encoded header hash is
  `0x1f32bf3f8690dd58014f00465350e5cfae5a25afd093188b9681cdac6fa0fb4f`.
- The older checkpoint remained pinned across the new publication and cleanup;
  its 46 slots could still be paginated consistently. After unpinning, cleanup
  removed it and preserved the newer checkpoint.

No selected-account state changes occurred in this continuation interval. It
qualifies empty-block continuity and restored-base preservation on BSC; changed
slots and zero clears across restore are exercised by synthetic integration tests.
These small-sample times and file sizes do not establish customer capacity or SLA.

The [bound-source continuation record](evidence/bsc-bound-source-2026-09-11.json)
advances that checkpoint by another 2,305 blocks, through 121306024, using native
identity format 2. The checkpoint verified in 0.272 seconds with the same 46 slots
and state checksum. Its manifest records the verified native run ID, database UUID,
module/package hashes and schema metadata. No selected state changed in this
interval either; its 50.217-second ingestion time includes streaming overhead and
is not a cold-cache or sustained-live measurement.

The [cursor-recovery record](evidence/bsc-cursor-recovery-2026-09-11.json) replays
195,056 blocks from 121114203 through 121309258 using destination-bound format 3:

- The native process group was killed after checked progress at 121163736. Its
  cursor file was deliberately truncated, restored from the validated backup,
  and replay resumed with the published CLI.
- The final checkpoint verified all 46 slots and account metadata/code with the
  same logical state checksum. Its encoded header hashes to
  `0xfe2d9b1b55c1220b94909c25c0bfe33992248763940daa4a27d608b241b89d28`.
- Replay including recovery took 37.104 seconds; checkpoint verification took
  1.927 seconds. Overlapping history had already been processed; cache warmth was
  not independently quantified. These are not cold-build or SLA measurements.
- Native cleanup removed the complete 2026-09-10 data partition (48,092 block
  envelopes), preserving the cursor and continuation intervals for all three
  retained checkpoints. No monthly marker partition was eligible for removal.
- Source database parts measured 70,959,941 bytes before cleanup and 60,529,347
  afterward, including inactive parts at measurement time. Logical partition
  removal does not promise immediate physical reclamation.

This is still the same small, quiet account and does not establish a hot/shared
contract bound, initial-history space cap or customer-set cost.

The local test transport adapter uses gRPC-Go 1.83.2, incorporating the upstream
fixes for the three dependency advisories reported against its earlier 1.83.0 pin.
It remains outside the production data path.

## Inputs and release gates

The customer questions/proposal have been drafted, not sent. Their account list,
deployment/consumer interface, latency and onboarding targets, firm retained-data
cap, retention window and header trust requirements are still unknown. Continue
independent local qualification without substituting sample results for theirs.

`make build` must produce the final `.spkg`, and the first GitHub release must be
`v0.1.0` with detailed notes and package asset. Release remains gated by the open
acceptance items above; do not call the goal complete based only on these tests.
