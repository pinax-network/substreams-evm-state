# ClickHouse qualification evidence

Status: prototype, 2026-09-11. **Do not publish v0.1.0 yet.** This records current
evidence and preserves the four original deliverables; partial coverage does not
make an entire deliverable complete.

| Deliverable | Evidence in this prototype | Remaining acceptance work |
|---|---|---|
| Native finalized projection and coherent reads/restarts | One physical block envelope; generated native Nested schema; direct and spooled process-kill recovery; failure after block insertion before cursor write; frozen package/filter and database ownership guards; copied-directory/host rejection; native child retains the writer lock after wrapper SIGKILL; publication checks persisted source identity | Power-loss/metadata recovery, durable cursor validation/backup, sustained follow and agreed read/publication latency |
| Verified isolated bootstrap and onboarding | Real 180,090-block BSC replay; complete storage/account proof/code verification; immutable ready manifests; synthetic new-account catch-up; portable paginated export and verified import; real restore followed by 9,427 BSC blocks, pinned reads and checkpoint cleanup; another 2,305-block continuation with verified native ownership | Exercise representative multi-account onboarding and interrupted operational cutover; connect durable cursor continuation evidence to readiness |
| Completeness/lifecycle/proof tests | Wrong/missing proofs, missing/extra slots, bad metadata/code, wrong header commitments, gaps/forks/filter changes all fail; zero storage and proven non-inclusion pass | Producer/fork fixtures, persistent failed transaction and full 7702 matrix, CREATE/CREATE2 and deletion/recreation reconciliation; repair legacy verifier false-positive behavior |
| Retention/cost qualification and release | Local sample disk/throughput evidence; early database-budget rejection; checkpoint/candidate partition cleanup preserving readers and latest account state; pinned toolchain; package-producing `make build`; CI integration job | Bounded native delta retention, peak merge/spool/trie/export space accounting, representative hot/old/growing-account measurements, actual customer set when available, exact-head CI and v0.1.0 assets/notes |

## Reproducible local checks

`make test` runs the Rust tests and offline Python tests. With local ClickHouse
running, `make test-integration` also exercises the **published Substreams
1.22.0 binary**, not an emulated SQL writer. The tested release commit is
`be35ad36f63a52ff49d3e15cf993de4cad6bfbd9`; ClickHouse is `26.3.33.24`.

The Python suite currently contains 110 tests (35 offline, 75 integration).
Integration databases have random `evm_test_` names and are deleted afterward.
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

Publication rejects missing or changed source ownership, mismatched declared
filter/module/finality, changed package/schema metadata, replaced database identity,
unsupported producer versions and source ranges before the native run began.
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

## BSC evidence

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
