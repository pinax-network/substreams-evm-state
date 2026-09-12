# ClickHouse qualification evidence

Status: prototype, 2026-09-12. **Do not publish v0.1.0 yet.** This records current
evidence and preserves the four original deliverables; partial coverage does not
make an entire deliverable complete.

| Deliverable | Evidence in this prototype | Remaining acceptance work |
|---|---|---|
| Native finalized projection and coherent reads/restarts | One physical block envelope; generated native Nested schema; direct and spooled process-kill recovery; failure after block insertion before cursor write; frozen package/filter and database ownership guards; copied-directory/host rejection; inherited native writer lock; checked atomic cursor backup and explicit torn-cursor recovery; database-process SIGKILL before/after publication; 15-minute finalized follow; measured complete pinned reads; fixed finalized decode/spool latency; persistent OS host identity and explicit legacy hostname recovery | Customer latency targets and deployment conditions; storage must honor sync writes (physical host power loss is not emulated) |
| Verified isolated bootstrap and onboarding | Real 180,090-block BSC replay; complete storage/account proof/code verification; immutable ready manifests; synthetic new-account catch-up; portable export and verified import; real restore followed by 9,427 BSC blocks and another 2,305-block continuation; 195,056-block replay with forced kill, cursor recovery and root verification; publication requires durable cursor coverage; real three-account cutover with a killed publisher, unchanged old reader and combined-filter continuation | Broader nonempty hot-account bootstrap and customer account qualification |
| Completeness/lifecycle/proof tests | Wrong/missing proofs, missing/extra slots, bad metadata/code, wrong header commitments, gaps/forks/filter changes all fail; zero storage and proven non-inclusion pass; synthetic failed sender/self/multiple/discarded 7702 cases; nineteen additional captured v3/v4/v5 CREATE/CREATE2, deletion and authorization cases, including failed self-delegation with persistent code, with RPC parity; eight-account native interval verifies 24 observed fields against saved proofs; 80,813-block failed distinct-authority replay verifies both persisted delegations; deletion/recreation across native patches and inherited checkpoint storage; legacy verifiers fail on unknown/wrong metadata | Captured failed delegation clearing, post-Cancun existing-account destruction, broader representative lifecycle replay |
| Retention/cost qualification and release | Whole-directory capacity monitoring, sampled headroom enforcement and publication/trie guards; measured 64-account synthetic checkpoint/export/restore/churn/retention; observed active merge space and real BSC spool; checkpoint/candidate cleanup preserving readers; native history cleanup and bounded bootstrap chunks; pinned toolchain and package-producing build; separated cold/cached/live results, current price model and old-contract cohort addition | Nonempty hot-account initial state and sustained-growth qualification, actual customer set when available, exact-head CI and v0.1.0 assets/notes |

## Reproducible local checks

`make test` runs the Rust tests and offline Python tests. With local ClickHouse
running, `make test-integration` also exercises the **published Substreams
1.22.0 binary**, not an emulated SQL writer. The tested release commit is
`be35ad36f63a52ff49d3e15cf993de4cad6bfbd9`; ClickHouse is `26.3.33.24`.

The Python suite currently contains 260 tests (117 offline, 141 ClickHouse
integration and two PostgreSQL integration).
Integration databases have random `evm_test_` names and are deleted afterward.
One fault test creates its own `evm-crash-` Docker container, kills/restarts that
database process and removes the container afterward. It never restarts the
configured development database. The previous 258-test Python baseline passed
locally in 113.65 seconds. Current local checks pass 117 offline Python tests,
36 Rust tests, the native chunked bootstrap/restart test with a separate metrics
listener, and `make build`. CI also runs the complete integration suite. The
current suite includes stable host identity/recovery, captured recreation/storage
clearing and repeated authorization proof checks. A process-kill
test waits for the killed native child to release its inherited writer lock before recovery; reaping its
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

Capacity tests validate whole-directory accounting, local allocated bytes and
component breakdowns without duplicate totals, container/endpoint identity,
persistent mounts, symlink rejection, independent budget and free-space reserves,
and incomplete-sample failure. The supervisor rejects unsafe initial work and
stops descendants that ignore SIGTERM. A final rejected sample cannot report a
successful run just because its child exited zero. Native ingestion resumes after
capacity stops during the first batch and after a durable cursor; checkpoint,
bootstrap, export and import candidates remain unpublished if their final guard
fails. Cursor observation after native shutdown captures a valid newly flushed
position when present.

## Capacity qualification

The [capacity operations guide](CAPACITY.md) describes the measured scope,
headroom policy, recovery and sampling limits. The
[machine-readable evidence](evidence/capacity-qualification-2026-09-12.json)
records three completed runs, all using a 100 GB data budget, 10 GB budget reserve
and 1 GiB minimum available/unreserved filesystem space:

| Workload | Result | Observed allocated peak, including shared server data |
|---|---|---|
| 64 synthetic accounts, 104,032 nonzero slots; one account holds 100,000 slots | Complete checkpoint, portable export, verified restore, 50,000 slot clears/replacements and pinned-reader retention all pass | 1,074,298,880 bytes |
| Eight incompressible parts, 256 MiB payload, forced final merge | Three samples observed an active merge; part catalog grew from 269,764,725 to 809,286,373 bytes including retained intermediate/old parts | 1,893,478,400 bytes |
| Real BSC native output, blocks 121300000–121309999, three public accounts including WBNB | 10,000 contiguous finalized block envelopes and cursor verified against the encoded RPC header; spool peak 23,080,960 allocated bytes | 1,823,137,792 bytes |

The state workload recorded 275 periodic and 531 in-operation guard samples, with
no failed samples. Its largest periodic gap was 1.587 seconds; trie guards run
before temporary files disappear. Local allocation peaked at 29,786,112 bytes.
The first complete checkpoint took 56.49 seconds, export 45.57 seconds, verified
restore 108.51 seconds, and the next checkpoint after slot churn 58.50 seconds.
The portable export was 4,744,344 bytes. Pruning respected the pinned old
checkpoint and removed it after release. These are real SQL/proof operations over
synthetic state, not a customer simulation or a producer replay.

The merge fixture sampled every 0.1 seconds plus measurement overhead; its largest
gap was 0.490 seconds. It illustrates why active part sizes alone understate
transient/retained merge space. The native BSC run recorded 48 periodic and 26
guard samples, with no failed samples and a largest gap of 0.756 seconds. It took
31.97 seconds including guarded setup/ingestion. Cache warmth was not established.

Logical native output for that BSC interval was 45,103,529 protobuf bytes
(4,510.35 bytes/block on average; maximum 23,861), reconstructed with the frozen
package descriptor from deduplicated native rows. It contained 219,692 storage
patches and 9,936 balance patches. This excludes transport framing, retries and
billing adjustments. `scripts/measure_native_output.py` reproduces the measurement
and verifies interval/header/cursor identity; it does not claim complete initial
storage. Separate cold/cached/live results and a current price model now appear
in [THROUGHPUT.md](THROUGHPUT.md).

These totals deliberately include other databases/system data sharing the server
disks and all declared local files. They are conservative workload observations,
not per-account storage bounds. No sample exceeded the operating thresholds, but
sampling cannot prove an absolute disk ceiling between observations. Hard limits
require storage quotas. The customer's absent account set, acceptable publication
latency and long-term growth remain unqualified.

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

The [expanded captured matrix](../tests/fixtures/lifecycle/manifest.json) now adds
nineteen original transaction/header pairs: v3 WBNB creation, CREATE2 with
delegated initialization, pre-Cancun deletion/recreation and storage clearing,
post-Cancun same-transaction creation/destruction, v4 delegation installation and
failed self-delegation, and v5 self/multiple/discarded/cleared/repeated authorizations. Literal Rust expectations are cross-checked with before/after
archive RPC metadata and touched slots. RPC `callTracer` independently identifies
the CREATE2 opcode; the Firehose model uses CREATE for both creation opcodes.

A new [eight-account native interval](evidence/bsc-lifecycle-matrix-2026-09-12.json)
covers 121464944–121467555: all 2,612 block envelopes and cursor continuity pass,
24 observed account fields match saved account proofs, and the one final touched
slot matches archive RPC. Offline CI rechecks those proofs and fixture hashes.
This is update parity, not a complete bootstrap of all eight accounts. A separate [145-block replay](evidence/bsc-recreation-2026-09-12.json) verifies two
same-address destruction/recreation cycles, replacement bytecode and clearing a
nonzero slot written between cycles. This historical diagnostic uses archive RPC
field/slot comparisons because account proofs exceed the provider's proof window. Capture
limitations and the remaining matrix are detailed in [LIFECYCLE.md](LIFECYCLE.md).

A further [five-account native interval](evidence/bsc-authorization-edges-2026-09-12.json)
covers 121468046–121477785, including accepted and discarded repeated authorities
and a reverted transaction with persistent nonce/code changes. All 9,740 blocks
and the durable cursor verify; nine observed metadata fields match saved account
proofs. No storage patches occur in this interval, and no complete-state
checkpoint is claimed for this filter.

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

## Native throughput and interrupted real onboarding

The [throughput record](evidence/bsc-throughput-2026-09-12.json) and
[operating guide](THROUGHPUT.md) separate the new module-output cache run from
its identical repeat: **31.929 versus 2.293 seconds for 10,000 blocks**. Server
telemetry reports 10,000 versus zero processed blocks, and the ordered output
digests match. The public filter adds an unused random address only to establish
a fresh cache identity. This does not establish cold underlying block storage.

Two 15-minute finalized follows each verified 2,101 contiguous envelopes. The
new one-block/100 ms defaults reduce post-startup p95 checked-cursor lag from
**37 to five blocks** behind RPC finality. The pinned sink's `is_live=false` flag
persists for irreversible cursors, so it cannot serve as the head-lag measurement.
Pinned pagination returned identical full storage across ten real 46-slot scans
and five synthetic 100,000-slot scans. Their page p95 latencies were 20.67 ms and
33.33 ms, respectively; the latter's maximum was 634.88 ms. These are local,
sequential samples with shared-server background work, not an SLA.

The [three-account onboarding record](evidence/bsc-onboarding-2026-09-12.json)
adds two public accounts to the restored 46-slot checkpoint:

- `0x10ed43c718714eb63d5aa57b78b54704e256024e` has proven empty storage and
  21,936 bytes of code, also observed by archive RPC at block 10,000,000.
- `0xd52573f6d4f68d8e7f8fe2ed50a1023c5f6fe82a` is the real failed-7702 sender
  from the earlier fixtures; its storage is proven empty at this cutover.

The existing account catches up from 121309259 through **121461488**; the new
cohort independently covers the 501 blocks ending at that common target. Empty
storage roots permit complete verification from this recent interval. An account
with nonempty storage would need complete enumeration; a separately examined
7702 authority had nonempty storage and was not included in this short bootstrap.

The first replay was capacity-stopped with an incomplete sample. Its native
cursor/spool were retained, and it resumed from block **121368989**. Diagnosis
reproduced separately read ClickHouse free/unreserved counters disagreeing by
4 KiB. The monitor now uses the lower valid reading instead of assuming an atomic
counter snapshot. The original generic failure record is preserved in evidence.
The resumed capacity run completed with 154 periodic and 165 guard samples,
no failed samples and a measured peak of **2,228,838,400 bytes**, including shared
server data. Its 101.403-second catch-up phase times only the resumed remainder,
not the full 152,230-block interval or time spent diagnosing the stop.

After ingesting both cohorts, the workload sends SIGKILL to its own publication
child after acknowledged account insertion but before the ready manifest. The
ready count remains unchanged, and the pinned old account still reads identically.
A retry verifies and publishes all three accounts at 121461488 in **3.575 seconds**.
A fresh, combined filter then ingests **974 blocks** through **121462462** and
publishes another fully verified checkpoint in **2.382 seconds**. The combined
filter has its own new native identity; neither old run is silently retargeted.
The two new generations have the same state checksum, with 46 total nonzero slots.

`scripts/qualify_onboarding.py` reproduces this path from an existing ready
checkpoint into fresh databases. Run it under `capacity-run`, cover the source
controller and new work root, and set `NATIVE_DSN_TEMPLATE` in the environment
with a `{database}` placeholder. Supply `--prefix`, `--root`, `--package`,
`--source-database`, `--source-snapshot`, `--source-control`, and `--new-accounts`.
It rejects nonempty new-account storage for this bounded scenario. `--resume`
continues a capacity-stopped run after import and before completed cutover, using
the already captured target proofs and original native cursors. It keeps failed
candidates and measurements for inspection. It does not qualify complete initial
storage for a hot token or the missing customer list.

## Inputs and release gates

The [customer questions and ClickHouse proposal](CUSTOMER_PROPOSAL.md) are drafted,
not sent. Their account list,
deployment/consumer interface, latency and onboarding targets, firm retained-data
cap, retention window and header trust requirements are still unknown. Continue
independent local qualification without substituting sample results for theirs.

`make build` must produce the final `.spkg`, and the first GitHub release must be
`v0.1.0` with detailed notes and package asset. Release remains gated by the open
acceptance items above; do not call the goal complete based only on these tests.

## Long replay host recovery

A real WBNB bootstrap exposed a hostname change on the running macOS host:
`MacBookPro` became `Deniss-MacBook-Pro.local`. The legacy ownership check stopped
before the next compaction, preserving the private prefix through 6849267
(306,507 nonzero slots) and the later native cursor. Its capacity report recorded
441 periodic and 1,796 in-operation samples, zero rejected samples and a
2,803,269,632-byte observed peak. The stop was an ownership mismatch, not a
capacity threshold breach.

New runs/controllers now use a hashed persistent OS machine identity. Recovery
of this known original machine checked the original database/controller records,
frozen package/schema, durable/native cursor and compacted state under exclusive
locks, then wrote matching host recovery sidecars. The original identities,
package, cursors and prefix were preserved, and the same bounded replay resumed.
Tests cover network-name changes, another machine, copied/changed recovery
records, damaged state, writer exclusion and interruption between the two
sidecar writes. The [operations guide](CHECKPOINTS.md) explains this explicit
operator attestation and its limits. WBNB remains unverified until its complete
storage matches the saved final account proof; this recovery is not completion
of the hot-account qualification.

## Supplied customer example

The customer supplied `0x32c59d556b16db81dfc32525efb3cb257f7e493d` as an example,
although the full 19-account/76-slot list remains unavailable. Archive RPC located
a code-presence boundary at 48261373, and the captured v4 Extended block confirms
CREATE in transaction
`0xdbd9845e36fff38c42f082763ce2ed0e6aa4932602fcef243c92bb30827c4793`.
The current bytecode has 16,415 bytes. The two constructor-written keys alone,
and a larger set of 137 keys observed in the existing native samples, both fail
to reconstruct its storage root at saved proof target 121478038. These are
incomplete initial-state candidates, not ready exports.

An isolated creation-to-target replay now runs with its own database, controller,
frozen package, cursor, spool and capacity guard. Its first 200,000 blocks retain
22 nonzero slots in a private prefix. Final completeness, export and continuation
remain pending. This is one actual customer example, not qualification of the
missing complete pilot set. `bootstrap-replay --prometheus-addr` now permits a
separate metrics listener for each concurrent cohort, as `ingest` already did.

## Capacity stop and local data migration

Both long replays stopped when the shared Docker VM filesystem fell below the
1 GiB free-space floor. The measured prototype allocation was about 2.1 GB at the
stop, far below the selected 100 GB budget. WBNB retained its private prefix through
8449267 (827,900 nonzero slots) and a validated native cursor through 8449994; the
customer example retained its prefix through 48461372 (22 slots) and cursor through
48539979. Neither stop published a ready checkpoint.

The [migration record](evidence/storage-migration-2026-09-12.json) captures a
gracefully stopped database copy to a persistent host bind, matching database and
runtime recovery archives, and verification of all 36 database UUIDs, original
local file hashes, cursor coverage and compacted state. The new data filesystem
reported about 410 GB free. The original Docker volume remains intact; unrelated
Docker resources were left alone.

Its 1,742,835,712 allocated bytes are reserved outside the live meter by reducing
both policies to 98,257,164,288 bytes. The database and runtime archives are inside
the measured local roots. Together this preserves the overall 100 GB budget,
10 GB operating reserve and 1 GiB free-space floor. Both resumed runs passed the
new capacity checks and compacted their preserved suffixes through 8449994 and
48539979 respectively. They remain unverified historical prefixes, with final
root verification and sustained-growth qualification still pending. This recovery
does not establish host-bind performance or a customer footprint bound.
