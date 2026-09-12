# Customer follow-up draft

Draft for Kodak; not sent. Updated 2026-09-12.

Hi Kodak — we have a ClickHouse-first prototype for the selective BSC state
workflow you described, and propose qualifying it against your pilot accounts.

The pipeline uses an account-keyed Substreams map module to send storage, balance,
nonce and code updates directly into the native ClickHouse sink. It does not
require the PostgreSQL `db_out` adapter. It follows finalized blocks and retains
block/hash continuity, including blocks with no changes to your selected accounts.

For initial state, the proposed path replays the selected accounts' relevant
history in bounded chunks. Before publishing a checkpoint, it reconstructs the
complete nonzero storage trie and checks it against the account proof, together
with balance, nonce and bytecode. Every checkpoint identifies its exact block,
hash and state root. Header finality currently comes from the RPC provider;
verifying the state against that header is separate from independently verifying
BSC consensus.

Consumers read a pinned, immutable checkpoint, with paginated storage and a
portable export. Newly discovered accounts bootstrap separately, catch up to a
common finalized block, and join a new ready checkpoint after verification.
Existing readers can finish using their previous checkpoint.

ClickHouse accepts new row versions efficiently, but background deduplication
alone does not provide a coherent current-state snapshot. The prototype explicitly
deduplicates reads, orders changes by blockchain position and publishes a ready
manifest only after verification. See ClickHouse's
[ReplacingMergeTree guidance](https://clickhouse.com/docs/engines/table-engines/mergetree-family/replacingmergetree).

We are developing against the conservative end of your budget: 100 GB of retained
data, with headroom for ingestion, merges, verification and checkpoints. Small
real-account and larger synthetic tests have passed; a full historical WBNB
bootstrap is still being qualified. We cannot yet confirm the footprint or
bootstrap time for your account set. This remains technical qualification, with
no purchase or subscription change requested.

To make the next test useful, could you share:

1. **Pilot input:** the 19 account addresses and 76 named slots, and any candidate
   additions for the 64-account test. Known creation blocks and a few expected
   values at a specific block/hash would help. We will treat the named slots as
   validation samples, not as the complete storage requirement.
2. **Consumer interface:** would you prefer paginated checkpoint files, direct
   ClickHouse access, or an API feeding your local executor? Does ClickHouse need
   to run within your local storage budget, or could it be hosted with only the
   selected checkpoint retained locally?
3. **Freshness and onboarding:** what maximum finalized-state lag is acceptable,
   how frequently should a new ready checkpoint appear, and how long can a newly
   discovered account remain unavailable while it bootstraps?
4. **Retention and capacity:** is 100 GB the firm limit, or may qualification use
   more within the stated 100–300 GB range? How many old checkpoints and how much
   update history must remain available, including recovery after an outage?
5. **Proof trust:** is complete state verified against a provider-finalized header
   sufficient, or will your application supply independently trusted block hashes?
6. **Execution acceptance:** can you provide a few representative router/token/
   hook executions and their expected results, including the shared manager/token
   accounts that dominate the workload? This will let us test your actual reads
   and dependencies in addition to matching sampled RPC values.

With those inputs, we can report measured completeness, bootstrap time, finalized
lag, read latency and retained footprint for the pilot, then expand to the bounded
64-account set.
