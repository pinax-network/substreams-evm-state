# Native ClickHouse throughput and operating settings

The September 12 qualification uses the published Substreams 1.22.0 CLI and
ClickHouse 26.3.33.24 on the local development machine. These are public-account
measurements, not a customer capacity guarantee or a production SLA.

## Finalized follow needs different batching

The pinned native sink switches to direct inserts when its cursor reaches
`STEP_NEW`. A finalized-only stream keeps `STEP_NEW_IRREVERSIBLE` even while
following newly finalized blocks. Consequently its log can keep `is_live=false`
while it follows the chain, and it continues using the spool. See the pinned
[liveness checker](https://github.com/streamingfast/substreams/blob/be35ad36f63a52ff49d3e15cf993de4cad6bfbd9/sink/liveness.go)
and [native sink](https://github.com/streamingfast/substreams/blob/be35ad36f63a52ff49d3e15cf993de4cad6bfbd9/sink/sql/db_proto/sinker.go).

A 32-block decode batch therefore delays BSC finalized updates by roughly
15 seconds. Reducing the batch alone is insufficient: continuously arriving
blocks can keep a one-second-idle spool open until it reaches its size target.

The guarded `ingest` command now defaults to **one-block decoding and a 100 ms
spool idle threshold**. This permits small finalized batches to reach ClickHouse
between blocks. The threshold is not a wall-time latency guarantee: scheduling,
network arrival patterns, SQL writes and the checked cursor backup add time.
The integration regression holds an irreversible stream open after its second
block and requires durable progress before sending the next block.

`bootstrap-replay` keeps **32-block decoding and a 1,000 ms idle threshold** for
bounded historical work. For a separate historical `ingest`, select those settings
explicitly with `--decode-batch-size 32 --spool-max-idle-ms 1000`. Performance
settings can change between resumes without changing the module/filter identity.
Use a distinct `--prometheus-addr` for concurrent native cohorts; `127.0.0.1:0`
assigns an ephemeral local metrics port for qualification runs.

Both commands accept `--parallel-workers 100` to request server-side execution
workers through the native sink's `X-Substreams-Parallel-Workers` header. Omitting
it preserves the provider default; the provider decides the admitted limit.
This is separate from local decode workers and does not change the package,
module/filter identity, cursor or spool budget. The native chunk/restart test
verifies that a worker request can change from 50 to 100 across resumes while
the source continues at the checked cursor. Record the admitted session limit
and observed running jobs when interpreting performance.

The [worker scheduling comparison](evidence/bsc-worker-scheduling-2026-09-12.json)
uses the customer's one supplied example account and its frozen bootstrap
package. Both 100,000-block intervals pass native ownership, block continuity,
encoded target-header and durable-cursor checks:

| Historical range | Requested/admitted workers | Peak reported running jobs | Guarded ingestion |
|---|---|---|---|
| 56000000–56099999 | 100 / 100 | 100 | 61.90 s |
| 56100000–56199999 | 50 / 50 | 49 | 66.58 s |

Server telemetry reports 100,000 processed blocks for each interval. The first
100-worker attempt stopped during its initial capacity measurement before
starting ingestion; the reported successful run used a fresh database/directory.
These adjacent ranges are not a controlled same-block scaling experiment, and
shared infrastructure and underlying block-cache warmth were not controlled.
The small timing difference does not establish linear scaling. The measurement
does establish that the guarded native path passes the request to the provider
and verifies the resulting complete update interval. Full initial storage and
customer-set throughput remain separate qualifications.

An additional [200-worker measurement](evidence/bsc-worker-200-2026-09-12.json)
covers 65,000,000–65,399,999 with the same customer-example filter and frozen
package. The provider admitted 200 workers, telemetry observed 200 running jobs
and 400,000 processed blocks, and guarded ingestion finished in **176.76 seconds**.
Ownership, all 400,000 block identities, the durable cursor and encoded RPC target
header pass. The capacity supervisor completed with no rejected samples. This is
a different interval under shared load, not a controlled linear-scaling result.

Both long bootstraps subsequently resumed with `--parallel-workers 200`, retaining
their package/filter identities, million-block chunks, 1 GiB spool limit and
capacity policy. The previous native clients were intentionally stopped and their
supervisors confirmed terminal before replacement. WBNB's resumed session starts
immediately after its checked private prefix. The customer source first recovered
two local spool segments: all **78,009 blocks** between its private prefix and the
new server session are present with continuous hashes and durable cursor coverage.
Server session start alone therefore does not describe all data recovered during
a native restart. These were private prefixes at measurement time. The
[customer example](evidence/bsc-customer-example-rust-2026-09-12.json) and
[WBNB bootstrap](evidence/bsc-wbnb-bootstrap-rust-2026-09-13.json) subsequently
passed complete proof verification at their captured targets.

## Historical chunk size

The first million-block chunk measurements used `--chunk-blocks 1000000`,
100 requested workers and the same 1 GiB spool limit and capacity policy.
Those completed chunks are recorded in
[the chunk-size evidence](evidence/bsc-million-block-chunks-2026-09-12.json):

| Cohort | Historical range | Start to next chunk start | Resulting private nonzero slots | Compaction query memory |
|---|---|---|---|---|
| WBNB | 11110982–12110981 | 544.14 s | 1,517,479 | 2,790,047,576 bytes |
| Supplied customer example | 51813968–52813967 | 534.77 s | 1,172 | 83,067,709 bytes |

These timings include ingestion, compaction, cleanup and the next session's
setup. The previous four 100,000-block chunks had median start-to-start times
of 117.92 seconds for WBNB and 120.17 seconds for the customer example. Longer
chunks amortize that repeated work, but these are different historical ranges
under uncontrolled shared load; the comparison does not establish a universal
speedup or completion estimate.

The largest periodic disk sample through those first chunks was 8,088,326,144
allocated bytes across the shared server and declared local roots. A further
1,742,835,712 bytes is reserved for the retained original volume, within the
100 GB operating budget. No periodic sample failed in those intervals. The
previous runs were intentionally stopped before changing chunk size, and their
capacity reports and checked cursors were retained. These are checksummed private
prefixes, not complete account-root proofs or ready checkpoints. The WBNB query
memory measurement also needs qualification as retained slot count grows; chunk
size alone does not bound aggregation memory.

## Cache comparison

Both runs cover **121300000–121309999**, inclusive, and use a fresh local source
database. Their package SHA-256 is
`93d0be987950029816bd53462bd61fc110ffc94c6145aa22589816247e0ae347`,
and their module hash is `d7842af0f32665ec5f6f61d695c668862d5f8822`.

The filter is the three public sample accounts, including WBNB, plus a randomly
generated address used only to create a new cache identity. The sentinel has no
observed changes in this interval. A recent account proof establishes its
non-inclusion; it is not a customer contract. The repeat uses exactly the same
package, filter and range.

| Run | End-to-end native ingestion | Observed server processing | Sampled active jobs |
|---|---:|---:|---:|
| Fresh module-output cache identity | 31.929 s; 313 blocks/s | 10,000 blocks | Up to 10 |
| Identical cached repeat | 2.293 s; 4,362 blocks/s | 0 blocks | 0 |

The worker header was omitted; the server granted its default limit of 50.
Ten 1,000-block segments cannot establish sustained 50-worker scaling. These
times include native setup, capacity guards, ingestion, flush, cursor checks and
completion of the final RPC sample. They use the historical 32/1,000 batching.
Underlying raw-block caches and local OS caches were not cleared.

Both runs reconstructed **45,533,529 logical protobuf bytes**, and their ordered
output digest is identical:
`d12d9e54a90653bb811c011029d1ec4a64906e34479de1d8e5563990fda67910`.
Each measurement verifies complete block continuity, the durable cursor and the
encoded RPC-finalized end header. This qualifies the update interval, not complete
initial WBNB storage. Logical output excludes transport framing and retries and
is distinct from billable egress and retained database size.

## Live follow and reads

The full [machine-readable throughput record](evidence/bsc-throughput-2026-09-12.json)
contains ranges, hashes, session/worker observations, output bytes and capacity
samples. The associated [onboarding record](evidence/bsc-onboarding-2026-09-12.json)
tests a separate three-account publication and cutover.

Each live run followed 2,000 newly finalized blocks after a 100-block catch-up,
with 2,101 envelopes in about 901 seconds. Both completed with continuous block
identities, valid end cursors/headers and no failed capacity or RPC lag samples.

| Setting | Lag behind RPC finality, p50 / p95 / max | Durable block age, p95 |
|---|---:|---:|
| 32-block batch, 1,000 ms idle | 22 / 37 / 41 blocks | 18.18 s |
| 1-block batch, 100 ms idle | 3 / 5 / 5 blocks | 3.92 s |

Each row uses 169 samples after excluding the first 60 seconds. RPC and cursor
reads are sequential, and block age uses the host clock and second-resolution
block timestamps. A shared 470-second overlap also measured p95 lag of 37 versus
5 blocks, so the change is not explained just by separate time periods. The
streams shared a development server with read tests, recovery tests and a
separate onboarding replay. This is approximately 15 minutes of follow per run,
not a long-term load or availability guarantee. The selected account updates
averaged 5,466 bytes/block in the tuned run.

The largest sampled total across the two runs was **2,343,714,816 allocated
bytes**, including other databases on the shared ClickHouse data disks. The
tuned spool peak was 32,768 bytes; the cached historical replay peaked at
42,315,776 bytes. Capacity samples were at most 2.31 seconds apart in these runs.
These observed peaks do not imply a hard quota or qualify long-term growth.

Pinned reads of the real 46-slot checkpoint returned the same complete account
in all ten scans: p95 **20.67 ms**. Five complete scans of the synthetic
100,000-slot account returned identical ordered storage, with **500 pages** at
1,000 slots/page: p50 **28.09 ms**, p95 **33.33 ms**, maximum **634.88 ms**.
Complete large-account scans took 2.90–3.50 seconds. These timings include local
Python coordination and SQL; they exclude a remote API network and are repeated
sequential reads, not a concurrent-user benchmark.

## Reproduction

Use a fresh database and run directory for each measurement. Configure
[capacity monitoring](CAPACITY.md) to cover their common parent directory and
the local ClickHouse container. Keep credentials in environment variables.
The workload refuses an existing native run so a cursor resume cannot masquerade
as a full replay timing.

```bash
# Set SUBSTREAMS_SINK_DSN for the new measurement database first.
target/release/evm-state --database <new-source> capacity-run \
  --config <capacity.json> --output <run>/capacity --interval 1 -- \
  target/release/evm-state-qualify throughput \
    --database <new-source> --root <run> --package <frozen-package.spkg> \
    --accounts <public-filter> --start-block 121300000 --stop-block 121310000 \
    --decode-batch-size 32 --spool-max-idle-ms 1000 > <run>/run.log 2>&1

# Follow 2,000 newly finalized blocks after a 100-block catch-up. Replace the
# explicit start/stop and historical batching above with --live-blocks 2000.
# Current ingest defaults supply one-block decoding and 100 ms spool idle.

target/release/evm-state-qualify native-output \
  --database <new-source> --state-dir <run>/native --output <run>/output.json

# Uses the checkpoint database's existing EVM_STATE_HOME; creates and releases
# a pin, then proves every returned slot against the captured account proof.
target/release/evm-state --database <checkpoint-db> capacity-run \
  --config <capacity.json> --output <read-run>/capacity --interval 1 -- \
  target/release/evm-state-qualify checkpoint-reads \
    --database <checkpoint-db> --snapshot-id <ready-id> --account <address> \
    --page-size 1000 --passes 5 --output <read-run>/result.json \
    --work-dir <read-run>/work
```

Capture stdout and stderr in `run.log`: session and worker telemetry come from
the native CLI's structured log records. `result.json` records the bounded range,
package/module identity and elapsed time; `lag-samples.jsonl` samples RPC finality
and the writer's atomic checked cursor backup every five seconds. Errors remain
visible and never count as zero lag. The live summary excludes the first 60
seconds by elapsed time, independent of whether a sample is fast or slow.

After all four named runs (`cold`, `warm`, `live`, `live-tuned`) and their output
measurements complete, `target/release/evm-state-qualify summarize-throughput <common-parent> --output
<new-evidence.json>` validates their identity, output equality, server processing
observations and growing live windows. Its input also includes `cache-design.json`
and the two pinned-read reports. Raw logs remain local; the summary includes their
hashes. Do not copy API keys or cursor tokens into public evidence.

## Cost model

Pinax's [published prices](https://pinax.network/pricing), checked September 13,
2026, are **$150 per TiB of Substreams egress** and **$1.75 per million processed
blocks**, in USD. A TiB is 1,099,511,627,776 bytes. The listed Pro plan is $49/month
with $200 of included resource usage; resource cost and the subscription invoice
are different quantities.

The published Pro plan lists 50 Substreams workers. The 200-worker measurements
in this report reflect the allocation admitted by the provider for the test
account; plan selection must account for that difference when using these timings.

For a 30-day illustration at 0.45 seconds/block, use 5,760,000 blocks:

```text
modeled resource cost = (logical output bytes per block × 5,760,000 / 2^40 × $150)
                      + (5,760,000 / 1,000,000 × $1.75)
```

The earlier three-account native sample measured 4,510.3529 bytes/block. Holding
that activity constant gives **25.98 GB of logical output and about $13.62 in
modeled monthly resources**. This treats logical output as an egress proxy and
one full month of followed blocks as processed; it is not an invoice measurement.
Proof RPC usage, database hosting, backups, taxes and other account usage are
additional considerations. Cache processing counters do not establish billing
semantics or a free replay entitlement.

Applying the same formula to the tuned live sample gives **31.48 GB** of logical
monthly output and **about $14.38** in modeled resources. The difference from the
historical sample shows why one short window should not become a fixed customer
price.

Do not multiply this figure by 19 or 64: storage and update volume depend on
which contracts are selected. The actual account lists remain unavailable.
The old PostgreSQL/Firehose numbers use different output and sampling methods,
so this evidence does not support the former 45-times-cheaper customer claim.
