.DEFAULT_GOAL := pack

# Pinax BSC Substreams endpoint. Auth: export SUBSTREAMS_API_TOKEN=<jwt>
# or SUBSTREAMS_API_KEY=<key> (the substreams CLI / sink read both).
ENDPOINT    ?= bsc.substreams.pinax.network:443
# Customer's probe window (32 blocks) by default so numbers are comparable.
START_BLOCK ?= 120140091
STOP_BLOCK  ?= 120140123
# Accounts filter passed as module params (comma-separated 0x addresses).
# Empty = every account. Override: make dev ACCOUNTS=0xabc...,0xdef...
ACCOUNTS    ?= 0x32c59d556b16db81dfc32525efb3cb257f7e493d,0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c,0x0000f90827f1c53a10cb7a02335b175320002935
PARAMS      := -p db_out=$(ACCOUNTS) -p map_state_changes=$(ACCOUNTS)
# Event-log partitions: create ranges of PARTITION_STEP blocks covering
# [PARTITION_FROM, PARTITION_TO) before sinking (see postgres/schema.2.events.sql).
PARTITION_STEP ?= 1000000
PARTITION_FROM ?= 120000000
PARTITION_TO   ?= 130000000

PG_DSN ?= psql://dev-node:insecure-change-me-in-prod@localhost:5432/dev-node?sslmode=disable
PG_URL ?= postgresql://dev-node:insecure-change-me-in-prod@localhost:5432/dev-node
SPKG   := spkg/evm-state-v0.1.0.spkg

.PHONY: protogen
protogen: schema
	substreams protogen substreams.yaml --exclude-paths sf/substreams,sf/ethereum,sf/firehose,google

.PHONY: build
build:
	cargo build --target wasm32-unknown-unknown --release

.PHONY: test
test:
	cargo test

# Concatenate postgres/schema.*.sql (sorted) into the single schema.sql the sink loads.
.PHONY: schema
schema:
	awk 'FNR==1 && NR>1 {print ""} {print}' postgres/schema.*.sql > postgres/schema.sql

.PHONY: pack
pack: build schema
	substreams pack -o $(SPKG)

.PHONY: info
info:
	substreams info $(SPKG)

# Stream map_state_changes in the GUI (no database).
.PHONY: gui
gui: build
	substreams gui -e $(ENDPOINT) substreams.yaml map_state_changes -s $(START_BLOCK) -t $(STOP_BLOCK) $(PARAMS)

# Run the module once and dump JSON output (no database).
.PHONY: run
run: build
	substreams run -e $(ENDPOINT) substreams.yaml map_state_changes -s $(START_BLOCK) -t $(STOP_BLOCK) $(PARAMS) -o jsonl

.PHONY: pg-up
pg-up:
	docker compose up -d --wait

.PHONY: pg-down
pg-down:
	docker compose down -v

.PHONY: psql
psql:
	psql "$(PG_URL)"

# Create system tables + apply postgres/schema.sql. Run once per database.
.PHONY: setup
setup: pack
	substreams sink postgres setup $(SPKG) --dsn "$(PG_DSN)"
	$(MAKE) partitions

# Create event-log partitions for [PARTITION_FROM, PARTITION_TO). Idempotent.
.PHONY: partitions
partitions:
	psql "$(PG_URL)" -q -c "SELECT create_event_partitions($(PARTITION_FROM), $(PARTITION_TO), $(PARTITION_STEP));"

# Stream db_out into Postgres over START_BLOCK:STOP_BLOCK (dev: flush every block).
.PHONY: dev
dev: pack
	substreams sink postgres $(SPKG) -e $(ENDPOINT) -s $(START_BLOCK) -t $(STOP_BLOCK) --dsn "$(PG_DSN)" $(PARAMS) \
		--development-mode --undo-buffer-size 0 --batch-block-flush-interval 1 --live-block-flush-interval 1 --on-module-hash-mismatch=warn

# Follow head, final blocks only (recommended production mode).
.PHONY: sink
sink: pack
	substreams sink postgres $(SPKG) -e $(ENDPOINT) -s $(START_BLOCK) --dsn "$(PG_DSN)" $(PARAMS) \
		--final-blocks-only --max-retries -1 --on-module-hash-mismatch=warn

# Verify Postgres state against JSON-RPC (RPC_API_KEY / RPC_URL from env).
.PHONY: verify
verify:
	PG_DSN="$(PG_URL)" python3 scripts/verify_rpc.py

.PHONY: verify-root
verify-root:
	PG_DSN="$(PG_URL)" python3 scripts/verify_storage_root.py $(ADDRESS)
