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
PARAMS      := map_state_changes=$(ACCOUNTS)

PG_DSN ?= psql://dev-node:insecure-change-me-in-prod@localhost:5432/dev-node?sslmode=disable
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
	substreams gui -e $(ENDPOINT) substreams.yaml map_state_changes -s $(START_BLOCK) -t $(STOP_BLOCK) -p "$(PARAMS)"

# Run the module once and dump JSON output (no database).
.PHONY: run
run: build
	substreams run -e $(ENDPOINT) substreams.yaml map_state_changes -s $(START_BLOCK) -t $(STOP_BLOCK) -p "$(PARAMS)" -o jsonl

.PHONY: pg-up
pg-up:
	docker compose up -d --wait

.PHONY: pg-down
pg-down:
	docker compose down -v

.PHONY: psql
psql:
	psql "postgresql://dev-node:insecure-change-me-in-prod@localhost:5432/dev-node"

# Create system tables + apply postgres/schema.sql. Run once per database.
.PHONY: setup
setup: pack
	substreams-sink-sql setup "$(PG_DSN)" $(SPKG)

# Stream db_out into Postgres over START_BLOCK:STOP_BLOCK (dev: flush every block).
.PHONY: dev
dev: pack
	substreams-sink-sql run "$(PG_DSN)" $(SPKG) $(START_BLOCK):$(STOP_BLOCK) -e $(ENDPOINT) -p "$(PARAMS)" \
		--development-mode --undo-buffer-size 0 --batch-block-flush-interval 1 --live-block-flush-interval 1 --on-module-hash-mistmatch=warn

# Follow head, final blocks only (recommended production mode).
.PHONY: sink
sink: pack
	substreams-sink-sql run "$(PG_DSN)" $(SPKG) $(START_BLOCK): -e $(ENDPOINT) -p "$(PARAMS)" --final-blocks-only --infinite-retry
