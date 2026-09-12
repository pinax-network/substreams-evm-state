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
PARAMS      := -p db_out=$(ACCOUNTS) -p map_state_changes=$(ACCOUNTS) -p map_block_state=$(ACCOUNTS)
# Event-log partitions: create ranges of PARTITION_STEP blocks covering
# [PARTITION_FROM, PARTITION_TO) before sinking (see postgres/schema.2.events.sql).
PARTITION_STEP ?= 1000000
PARTITION_FROM ?= 120000000
PARTITION_TO   ?= 130000000

PG_DSN ?= psql://dev-node:insecure-change-me-in-prod@localhost:5432/dev-node?sslmode=disable
PG_URL ?= postgresql://dev-node:insecure-change-me-in-prod@localhost:5432/dev-node
SPKG   := spkg/evm-state-v0.1.0.spkg
CH_DATABASE ?= evm_native
CH_DSN ?= clickhouse://evm_state:local-development-only@localhost:19000/$(CH_DATABASE)
CH_STATE ?= localdata/$(CH_DATABASE)
CH_CHECKPOINT_DATABASE ?= $(CH_DATABASE)
PYTHON ?= .venv/bin/python
EVM_STATE ?= target/release/evm-state
CH_ARGS := --package $(SPKG) --endpoint $(ENDPOINT) --accounts "$(ACCOUNTS)" --start-block $(START_BLOCK) --state-dir "$(CH_STATE)" --checkpoint-database $(CH_CHECKPOINT_DATABASE)

.PHONY: protogen
protogen: schema
	substreams protogen substreams.yaml --exclude-paths sf/substreams,sf/ethereum,sf/firehose,google

.PHONY: wasm build
wasm:
	cargo build --locked --target wasm32-unknown-unknown --release

# The release build produces the package, not just intermediate WASM.
build: pack

.PHONY: test
test:
	cargo test --locked
	$(PYTHON) -m pytest -q

.PHONY: test-integration python-deps
test-integration: pack
	$(PYTHON) -m pytest --run-clickhouse --run-database-crash -q

.PHONY: native
native:
	cargo build --locked --release -p evm-state

.PHONY: test-postgres
test-postgres:
	cargo test --locked -p evm-state --test postgres_verifier
	cargo test --locked -p evm-state --test postgres_database -- --ignored

python-deps:
	python3 -m venv .venv
	$(PYTHON) -m pip install -r requirements-test.lock
	$(PYTHON) -m pip install --no-deps -e .

# Concatenate postgres/schema.*.sql (sorted) into the single schema.sql the sink loads.
.PHONY: schema
schema:
	awk 'FNR==1 && NR>1 {print ""} {print}' postgres/schema.*.sql > postgres/schema.sql

.PHONY: pack
pack: wasm schema
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
	docker compose up -d --wait postgres

.PHONY: pg-down
pg-down:
	docker compose stop postgres

.PHONY: psql
psql:
	psql "$(PG_URL)"

# Create system tables + apply postgres/schema.sql. Run once per database.
.PHONY: pg-setup
pg-setup: pack
	substreams sink postgres setup $(SPKG) db_out --dsn "$(PG_DSN)"
	$(MAKE) partitions

# Create event-log partitions for [PARTITION_FROM, PARTITION_TO). Idempotent.
.PHONY: partitions
partitions:
	psql "$(PG_URL)" -q -c "SELECT create_event_partitions($(PARTITION_FROM), $(PARTITION_TO), $(PARTITION_STEP));"

# Stream db_out into Postgres over START_BLOCK:STOP_BLOCK (dev: flush every block).
.PHONY: pg-dev
pg-dev: pack
	substreams sink postgres $(SPKG) db_out -e $(ENDPOINT) -s $(START_BLOCK) -t $(STOP_BLOCK) --dsn "$(PG_DSN)" $(PARAMS) \
		--development-mode --undo-buffer-size 0 --batch-block-flush-interval 1 --live-block-flush-interval 1 --on-module-hash-mismatch=warn

# Follow head, final blocks only (recommended production mode).
.PHONY: pg-sink
pg-sink: pack
	substreams sink postgres $(SPKG) db_out -e $(ENDPOINT) -s $(START_BLOCK) --dsn "$(PG_DSN)" $(PARAMS) \
		--final-blocks-only --max-retries -1

.PHONY: ch-up setup dev sink
ch-up:
	docker compose up -d --wait clickhouse

setup: pack
	SUBSTREAMS_SINK_DSN="$(CH_DSN)" $(PYTHON) -m evm_state.cli --database $(CH_DATABASE) prepare $(CH_ARGS)

dev: setup
	SUBSTREAMS_SINK_DSN="$(CH_DSN)" $(PYTHON) -m evm_state.cli --database $(CH_DATABASE) ingest $(CH_ARGS) --stop-block $(STOP_BLOCK)

sink: setup
	SUBSTREAMS_SINK_DSN="$(CH_DSN)" $(PYTHON) -m evm_state.cli --database $(CH_DATABASE) ingest $(CH_ARGS) --max-retries -1

# Verify Postgres state against JSON-RPC (RPC_API_KEY / RPC_URL from env).
.PHONY: verify
verify: native
	PG_DSN="$(PG_URL)" $(EVM_STATE) postgres-verify

.PHONY: verify-root
verify-root: native
	PG_DSN="$(PG_URL)" $(EVM_STATE) postgres-verify --complete --address "$(ADDRESS)"
