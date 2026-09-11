"""Explicitly opted-in integration tests use uniquely named disposable databases."""
import os
from pathlib import Path
import shutil
import subprocess
import urllib.parse
import uuid

import pytest

from evm_state.ch import ClickHouse

ROOT = Path(__file__).resolve().parents[1]
SPKG = ROOT / "spkg/evm-state-v0.1.0.spkg"


def pytest_addoption(parser):
    parser.addoption("--run-clickhouse", action="store_true", help="run local ClickHouse/native sink integration tests")


def pytest_collection_modifyitems(config, items):
    if not config.getoption("--run-clickhouse"):
        for item in items:
            if "clickhouse" in item.keywords:
                item.add_marker(pytest.mark.skip(reason="use --run-clickhouse with local ClickHouse running"))


def native_dsn(database):
    value = os.environ.get("CH_TEST_DSN", "clickhouse://evm_state:local-development-only@127.0.0.1:19000/default")
    parsed = urllib.parse.urlsplit(value)
    return urllib.parse.urlunsplit(parsed._replace(path="/" + database))


def native_env(database):
    # No provider credentials are needed or sent to the local mock gRPC server.
    env = {k: v for k, v in os.environ.items() if not k.startswith("SUBSTREAMS_")}
    env["SUBSTREAMS_SINK_DSN"] = native_dsn(database)
    return env


def native_setup(database, directory):
    if not shutil.which("substreams") or not SPKG.is_file():
        pytest.fail("integration tests require substreams CLI and make build")
    metadata = directory / "meta"
    metadata.mkdir(parents=True)
    result = subprocess.run(["substreams", "sink", "clickhouse", "setup", str(SPKG), "map_block_state",
        "--sink-info-folder", str(metadata), "--bytes-encoding", "0xhex"],
        env=native_env(database), capture_output=True, text=True, timeout=60)
    if result.returncode:
        pytest.fail("native schema setup failed: " + result.stderr[-4000:])


@pytest.fixture(scope="session")
def native_proxy(tmp_path_factory):
    binary = tmp_path_factory.mktemp("grpc-proxy") / "grpc-proxy"
    subprocess.run(["go", "build", "-mod=readonly", "-o", str(binary), "."],
                   cwd=ROOT / "tests/grpc_proxy", check=True, capture_output=True, timeout=180)
    return binary


@pytest.fixture(scope="session")
def native_template(tmp_path_factory):
    database = "evm_test_template_" + uuid.uuid4().hex
    admin = ClickHouse("default")
    try:
        native_setup(database, tmp_path_factory.mktemp("native-template"))
        yield database
    finally:
        admin.execute(f"DROP DATABASE IF EXISTS {database} SYNC")


@pytest.fixture
def databases(native_template):
    admin = ClickHouse("default")
    created = []

    def create(native=True):
        database = "evm_test_" + uuid.uuid4().hex
        created.append(database)
        admin.execute(f"CREATE DATABASE {database}")
        client = ClickHouse(database)
        if native:
            client.execute(f"CREATE TABLE state_blocks AS {native_template}.state_blocks")
        return client

    yield create
    for database in reversed(created):
        admin.execute(f"DROP DATABASE IF EXISTS {database} SYNC")
