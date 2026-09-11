"""Build immutable ClickHouse checkpoints, verify, then publish one manifest row."""
import hashlib
from contextlib import ExitStack
import json
from pathlib import Path
import tempfile
import time
import uuid

from .ch import ClickHouse, identifier
from .proof import VerificationError, address, unhex, verify_account, verify_complete, EMPTY_STORAGE_ROOT
from .triedb import TrieDB
from .header import verify_header
from .control import control, object_id
from .source import verified_source

ZERO = "0x" + "00" * 32


def canonical_accounts(values):
    if isinstance(values, str):
        values = values.replace(",", " ").split()
    result = sorted({address(value) for value in values})
    if not result:
        raise ValueError("selected account list is empty")
    return result


def connect_like(client, database):
    return ClickHouse(database, client.url, client.user, client.password)


def setup(client):
    connect_like(client, "default").execute(f"CREATE DATABASE IF NOT EXISTS {client.database}")
    # Loaded via package data; works from an installed wheel as well as the repo.
    sql = (Path(__file__).parent / "checkpoints.sql").read_text()
    for statement in sql.split(";"):
        if statement.strip():
            client.execute(statement)


def manifest(client, snapshot_id):
    with control(client).reader():
        return _manifest(client, snapshot_id)


def _manifest(client, snapshot_id):
    object_id(snapshot_id)
    row = client.one("SELECT manifest FROM checkpoints FINAL WHERE snapshot_id={id:String}", {"id": snapshot_id})
    data = json.loads(row["manifest"])
    if data.get("snapshot_id") != snapshot_id or data.get("status") != "ready":
        raise VerificationError("checkpoint has no valid ready manifest")
    return data


def validate_interval(client, source, header):
    """Read block metadata in one query and reject gaps, forks or filter drift."""
    expected = int(source["start_block"])
    end = int(header["number"])
    if expected < 0 or expected > end:
        raise VerificationError("invalid checkpoint input range")
    accounts = ",".join(canonical_accounts(source["accounts"]))
    previous_hash = None
    count = 0
    for row in client.rows(
        "SELECT number, hash, parent_hash, state_root, accounts, schema_version, producer_version "
        "FROM state_blocks FINAL WHERE number >= {start:UInt64} AND number <= {end:UInt64} ORDER BY number, hash",
        {"start": expected, "end": end},
    ):
        number = int(row["number"])
        if number != expected:
            raise VerificationError(f"missing or conflicting block at {expected}")
        if row["accounts"] != accounts or int(row["schema_version"]) != 1:
            raise VerificationError(f"account filter or schema changed at block {number}")
        if int(row["producer_version"]) not in {3, 4, 5}:
            raise VerificationError(f"unsupported producer version at block {number}")
        unhex(row["hash"], 32)
        unhex(row["parent_hash"], 32)
        if previous_hash is not None and row["parent_hash"] != previous_hash:
            raise VerificationError(f"broken parent continuity at block {number}")
        if number == end and (row["hash"] != header["hash"] or row["state_root"] != header["state_root"]):
            raise VerificationError("stream checkpoint differs from proof header")
        if count == 0 and source.get("parent_hash") and row["parent_hash"] != source["parent_hash"]:
            raise VerificationError("stream does not continue the base checkpoint")
        previous_hash = row["hash"]
        expected += 1
        count += 1
    if expected != end + 1:
        raise VerificationError(f"incomplete stream: expected through block {end}, got through {expected - 1}")
    return count


def _union_storage(sources, base, client, params):
    queries = []
    for i, source in enumerate(sources):
        database = identifier(source["database"])
        params[f"start{i}"] = source["start_block"]
        queries.append(
            f"SELECT storage.address AS address, storage.slot AS slot, storage.value AS value, "
            f"tuple(number, storage.ordinal) AS position FROM {database}.state_blocks FINAL ARRAY JOIN storage "
            f"WHERE number >= {{start{i}:UInt64}} AND number <= {{end:UInt64}}"
        )
    if base:
        params["base"], params["base_number"] = base["snapshot_id"], base["header"]["number"]
        queries.append(
            "SELECT address, slot, value, tuple({base_number:UInt64}, toUInt64(0)) AS position "
            "FROM checkpoint_storage FINAL WHERE snapshot_id={base:String}"
        )
    if not queries:
        raise VerificationError("checkpoint has no source state")
    return " UNION ALL ".join(queries)


def _observed_fields(client, sources, base, params):
    result = {}
    if base:
        for row in client.rows("SELECT address, nonce, balance, code_hash, code FROM checkpoint_accounts FINAL "
                               "WHERE snapshot_id={base:String}", params):
            result[row["address"]] = {"nonce": int(row["nonce"]), "balance": row["balance"],
                "code_hash": row["code_hash"], "code": row["code"]}
    for i, source in enumerate(sources):
        database = identifier(source["database"])
        for group, fields in [("balances", ["value"]), ("nonces", ["value"]), ("codes", ["hash", "code"])]:
            selection = ", ".join(f"argMax({group}.{field}, tuple(number, {group}.ordinal)) AS {field}" for field in fields)
            for row in client.rows(
                f"SELECT {group}.address AS address, {selection} FROM {database}.state_blocks FINAL ARRAY JOIN {group} "
                f"WHERE number >= {{start{i}:UInt64}} AND number <= {{end:UInt64}} GROUP BY address", params,
            ):
                out = result.setdefault(row["address"], {})
                if group == "balances": out["balance"] = row["value"]
                elif group == "nonces": out["nonce"] = int(row["value"])
                else: out.update(code_hash=row["hash"], code=row["code"])
    return result


def build(client, bundle, sources, base_id=None, budget_bytes=100_000_000_000, work_dir=None):
    setup(client)
    with control(client).publisher(), ExitStack() as inputs:
        checked = [inputs.enter_context(verified_source(connect_like(client, source["database"]), source,
                                                       client, int(bundle["header"]["number"])))
                   for source in sources]
        return _build(client, bundle, checked, base_id, budget_bytes, work_dir)


def _build(client, bundle, sources, base_id=None, budget_bytes=100_000_000_000, work_dir=None):
    """Build from disjoint account cohorts and optionally a previous ready checkpoint.

    A source starts at creation for new accounts, or at base.number+1 for existing
    accounts. Root verification establishes complete initial storage; source range
    assumptions alone never mark an account ready.
    """
    if bundle.get("format_version") != 1 or bundle.get("chain_id") != 56:
        raise VerificationError("v0.1 qualification requires a BSC proof bundle")
    header = bundle["header"]
    target = int(header["number"])
    unhex(header["hash"], 32)
    unhex(header["state_root"], 32)
    if bundle.get("header_trust") not in {"provider-finalized-header", "operator-pinned-hash"}:
        raise VerificationError("unrecognized header trust source")
    if bundle.get("header_rlp"):
        verify_header(bundle["header_rlp"], header)
    elif bundle["header_trust"] == "operator-pinned-hash":
        raise VerificationError("a pinned block hash requires its encoded header")
    accounts = canonical_accounts(bundle["accounts"])
    base = _manifest(client, base_id) if base_id else None
    base_accounts = set(base["accounts"]) if base else set()
    if base and (target <= base["header"]["number"] or not base_accounts.issubset(accounts)):
        raise VerificationError("checkpoint must advance its base and preserve all base accounts")
    seen = set()
    source_bytes = 0
    sources = [dict(source) for source in sources]
    for source in sources:
        selected = set(canonical_accounts(source["accounts"]))
        if selected & seen:
            raise VerificationError("source cohorts overlap")
        seen |= selected
        if base and selected & base_accounts:
            if int(source["start_block"]) != base["header"]["number"] + 1:
                raise VerificationError("existing accounts must continue immediately after the base checkpoint")
            source["parent_hash"] = base["header"]["hash"]
        db = connect_like(client, source["database"])
        validate_interval(db, source, header)
        if source["database"] != client.database:
            source_bytes += db.disk_usage()
    if seen != set(accounts):
        raise VerificationError("source account coverage differs from proof bundle")
    if client.disk_usage() + source_bytes >= budget_bytes:
        raise VerificationError("retained-data budget already exhausted")

    snapshot_id = uuid.uuid4().hex
    params = {"id": snapshot_id, "end": target}
    query = _union_storage(sources, base, client, params)
    # The zero filter follows argMax, so a clear cannot resurrect an older value.
    client.execute(
        "INSERT INTO checkpoint_storage SELECT {id:String}, address, slot, argMax(value, position) AS final_value "
        f"FROM ({query}) GROUP BY address, slot HAVING final_value != '{ZERO}'", params,
    )
    observed = _observed_fields(client, sources, base, params)
    account_rows, verification = [], {}
    work_dir = Path(work_dir or tempfile.gettempdir())
    work_dir.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256()
    for account_address in accounts:
        evidence = bundle["accounts"][account_address]
        proven = verify_account(header["state_root"], account_address, evidence["proof"])
        # Only fields not observed in the update interval may be filled from proof.
        # A changed-but-wrong nonce/balance/code remains a hard verification failure.
        metadata = {**proven.json(), **observed.get(account_address, {})}
        code = metadata.get("code", evidence["code"])
        params["address"] = account_address
        def slots():
            for row in client.rows("SELECT slot,value FROM checkpoint_storage FINAL "
                                  "WHERE snapshot_id={id:String} AND address={address:String} ORDER BY slot", params):
                digest.update((account_address + row["slot"] + row["value"]).encode())
                yield row["slot"], row["value"]
        with tempfile.TemporaryDirectory(prefix="evm-state-trie-", dir=work_dir) as directory:
            trie_db = TrieDB(Path(directory) / "nodes.sqlite")
            try:
                count = verify_complete(proven, slots(), code, metadata, trie_db)
            finally:
                trie_db.close()
        row = {"snapshot_id": snapshot_id, **proven.json(), "code": code, "nonzero_slots": count}
        digest.update(json.dumps({k: v for k, v in row.items() if k != "snapshot_id"},
                                 sort_keys=True, separators=(",", ":")).encode())
        account_rows.append(row)
        verification[account_address] = {"nonzero_slots": count, "account_proof": "verified", "storage_root": "verified"}
        if client.disk_usage() + source_bytes >= budget_bytes:
            raise VerificationError("checkpoint exceeds retained-data budget; candidate remains unpublished")
    expected_slots = sum(v["nonzero_slots"] for v in verification.values())
    actual_slots = int(client.one("SELECT count() AS count FROM checkpoint_storage FINAL "
                                 "WHERE snapshot_id={id:String}", params)["count"])
    if actual_slots != expected_slots:
        raise VerificationError("checkpoint contains unverified or unexpected storage accounts")
    if set(observed) - set(accounts):
        raise VerificationError("stream contains account metadata outside its declared filter")
    client.insert("checkpoint_accounts", account_rows)
    record = {
        "format_version": 1, "snapshot_id": snapshot_id, "status": "ready", "chain_id": bundle["chain_id"],
        "header": header, "header_trust": bundle["header_trust"], "accounts": accounts,
        "base_snapshot": base_id, "sources": sources, "verification": verification,
        "state_sha256": digest.hexdigest(), "proof_bundle": bundle,
        "created_at": time.time_ns(), "retained_budget_bytes": budget_bytes,
        "account_count": len(accounts), "nonzero_slots": sum(v["nonzero_slots"] for v in verification.values()),
    }
    # No public ready row exists before every account passes. One insert publishes
    # the complete immutable generation; a failed candidate cannot change the head.
    client.insert("checkpoints", [{"snapshot_id": snapshot_id, "block_number": target,
        "block_hash": header["hash"], "created_at": record["created_at"],
        "manifest": json.dumps(record, sort_keys=True, separators=(",", ":"))}])
    return record


def read_account(client, snapshot_id, account_address):
    with control(client).reader():
        return _read_account(client, snapshot_id, account_address)


def _read_account(client, snapshot_id, account_address):
    published = _manifest(client, snapshot_id)
    account_address = address(account_address)
    if account_address not in published["accounts"]:
        raise VerificationError("account is not ready in this checkpoint")
    params = {"id": snapshot_id, "address": account_address}
    row = client.one("SELECT address, exists, nonce, balance, code_hash, code, storage_root, nonzero_slots "
        "FROM checkpoint_accounts FINAL WHERE snapshot_id={id:String} AND address={address:String}", params)
    return {"snapshot_id": snapshot_id, "header": published["header"], **row}
