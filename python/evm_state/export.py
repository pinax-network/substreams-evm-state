"""Portable, paginated, independently verifiable checkpoint files.

manifest.json is written last, after checking every exported slot and proof.
The verifier uses only these files and can be run without database/RPC access.
"""
import gzip
import hashlib
import json
import os
from pathlib import Path
import re
import tempfile

from .checkpoint import _manifest, canonical_accounts
from .control import control, object_id
from .files import atomic_json
from .header import verify_header
from .proof import VerificationError, address, unhex, verify_account, verify_complete
from .triedb import TrieDB
from .capacity import check as capacity_check
from .ch import ClickHouse

FORMAT = "evm-state-checkpoint-v1"
MAX_JSON_BYTES = 64 * 1024 * 1024
ACCOUNT_FIELDS = {"address", "exists", "nonce", "balance", "code_hash", "code", "storage_root", "nonzero_slots"}


def file_hash(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _file(directory, name):
    if not isinstance(name, str) or not re.fullmatch(r"accounts\.json|storage-[0-9]{6}\.jsonl\.gz", name):
        raise VerificationError("invalid checkpoint export filename")
    path = directory / name
    if path.is_symlink() or not path.is_file():
        raise VerificationError("export file is missing or is a symlink")
    return path


def _json(path):
    if path.is_symlink() or path.stat().st_size > MAX_JSON_BYTES:
        raise VerificationError("export metadata is a symlink or exceeds its size limit")
    return json.loads(path.read_bytes())


def _check_file(directory, item):
    path = _file(directory, item["file"])
    if path.stat().st_size != item["bytes"] or file_hash(path) != item["sha256"]:
        raise VerificationError("export file size or checksum mismatch")
    return path


def _write_page(directory, index, account, rows):
    name = f"storage-{index:06d}.jsonl.gz"
    path = directory / name
    with path.open("xb") as handle:
        with gzip.GzipFile(filename="", fileobj=handle, mode="wb", mtime=0) as output:
            for row in rows:
                output.write((json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n").encode())
        handle.flush()
        os.fsync(handle.fileno())
    return {"file": name, "address": account, "rows": len(rows), "bytes": path.stat().st_size,
            "sha256": file_hash(path), "first_slot": rows[0]["slot"], "last_slot": rows[-1]["slot"]}


def _page_rows(directory, item):
    path = _check_file(directory, item)
    if not isinstance(item["rows"], int) or isinstance(item["rows"], bool) or not 1 <= item["rows"] <= 10000:
        raise VerificationError("invalid export page row count")
    count, first, last = 0, None, None
    with gzip.open(path, "rb") as handle:
        while line := handle.readline(1025):
            if len(line) > 1024 or not line.endswith(b"\n"):
                raise VerificationError("oversized or truncated export storage row")
            row = json.loads(line)
            if set(row) != {"address", "slot", "value"} or row["address"] != item["address"]:
                raise VerificationError("export row has unexpected fields or account")
            unhex(row["slot"], 32); unhex(row["value"], 32)
            if last is not None and row["slot"] <= last:
                raise VerificationError("export page contains duplicate or unordered slots")
            first = row["slot"] if first is None else first
            last = row["slot"]
            count += 1
            if count > item["rows"]:
                raise VerificationError("export page exceeds its declared row count")
            yield row
    if count != item["rows"] or first != item["first_slot"] or last != item["last_slot"]:
        raise VerificationError("export page count or boundary mismatch")


def _verify(directory, layout, expected_hash=None, work_dir=None, capacity_client=None):
    # Without an explicit capacity policy this stays entirely offline.
    capacity_client = capacity_client or ClickHouse("default")
    capacity_paths = [directory, Path(work_dir or tempfile.gettempdir())]
    capacity_check(capacity_client, capacity_paths, "export-verify-start")
    if layout.get("format") != FORMAT or layout.get("status") != "ready":
        raise VerificationError("unsupported or incomplete checkpoint export")
    snapshot = layout["checkpoint"]
    if snapshot.get("status") != "ready" or snapshot.get("format_version") != 1 or snapshot.get("chain_id") != 56:
        raise VerificationError("invalid BSC checkpoint manifest")
    object_id(snapshot["snapshot_id"])
    bundle = snapshot["proof_bundle"]
    if bundle.get("chain_id") != 56 or bundle.get("format_version") != 1 or bundle["header"] != snapshot["header"]:
        raise VerificationError("checkpoint and proof bundle identities differ")
    if bundle.get("header_trust") not in {"operator-pinned-hash", "provider-finalized-header"} or bundle["header_trust"] != snapshot["header_trust"]:
        raise VerificationError("checkpoint header trust record is inconsistent")
    if not bundle.get("header_rlp"):
        raise VerificationError("portable exports require the encoded block header")
    verify_header(bundle["header_rlp"], snapshot["header"], expected_hash)
    selected = canonical_accounts(snapshot["accounts"])
    if selected != snapshot["accounts"] or selected != sorted(bundle["accounts"]):
        raise VerificationError("checkpoint account coverage differs from its proofs")
    metadata_file = _check_file(directory, layout["account_file"])
    accounts = _json(metadata_file)
    if not isinstance(accounts, list) or [r.get("address") for r in accounts] != selected:
        raise VerificationError("export account coverage is missing, duplicated or unordered")
    if len(accounts) != snapshot["account_count"]:
        raise VerificationError("export account count mismatch")
    pages = {account: [] for account in selected}
    last_account = ""
    for i, item in enumerate(layout["storage_pages"]):
        if item["file"] != f"storage-{i:06d}.jsonl.gz" or item["address"] not in pages or item["address"] < last_account:
            raise VerificationError("export page sequence or account is invalid")
        pages[item["address"]].append(item)
        last_account = item["address"]
    digest = hashlib.sha256()
    total_slots = 0
    work_dir = Path(work_dir or tempfile.gettempdir())
    work_dir.mkdir(parents=True, exist_ok=True)
    for metadata in accounts:
        if set(metadata) != ACCOUNT_FIELDS or not isinstance(metadata["exists"], bool):
            raise VerificationError("invalid exported account metadata")
        # JSON consumers such as Node.js must not round uint64 nonces through a
        # floating-point Number. The file uses decimal text; the trie uses ints.
        if not isinstance(metadata["nonce"], str) or not re.fullmatch(r"0|[1-9][0-9]*", metadata["nonce"]):
            raise VerificationError("export nonce must be exact decimal text")
        metadata = {**metadata, "nonce": int(metadata["nonce"])}
        account = address(metadata["address"])
        proof = bundle["accounts"][account]
        proven = verify_account(snapshot["header"]["state_root"], account, proof["proof"])
        if (metadata["exists"] != proven.exists or unhex(metadata["storage_root"], 32) != proven.storage_root
                or metadata["code"] != proof["code"]):
            raise VerificationError("exported metadata differs from its proven account")
        def slots():
            previous = None
            for item in pages[account]:
                for row in _page_rows(directory, item):
                    if previous is not None and row["slot"] <= previous:
                        raise VerificationError("duplicate or unordered slots across export pages")
                    previous = row["slot"]
                    digest.update((account + row["slot"] + row["value"]).encode())
                    yield row["slot"], row["value"]
        with tempfile.TemporaryDirectory(prefix="evm-export-verify-", dir=work_dir) as directory_name:
            database = TrieDB(Path(directory_name) / "trie.sqlite")
            try:
                count = verify_complete(proven, slots(), metadata["code"], metadata, database)
                capacity_check(capacity_client, capacity_paths, "export-verify-trie")
            finally:
                database.close()
        if count != metadata["nonzero_slots"]:
            raise VerificationError("exported account storage count mismatch")
        total_slots += count
        digest.update(json.dumps(metadata, sort_keys=True, separators=(",", ":")).encode())
    if total_slots != snapshot["nonzero_slots"] or digest.hexdigest() != snapshot["state_sha256"]:
        raise VerificationError("exported checkpoint count or state checksum mismatch")
    return {"snapshot_id": snapshot["snapshot_id"], "header": snapshot["header"],
            "header_trust": snapshot["header_trust"], "accounts": selected, "nonzero_slots": total_slots,
            "state_sha256": digest.hexdigest(), "verification": "account proofs, complete storage, code and header hash verified"}


def export_checkpoint(client, snapshot_id, directory, page_size=10000, work_dir=None):
    if not isinstance(page_size, int) or isinstance(page_size, bool) or not 1 <= page_size <= 10000:
        raise ValueError("export page_size must be between 1 and 10000")
    directory = Path(directory).resolve()
    with control(client).reader():
        capacity_paths = [directory, Path(work_dir or tempfile.gettempdir()), control(client).path]
        capacity_check(client, capacity_paths, "export-start")
        snapshot = _manifest(client, snapshot_id)
        if not snapshot["proof_bundle"].get("header_rlp"):
            raise VerificationError("portable exports require the encoded block header")
        directory.mkdir(parents=True, exist_ok=False)
        # Reserving a new directory excludes another writer. A failed attempt has
        # no manifest.json and is never accepted as a complete export.
        accounts = list(client.rows("SELECT address,exists,nonce,balance,code_hash,code,storage_root,nonzero_slots "
            "FROM checkpoint_accounts FINAL WHERE snapshot_id={id:String} ORDER BY address", {"id": snapshot_id}))
        for account in accounts:
            account["nonce"] = str(int(account["nonce"]))
            account["nonzero_slots"] = int(account["nonzero_slots"])
        atomic_json(directory / "accounts.json", accounts)
        metadata = directory / "accounts.json"
        layout = {"format": FORMAT, "status": "ready", "checkpoint": snapshot,
                  "account_file": {"file": "accounts.json", "bytes": metadata.stat().st_size, "sha256": file_hash(metadata)},
                  "storage_pages": []}
        for account in snapshot["accounts"]:
            pending = []
            for row in client.rows("SELECT address,slot,value FROM checkpoint_storage FINAL "
                    "WHERE snapshot_id={id:String} AND address={address:String} ORDER BY slot",
                    {"id": snapshot_id, "address": account}):
                pending.append(row)
                if len(pending) == page_size:
                    layout["storage_pages"].append(_write_page(directory, len(layout["storage_pages"]), account, pending))
                    pending = []
            if pending:
                layout["storage_pages"].append(_write_page(directory, len(layout["storage_pages"]), account, pending))
        result = _verify(directory, layout, work_dir=work_dir, capacity_client=client)
        capacity_check(client, capacity_paths, "export-publish")
        atomic_json(directory / "manifest.json", layout)
        return {**result, "directory": str(directory), "pages": len(layout["storage_pages"]),
                "bytes": sum(path.stat().st_size for path in directory.iterdir() if path.is_file())}


def verify_export(directory, expected_hash=None, work_dir=None):
    directory = Path(directory).resolve()
    try:
        return _verify(directory, _json(directory / "manifest.json"), expected_hash, work_dir)
    except VerificationError:
        raise
    except (ValueError, TypeError, KeyError, OSError, EOFError) as error:
        raise VerificationError("invalid or incomplete checkpoint export: " + str(error)) from error
