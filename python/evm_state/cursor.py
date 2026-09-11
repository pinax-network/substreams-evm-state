"""Decode pinned Substreams cursors and bind durable progress to native rows.

Compatibility: bstream a513e03b7ada/cursor.go, opaque 0c01d37ea308/keys.go.
The upstream key and nonce below are PUBLIC obfuscation constants, not secrets
or an authentication mechanism. A decoded cursor alone proves no database state.
"""
import base64
import hashlib
import json
from pathlib import Path
import re

from nacl.exceptions import CryptoError
from nacl.secret import SecretBox

from .files import atomic_json
from .proof import VerificationError

PUBLIC_KEY = bytes.fromhex("7bfcacee257409099edd2bb6a442638b559a80bfbfc0b9acdea0d8344b10eb00")
PUBLIC_NONCE = bytes.fromhex("261554c45ab9b752abad4f19c242605702d55a0d91616a1b")


def decode(token):
    try:
        if (not isinstance(token, str) or not 1 <= len(token) <= 2048
                or not re.fullmatch(r"[A-Za-z0-9_-]+={0,2}", token)):
            raise ValueError("invalid cursor length")
        payload = SecretBox(PUBLIC_KEY).decrypt(base64.b64decode(token, altchars=b"-_", validate=True), PUBLIC_NONCE)
        parts = payload.decode("ascii").split(":")
        if not parts or (parts[0] in {"c1", "c2"} and len(parts) != 6) or (parts[0] == "c3" and len(parts) != 8):
            raise ValueError("invalid cursor segments")
        if parts[0] not in {"c1", "c2", "c3"} or parts[1] not in {"1", "16", "17"}:
            raise ValueError("unsupported cursor format or step")
        def ref(offset):
            number, block_hash = parts[offset:offset + 2]
            if not re.fullmatch(r"0|[1-9][0-9]*", number) or int(number) >= 2**64 or not re.fullmatch(r"[0-9a-f]{64}", block_hash):
                raise ValueError("invalid BSC block reference")
            return {"number": int(number), "hash": "0x" + block_hash}
        block = ref(2)
        head = block if parts[0] == "c1" else ref(4)
        lib = block if parts[0] == "c2" else ref(4 if parts[0] == "c1" else 6)
        if lib != block or head["number"] < block["number"] or (head["number"] == block["number"] and head != block):
            raise ValueError("cursor is not on a finalized block")
        return {"block": block, "head": head, "lib": lib, "step": int(parts[1])}
    except (ValueError, IndexError, CryptoError) as error:
        raise VerificationError("invalid or non-finalized native cursor") from error


def validate(client, run, token):
    position = decode(token)
    block = position["block"]
    identity = run["identity"]
    if block["number"] < identity["start_block"]:
        raise VerificationError("native cursor precedes its run's start")
    rows = list(client.rows("SELECT hash,accounts,schema_version,producer_version FROM state_blocks FINAL "
                           "WHERE number={number:UInt64}", {"number": block["number"]}))
    markers = list(client.rows("SELECT hash FROM _blocks_ FINAL WHERE number={number:UInt64}",
                              {"number": block["number"]}))
    if (len(rows) != 1 or rows[0]["hash"] != block["hash"] or rows[0]["accounts"] != ",".join(identity["accounts"])
            or int(rows[0]["schema_version"]) != 1 or int(rows[0]["producer_version"]) not in {3, 4, 5}
            or len(markers) != 1 or "0x" + markers[0]["hash"].removeprefix("0x") != block["hash"]):
        raise VerificationError("native cursor does not match its complete block data and marker")
    return position


def binding(run):
    identity_hash = hashlib.sha256(json.dumps(run["identity"], sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    return {"format_version": 1, "run_id": run["run_id"], "database_uuid": run["database_uuid"],
            "identity_sha256": identity_hash}


def load_progress(client, run, directory):
    try:
        record = json.loads((Path(directory) / "durable_progress.json").read_text())
        if not isinstance(record, dict) or any(record.get(key) != value for key, value in binding(run).items()):
            raise VerificationError("durable progress belongs to another native run")
        position = validate(client, run, record["cursor"])
        if record.get("position") != position:
            raise VerificationError("durable progress position is corrupt")
        return record
    except (OSError, KeyError, TypeError) as error:
        raise VerificationError("durable progress is missing or invalid; restore matching run metadata") from error


def save_progress(client, run, directory, token):
    directory = Path(directory)
    position = validate(client, run, token)
    path = directory / "durable_progress.json"
    if path.exists():
        previous = load_progress(client, run, directory)
        if previous["position"]["block"]["number"] > position["block"]["number"]:
            raise VerificationError("native cursor regressed behind durable progress")
        if previous["cursor"] == token:
            return previous
    record = {**binding(run), "cursor": token, "position": position}
    atomic_json(path, record, overwrite=True)
    return record


def observe(client, run, directory):
    """Save a valid cursor while its native writer may replace the cursor file.

    A missing/torn native file is transient. Once decoded, mismatched database
    rows or a failed durable write are hard failures, not silently skipped.
    """
    try:
        with (Path(directory) / "cursor.txt").open() as handle:
            token = handle.read(2049).strip()
    except FileNotFoundError:
        return None
    try:
        decode(token)
    except VerificationError:
        return None
    return save_progress(client, run, directory, token)
