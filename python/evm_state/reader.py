"""Pin an immutable checkpoint and page its complete nonzero storage."""
import base64
import json
import os
import time
import uuid

from .checkpoint import _manifest, _read_account
from .control import control, object_id
from .files import atomic_json
from .proof import VerificationError, address, unhex


def pin(client, snapshot_id, purpose="reader"):
    owner = control(client)
    with owner.reader():
        ready = _manifest(client, snapshot_id)
        result = {"pin_id": uuid.uuid4().hex, "snapshot_id": snapshot_id, "control_id": owner.record["control_id"],
                  "created_at": time.time_ns(), "purpose": purpose, "header": ready["header"]}
        atomic_json(owner.pins / (result["pin_id"] + ".json"), result)
        return result


def _pin(owner, pin_id):
    record = json.loads((owner.pins / (object_id(pin_id) + ".json")).read_text())
    if record.get("pin_id") != pin_id or record.get("control_id") != owner.record["control_id"]:
        raise VerificationError("pin belongs to another controller or is corrupt")
    object_id(record["snapshot_id"])
    return record


def unpin(client, pin_id):
    owner = control(client)
    with owner.reader():
        record = _pin(owner, pin_id)
        (owner.pins / (pin_id + ".json")).unlink()
        fd = os.open(owner.pins, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
        return {"pin_id": pin_id, "snapshot_id": record["snapshot_id"], "released": True}


def list_pins(client):
    owner = control(client)
    with owner.reader():
        return sorted((_pin(owner, path.stem) for path in owner.pins.glob("*.json")), key=lambda value: value["created_at"])


def _encode_cursor(snapshot_id, account, slot):
    value = {"version": 1, "snapshot_id": snapshot_id, "address": account, "after_slot": slot}
    return base64.urlsafe_b64encode(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).decode()


def _decode_cursor(token, snapshot_id, account):
    try:
        if len(token) > 1024:
            raise ValueError("oversized cursor")
        value = json.loads(base64.b64decode(token, altchars=b"-_", validate=True))
        if value.get("version") != 1 or value.get("snapshot_id") != snapshot_id or value.get("address") != account:
            raise ValueError("cursor belongs to another checkpoint or account")
        unhex(value["after_slot"], 32)
        return value["after_slot"]
    except (ValueError, KeyError, TypeError) as error:
        raise VerificationError("invalid or mismatched storage cursor") from error


def page(client, pin_id, account, cursor=None, limit=1000):
    if isinstance(limit, bool) or not 1 <= limit <= 10000:
        raise ValueError("page limit must be between 1 and 10000")
    owner = control(client)
    account = address(account)
    with owner.reader():
        pinned = _pin(owner, pin_id)
        snapshot_id = pinned["snapshot_id"]
        metadata = _read_account(client, snapshot_id, account)
        metadata["nonce"] = str(int(metadata["nonce"]))
        after = _decode_cursor(cursor, snapshot_id, account) if cursor else ""
        rows = list(client.rows("SELECT slot,value FROM checkpoint_storage FINAL WHERE snapshot_id={id:String} "
            "AND address={address:String} AND slot>{after:String} ORDER BY slot LIMIT {limit:UInt32}",
            {"id": snapshot_id, "address": account, "after": after, "limit": limit + 1}))
        has_more = len(rows) > limit
        rows = rows[:limit]
        return {"pin_id": pin_id, "snapshot_id": snapshot_id, "header": metadata["header"], "account": metadata,
                "storage": rows, "next_cursor": _encode_cursor(snapshot_id, account, rows[-1]["slot"]) if has_more else None}
