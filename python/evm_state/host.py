"""Stable local machine identity, with explicit recovery of legacy host names."""
import hashlib
import json
from pathlib import Path
import platform
import re
import socket
import subprocess

from .proof import VerificationError


def machine_id():
    # Network/DHCP host names can change during a long replay. Do not fall back
    # to one when a persistent OS identity is unavailable.
    system = platform.system()
    try:
        if system == "Linux":
            value = Path("/etc/machine-id").read_text().strip().lower()
            if not re.fullmatch(r"[0-9a-f]{32}", value) or int(value, 16) == 0:
                raise ValueError("invalid machine ID")
        elif system == "Darwin":
            result = subprocess.run(["/usr/sbin/ioreg", "-rd1", "-c", "IOPlatformExpertDevice"],
                                    capture_output=True, text=True, check=True, timeout=10)
            match = re.search(r'"IOPlatformUUID"\s*=\s*"([0-9A-Fa-f-]{36})"', result.stdout)
            if not match:
                raise ValueError("missing platform UUID")
            value = match[1].lower()
            if not re.fullmatch(r"[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}", value) or not int(value.replace("-", ""), 16):
                raise ValueError("invalid platform UUID")
        else:
            raise ValueError("unsupported operating system")
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        raise VerificationError("persistent local machine identity is unavailable") from error
    return "machine-sha256:" + hashlib.sha256((system + ":" + value).encode()).hexdigest()


def recovery_record(record, directory):
    return {"format_version": 1, "original_host": record["host"],
            "record_sha256": hashlib.sha256(json.dumps(record, sort_keys=True, separators=(",", ":")).encode()).hexdigest(),
            "directory": str(Path(directory).resolve()), "machine_id": machine_id()}


def matches(record, directory):
    expected = record.get("host")
    if not isinstance(expected, str) or not expected:
        return False
    if expected.startswith("machine-sha256:"):
        return expected == machine_id()
    # Existing prototype records retain their identity and cursor hashes. A
    # separate, explicit local recovery attestation can bind them to this OS.
    recovery = Path(directory) / "host-rebinding.json"
    if recovery.exists():
        try:
            return json.loads(recovery.read_text()) == recovery_record(record, directory)
        except (ValueError, OSError):
            return False
    return expected == socket.gethostname()
