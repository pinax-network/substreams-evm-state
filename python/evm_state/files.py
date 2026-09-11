"""Durable local metadata on the same persistent volume as the native sink state."""
from contextlib import contextmanager
import fcntl
import json
import os
from pathlib import Path
import tempfile


def atomic_write(path, data, overwrite=False):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix="." + path.name + ".", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as output:
            output.write(data)
            output.flush()
            os.fsync(output.fileno())
        if overwrite:
            os.replace(temporary, path)
        else:
            os.link(temporary, path)  # Atomic create; cannot overwrite another writer's result.
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def atomic_json(path, value, overwrite=False):
    atomic_write(path, (json.dumps(value, sort_keys=True, indent=2) + "\n").encode(), overwrite)


@contextmanager
def exclusive_lock(path):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise ValueError("another process owns this state directory") from None
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
