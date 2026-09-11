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
    with file_lock(path, exclusive=True, blocking=False) as handle:
        yield handle


@contextmanager
def file_lock(path, exclusive=False, blocking=True):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+") as handle:
        try:
            mode = fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH
            fcntl.flock(handle.fileno(), mode | (0 if blocking else fcntl.LOCK_NB))
        except BlockingIOError:
            raise ValueError("another process owns this state directory") from None
        try:
            yield handle
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
