"""Disposable SQLite workspaces for storage-root reconstruction."""
from collections.abc import MutableMapping
import sqlite3


class StorageSortDB:
    """Stage hashed slots once, then stream them in key order.

    This is a single-use sorting workspace, not a trie-node database or a
    portable checkpoint. A failed or completed reconstruction cannot reuse it.
    SQLite is configured with a suggested 16 MiB page-cache limit; the complete
    table is committed to disk before the ordered scan.
    """
    def __init__(self, path):
        self.connection = sqlite3.connect(path)
        try:
            self.connection.execute("PRAGMA journal_mode=OFF")
            self.connection.execute("PRAGMA synchronous=OFF")
            self.connection.execute("PRAGMA cache_size=-16384")
            self.connection.execute("CREATE TABLE storage (key BLOB PRIMARY KEY, value BLOB NOT NULL) WITHOUT ROWID")
        except BaseException:
            self.connection.close()
            raise
        self.state = "new"

    def begin(self):
        if self.state != "new":
            raise ValueError("storage sorting workspace must be fresh")
        self.state = "loading"

    def add(self, key, value):
        if self.state != "loading":
            raise ValueError("storage sorting workspace is not loading")
        try:
            self.connection.execute("INSERT INTO storage VALUES (?,?)", (key, value))
        except sqlite3.IntegrityError as error:
            raise KeyError("duplicate hashed storage key") from error

    def ordered_entries(self):
        if self.state != "loading":
            raise ValueError("storage sorting workspace is not loading")
        self.state = "sealed"
        # Flush the page cache before hashing and measuring the workspace. The
        # file is disposable even though this transaction is committed.
        self.connection.commit()
        return self.connection.execute("SELECT key,value FROM storage ORDER BY key")

    def __len__(self):
        return self.connection.execute("SELECT count(*) FROM storage").fetchone()[0]

    def close(self):
        self.state = "closed"
        self.connection.close()


class TrieDB(MutableMapping):
    def __init__(self, path):
        self.connection = sqlite3.connect(path)
        self.connection.execute("PRAGMA journal_mode=OFF")
        self.connection.execute("PRAGMA synchronous=OFF")
        self.connection.execute("PRAGMA cache_size=-16384")
        self.connection.execute("CREATE TABLE IF NOT EXISTS nodes (key BLOB PRIMARY KEY, value BLOB NOT NULL) WITHOUT ROWID")

    def __getitem__(self, key):
        row = self.connection.execute("SELECT value FROM nodes WHERE key=?", (key,)).fetchone()
        if row is None:
            raise KeyError(key)
        return row[0]

    def __setitem__(self, key, value):
        self.connection.execute("INSERT OR REPLACE INTO nodes VALUES (?,?)", (key, value))

    def __delitem__(self, key):
        if self.connection.execute("DELETE FROM nodes WHERE key=?", (key,)).rowcount == 0:
            raise KeyError(key)

    def __iter__(self):
        return (row[0] for row in self.connection.execute("SELECT key FROM nodes"))

    def __len__(self):
        return self.connection.execute("SELECT count(*) FROM nodes").fetchone()[0]

    def close(self):
        self.connection.close()
