"""Temporary disk-backed trie nodes; avoid retaining all account storage in RAM."""
from collections.abc import MutableMapping
import sqlite3


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
