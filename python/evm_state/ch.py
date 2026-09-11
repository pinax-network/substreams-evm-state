"""Small streaming HTTP client; the native Substreams CLI owns block ingestion."""
import base64
import json
import os
import re
import urllib.error
import urllib.parse
import urllib.request


def identifier(name: str) -> str:
    if not re.fullmatch(r"[a-zA-Z_][a-zA-Z0-9_]*", name):
        raise ValueError("invalid ClickHouse database/table identifier")
    return name


class ClickHouse:
    def __init__(self, database="evm_state", url=None, user=None, password=None):
        self.database = identifier(database)
        self.url = url or os.environ.get("CH_HTTP_URL", "http://127.0.0.1:18123")
        self.user = user if user is not None else os.environ.get("CH_USER", "evm_state")
        self.password = password if password is not None else os.environ.get("CH_PASSWORD", "local-development-only")

    def request(self, sql, params=None, body=None):
        query = {"database": self.database, "query": sql}
        query.update({"param_" + k: str(v) for k, v in (params or {}).items()})
        url = self.url.rstrip("/") + "/?" + urllib.parse.urlencode(query)
        auth = base64.b64encode((self.user + ":" + self.password).encode()).decode()
        request = urllib.request.Request(url, data=body or b"", headers={"Authorization": "Basic " + auth})
        try:
            return urllib.request.urlopen(request, timeout=300)
        except urllib.error.HTTPError as error:
            message = error.read(8000).decode(errors="replace")
            raise RuntimeError(f"ClickHouse query failed ({error.code}): {message}") from None

    def execute(self, sql, params=None):
        with self.request(sql, params) as response:
            return response.read().decode()

    def rows(self, sql, params=None):
        with self.request(sql + " FORMAT JSONEachRow", params) as response:
            for line in response:
                if line.strip():
                    yield json.loads(line)

    def one(self, sql, params=None):
        rows = list(self.rows(sql, params))
        if len(rows) != 1:
            raise ValueError(f"expected one result row, got {len(rows)}")
        return rows[0]

    def insert(self, table, rows, batch_size=1000):
        table = identifier(table)
        batch = []
        for row in rows:
            batch.append(json.dumps(row, separators=(",", ":")))
            if len(batch) == batch_size:
                self._insert_batch(table, batch)
                batch.clear()
        if batch:
            self._insert_batch(table, batch)

    def _insert_batch(self, table, lines):
        with self.request(f"INSERT INTO {table} FORMAT JSONEachRow", body=("\n".join(lines) + "\n").encode()) as response:
            response.read()

    def disk_usage(self):
        return int(self.one("SELECT sum(bytes_on_disk) AS bytes FROM system.parts WHERE database = {db:String}",
                            {"db": self.database})["bytes"])
