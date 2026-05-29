"""Apache Arrow Flight client (advanced task #3, Python side).

Pulls the collector's in-memory window records over Flight DoGet and returns
them as a Polars frame — a zero-copy, columnar alternative to the JSON/Parquet
hand-off, useful for the Go-vs-Python transfer comparison.
"""

from __future__ import annotations

import polars as pl
import pyarrow as pa
from pyarrow import flight


def pull(host: str = "localhost", port: int = 8815) -> pl.DataFrame:
    """Fetch all buffered aggregate records from the Flight server."""
    client = flight.connect(f"grpc://{host}:{port}")
    ticket = flight.Ticket(b"aggregates")
    reader = client.do_get(ticket)
    table: pa.Table = reader.read_all()
    if table.num_rows == 0:
        return pl.DataFrame()
    return pl.from_arrow(table)


def transfer_stats(host: str = "localhost", port: int = 8815) -> dict:
    """Pull once and report row/byte counts for the transfer comparison."""
    df = pull(host, port)
    if df.is_empty():
        return {"rows": 0, "arrow_bytes": 0}
    table = df.to_arrow()
    return {"rows": table.num_rows, "arrow_bytes": table.nbytes}
