"""Air-quality analysis package (variant 20, Python side of the pipeline).

Consumes the window aggregates produced by the Go collector — from a Parquet
file (batch), an Arrow Flight stream (zero-copy), or a Kafka topic (real-time) —
and turns them into Polars/DuckDB analyses and visualisations.
"""

import sys as _sys

# Windows consoles default to cp1251 here, which cannot encode the µg/m³ units
# in our data. Force UTF-8 on stdout/stderr so analysis output prints cleanly.
for _stream in (_sys.stdout, _sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8")  # type: ignore[union-attr]
    except (AttributeError, ValueError):
        pass

__all__ = ["config", "validation", "batch", "viz", "stream", "flight_client"]
