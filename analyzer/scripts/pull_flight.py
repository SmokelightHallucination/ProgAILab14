#!/usr/bin/env python3
"""Pull aggregates from the collector's Arrow Flight server (zero-copy).

    python -m scripts.pull_flight
"""

from __future__ import annotations

from aqanalyzer.config import CONFIG
from aqanalyzer.flight_client import pull, transfer_stats


def main() -> int:
    print(f"connecting to Flight grpc://{CONFIG.flight_host}:{CONFIG.flight_port}")
    df = pull(CONFIG.flight_host, CONFIG.flight_port)
    if df.is_empty():
        print("no records buffered yet on the server")
        return 0
    print(f"received {df.height} rows via Arrow Flight")
    print(df.head(10))
    stats = transfer_stats(CONFIG.flight_host, CONFIG.flight_port)
    print(f"\ntransfer stats: {stats['rows']} rows, {stats['arrow_bytes']} Arrow bytes")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
