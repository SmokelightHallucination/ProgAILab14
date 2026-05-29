#!/usr/bin/env python3
"""Run the Python asyncio collector once and print benchmark stats as JSON.

    python -m scripts.run --duration 15 --out data/py_aggregates.parquet
"""

from __future__ import annotations

import argparse
import asyncio
import json

from aqcollector_py.collector import collect


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--duration", type=float, default=15.0)
    ap.add_argument("--poll", type=float, default=0.5)
    ap.add_argument("--window", type=float, default=10.0)
    ap.add_argument("--out", default=None)
    args = ap.parse_args()

    stats = asyncio.run(collect(args.duration, args.poll, args.window, args.out))
    print(json.dumps(stats))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
