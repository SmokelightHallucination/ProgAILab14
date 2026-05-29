"""Asyncio/aiohttp air-quality collector mirroring the Go collector's logic.

It generates the same synthetic measurements, validates them, runs a tumbling
window and writes Parquet — so a like-for-like benchmark (throughput, CPU, RSS)
against the Go version is fair. Kept deliberately close in structure to
collector/main.go.
"""

from __future__ import annotations

import asyncio
import math
import random
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

import polars as pl

PARAMETERS = ["pm25", "pm10", "no2", "o3", "so2", "co"]
UNITS = {"pm25": "µg/m³", "pm10": "µg/m³", "no2": "µg/m³", "o3": "µg/m³", "so2": "µg/m³", "co": "mg/m³"}

# Same station catalogue as the Go synthetic source (kept in sync by hand).
SEEDS = [
    ("Moscow Centre", "Moscow", "RU", 55.751, 37.618, {"pm25": 14, "pm10": 28, "no2": 45, "o3": 60, "so2": 8, "co": 1.2}),
    ("Beijing Chaoyang", "Beijing", "CN", 39.921, 116.443, {"pm25": 58, "pm10": 92, "no2": 55, "o3": 70, "so2": 14, "co": 1.8}),
    ("Delhi Anand Vihar", "Delhi", "IN", 28.650, 77.316, {"pm25": 95, "pm10": 160, "no2": 60, "o3": 45, "so2": 18, "co": 2.4}),
    ("Los Angeles N. Main", "Los Angeles", "US", 34.066, -118.227, {"pm25": 12, "pm10": 30, "no2": 38, "o3": 95, "so2": 4, "co": 0.9}),
    ("London Marylebone", "London", "GB", 51.522, -0.155, {"pm25": 13, "pm10": 24, "no2": 70, "o3": 50, "so2": 5, "co": 0.7}),
    ("Paris Châtelet", "Paris", "FR", 48.862, 2.347, {"pm25": 15, "pm10": 26, "no2": 52, "o3": 55, "so2": 6, "co": 0.8}),
    ("Tokyo Shinjuku", "Tokyo", "JP", 35.690, 139.700, {"pm25": 11, "pm10": 21, "no2": 40, "o3": 48, "so2": 4, "co": 0.6}),
    ("Mexico City Merced", "Mexico City", "MX", 19.424, -99.119, {"pm25": 24, "pm10": 48, "no2": 50, "o3": 80, "so2": 9, "co": 1.5}),
    ("São Paulo Ibirapuera", "São Paulo", "BR", -23.591, -46.660, {"pm25": 18, "pm10": 35, "no2": 44, "o3": 65, "so2": 7, "co": 1.0}),
    ("Cairo Maadi", "Cairo", "EG", 29.960, 31.276, {"pm25": 75, "pm10": 140, "no2": 58, "o3": 40, "so2": 20, "co": 2.0}),
    ("Sydney Rozelle", "Sydney", "AU", -33.864, 151.171, {"pm25": 8, "pm10": 18, "no2": 28, "o3": 52, "so2": 3, "co": 0.5}),
    ("Krakow Aleja", "Krakow", "PL", 50.058, 19.926, {"pm25": 32, "pm10": 55, "no2": 48, "o3": 42, "so2": 11, "co": 1.3}),
]

_RANGES = {"pm25": 1000, "pm10": 2000, "no2": 3000, "o3": 1000, "so2": 3000, "co": 100}


@dataclass
class Bucket:
    count: int = 0
    total: float = 0.0
    lo: float = math.inf
    hi: float = -math.inf
    meta: dict = field(default_factory=dict)


def _validate(parameter: str, value: float) -> bool:
    rng = _RANGES.get(parameter)
    if rng is None:
        return False
    return math.isfinite(value) and 0 <= value <= rng


async def _fetch(seed) -> list[dict]:
    """Emulate one async source poll for a station (no real network needed)."""
    name, city, country, lat, lon, baseline = seed
    now = datetime.now(timezone.utc)
    hour = now.hour + now.minute / 60
    diurnal = 1 + 0.35 * math.cos((hour - 8) / 24 * 2 * math.pi) + 0.25 * math.cos((hour - 19) / 12 * 2 * math.pi)
    out = []
    for p in PARAMETERS:
        base = baseline[p]
        val = max(0.0, base * diurnal + random.gauss(0, base * 0.15))
        if random.random() < 0.005:
            val = base * 50
        out.append(dict(location=name, city=city, country=country, parameter=p,
                        value=round(val, 2), unit=UNITS[p], latitude=lat, longitude=lon))
    await asyncio.sleep(0)  # yield to the event loop, like an awaited HTTP call
    return out


async def collect(duration_s: float, poll_interval_s: float = 0.5, window_s: float = 10.0,
                  out_path: str | None = None) -> dict:
    """Run the collector for ``duration_s`` and return benchmark stats."""
    buckets: dict[tuple[str, str], Bucket] = {}
    flushed_rows: list[dict] = []
    raw_count = 0
    invalid_count = 0
    win_start = datetime.now(timezone.utc)

    start = time.perf_counter()
    next_poll = start
    while time.perf_counter() - start < duration_s:
        polls = await asyncio.gather(*[_fetch(s) for s in SEEDS])
        for station_readings in polls:
            for m in station_readings:
                raw_count += 1
                if not _validate(m["parameter"], m["value"]):
                    invalid_count += 1
                    continue
                key = (m["location"], m["parameter"])
                b = buckets.get(key)
                if b is None:
                    b = Bucket(meta=m)
                    buckets[key] = b
                b.count += 1
                b.total += m["value"]
                b.lo = min(b.lo, m["value"])
                b.hi = max(b.hi, m["value"])

        # Tumbling-window flush.
        now = datetime.now(timezone.utc)
        if (now - win_start).total_seconds() >= window_s:
            flushed_rows.extend(_flush(buckets, win_start, now))
            buckets.clear()
            win_start = now

        # poll_interval_s <= 0 → saturate mode: collect as fast as possible
        # (used by the benchmark to measure real throughput / CPU, not the
        # configured polling cadence).
        if poll_interval_s > 0:
            next_poll += poll_interval_s
            await asyncio.sleep(max(0.0, next_poll - time.perf_counter()))
        else:
            await asyncio.sleep(0)

    flushed_rows.extend(_flush(buckets, win_start, datetime.now(timezone.utc)))
    elapsed = time.perf_counter() - start

    if out_path and flushed_rows:
        pl.DataFrame(flushed_rows).write_parquet(out_path)

    return {
        "elapsed_s": round(elapsed, 3),
        "raw_measurements": raw_count,
        "invalid": invalid_count,
        "aggregates": len(flushed_rows),
        "throughput_msg_s": round(raw_count / elapsed, 1) if elapsed else 0,
    }


def _flush(buckets, win_start, win_end) -> list[dict]:
    rows = []
    for (location, parameter), b in buckets.items():
        avg = b.total / b.count
        rows.append(dict(
            location_id="", location=location, city=b.meta["city"], country=b.meta["country"],
            parameter=parameter, unit=b.meta["unit"], latitude=b.meta["latitude"],
            longitude=b.meta["longitude"], window_start=win_start, window_end=win_end,
            count=b.count, sum=round(b.total, 2), avg=round(avg, 2), min=b.lo, max=b.hi,
        ))
    return rows
