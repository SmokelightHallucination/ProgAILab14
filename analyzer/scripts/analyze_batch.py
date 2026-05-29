#!/usr/bin/env python3
"""Batch analysis entry point: Parquet → Polars/DuckDB → figures.

    python -m scripts.analyze_batch [parquet_path]

Reads the aggregates Parquet written by the Go collector, cleans and validates
it (Rust via PyO3), runs the same ranking in Polars and DuckDB with timing, and
renders the visualisations into docs/figures/.
"""

from __future__ import annotations

import sys

import polars as pl

from aqanalyzer import batch, viz, viz_static
from aqanalyzer.config import CONFIG
from aqanalyzer.validation import BACKEND

pl.Config.set_tbl_rows(20)


def main() -> int:
    path = sys.argv[1] if len(sys.argv) > 1 else CONFIG.parquet_path
    print(f"== Air-quality batch analysis ==")
    print(f"source parquet : {path}")
    print(f"validator      : {BACKEND}\n")

    raw = batch.load(path)
    print(f"loaded {raw.height} aggregate rows, {raw.width} columns")
    print(raw.head(5))

    clean = batch.clean(raw)
    dropped = raw.height - clean.height
    print(f"\nafter clean/validate: {clean.height} rows ({dropped} dropped)\n")

    print("-- pollutant summary (Polars) --")
    print(batch.pollutant_summary(clean))

    print("\n-- AQI category breakdown --")
    print(batch.aqi_category_breakdown(clean))

    polars_res = batch.worst_stations_polars(clean)
    duck_res = batch.worst_stations_duckdb(path)
    print(f"\n-- worst PM2.5 stations (Polars, {polars_res.seconds * 1e3:.1f} ms) --")
    print(polars_res.frame)
    print(f"\n-- worst PM2.5 stations (DuckDB, {duck_res.seconds * 1e3:.1f} ms) --")
    print(duck_res.frame)
    print(
        f"\nengine timing: Polars {polars_res.seconds * 1e3:.1f} ms vs "
        f"DuckDB {duck_res.seconds * 1e3:.1f} ms"
    )

    figures = viz.render_all(clean, CONFIG.figures_dir)        # interactive HTML
    figures += viz_static.render_all(clean, CONFIG.figures_dir)  # static PNG
    print(f"\nwrote {len(figures)} figure file(s) to {CONFIG.figures_dir}/:")
    for f in figures:
        print(f"  - {f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
