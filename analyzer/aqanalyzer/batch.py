"""Batch analysis of collector aggregates with Polars and DuckDB.

Covers the base-assignment analysis steps on the air-quality domain:
load Parquet, clean/validate, aggregate (Polars), run the same query in SQL
(DuckDB) and compare timing.
"""

from __future__ import annotations

import time
from dataclasses import dataclass

import duckdb
import polars as pl

from .validation import validate_frame


@dataclass
class TimedResult:
    """A query result plus how long the engine took (for the perf report)."""

    name: str
    seconds: float
    frame: pl.DataFrame


def load(parquet_path: str) -> pl.DataFrame:
    """Load the aggregates Parquet written by the Go collector."""
    return pl.read_parquet(parquet_path)


def clean(df: pl.DataFrame) -> pl.DataFrame:
    """Drop duplicates, validate via the Rust library, keep only valid rows."""
    df = df.unique(subset=["location_id", "parameter", "window_start"])
    df = validate_frame(df)
    return df.filter(pl.col("valid"))


def worst_stations_polars(df: pl.DataFrame, top: int = 10) -> TimedResult:
    """Rank stations by mean PM2.5 AQI using Polars (lazy)."""
    t0 = time.perf_counter()
    out = (
        df.filter(pl.col("parameter") == "pm25")
        .group_by("location", "city", "country")
        .agg(
            pl.col("aqi").mean().round(1).alias("avg_aqi"),
            pl.col("avg").mean().round(2).alias("avg_pm25"),
            pl.col("max").max().alias("peak_pm25"),
            pl.len().alias("windows"),
        )
        .sort("avg_aqi", descending=True)
        .head(top)
    )
    return TimedResult("polars:worst_pm25_stations", time.perf_counter() - t0, out)


def worst_stations_duckdb(parquet_path: str, top: int = 10) -> TimedResult:
    """Same ranking in SQL against the Parquet file directly (DuckDB)."""
    t0 = time.perf_counter()
    con = duckdb.connect()
    rel = con.execute(
        """
        SELECT location, city, country,
               ROUND(AVG(aqi), 1)  AS avg_aqi,
               ROUND(AVG(avg), 2)  AS avg_pm25,
               MAX(max)            AS peak_pm25,
               COUNT(*)            AS windows
        FROM read_parquet(?)
        WHERE parameter = 'pm25'
        GROUP BY location, city, country
        ORDER BY avg_aqi DESC
        LIMIT ?
        """,
        [parquet_path, top],
    ).pl()
    con.close()
    return TimedResult("duckdb:worst_pm25_stations", time.perf_counter() - t0, rel)


def pollutant_summary(df: pl.DataFrame) -> pl.DataFrame:
    """Per-pollutant SUM/AVG/MIN/MAX/COUNT across all stations."""
    return (
        df.group_by("parameter")
        .agg(
            pl.col("avg").mean().round(2).alias("mean"),
            pl.col("min").min().alias("min"),
            pl.col("max").max().alias("max"),
            pl.col("sum").sum().round(1).alias("sum"),
            pl.len().alias("count"),
        )
        .sort("parameter")
    )


def aqi_category_breakdown(df: pl.DataFrame) -> pl.DataFrame:
    """How many windows fell in each AQI health category."""
    return (
        df.group_by("aqi_category")
        .agg(pl.len().alias("windows"))
        .sort("windows", descending=True)
    )
