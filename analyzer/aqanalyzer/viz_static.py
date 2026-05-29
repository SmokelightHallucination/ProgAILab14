"""Static PNG renderings of the same charts as viz.py, via Matplotlib.

Plotly's PNG export (kaleido) is flaky on some platforms, so the PNGs embedded
in the README/report are produced here with Matplotlib, which is dependency-light
and reliable. viz.py still produces the interactive HTML versions.
"""

from __future__ import annotations

import os

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import polars as pl

AQI_COLORS = {
    "Good": "#00e400",
    "Moderate": "#cccc00",
    "Unhealthy for Sensitive Groups": "#ff7e00",
    "Unhealthy": "#ff0000",
    "Very Unhealthy": "#8f3f97",
    "Hazardous": "#7e0023",
    "Unknown": "#cccccc",
}


def render_all(df: pl.DataFrame, out_dir: str) -> list[str]:
    os.makedirs(out_dir, exist_ok=True)
    paths = [
        _timeseries(df, out_dir),
        _distribution(df, out_dir),
        _heatmap(df, out_dir),
        _category_pie(df, out_dir),
    ]
    return paths


def _timeseries(df: pl.DataFrame, out_dir: str) -> str:
    pm = df.filter(pl.col("parameter") == "pm25").sort("window_end")
    fig, ax = plt.subplots(figsize=(11, 6))
    for city in sorted(pm["city"].unique().to_list()):
        sub = pm.filter(pl.col("city") == city).sort("window_end")
        ax.plot(sub["window_end"].to_list(), sub["aqi"].to_list(), marker="o", ms=3, label=city)
    ax.set_title("PM2.5 Air Quality Index over time, by city")
    ax.set_xlabel("Window end (UTC)")
    ax.set_ylabel("AQI")
    ax.legend(fontsize=7, ncol=2)
    fig.autofmt_xdate()
    return _save(fig, out_dir, "aqi_timeseries")


def _distribution(df: pl.DataFrame, out_dir: str) -> str:
    params = sorted(df["parameter"].unique().to_list())
    data = [df.filter(pl.col("parameter") == p)["avg"].to_list() for p in params]
    fig, ax = plt.subplots(figsize=(11, 6))
    ax.boxplot(data, labels=params, showfliers=True)
    ax.set_yscale("log")
    ax.set_title("Distribution of pollutant concentrations (window averages)")
    ax.set_xlabel("Pollutant")
    ax.set_ylabel("Concentration (log scale)")
    return _save(fig, out_dir, "pollutant_distribution")


def _heatmap(df: pl.DataFrame, out_dir: str) -> str:
    pivot = (
        df.group_by("city", "parameter")
        .agg(pl.col("avg").mean().alias("mean"))
        .pivot(values="mean", index="city", on="parameter")
        .fill_null(0)
        .sort("city")
    )
    cities = pivot["city"].to_list()
    params = [c for c in pivot.columns if c != "city"]
    z = pivot.select(params).to_numpy()
    fig, ax = plt.subplots(figsize=(10, 7))
    im = ax.imshow(z, cmap="YlOrRd", aspect="auto")
    ax.set_xticks(range(len(params)), params)
    ax.set_yticks(range(len(cities)), cities, fontsize=8)
    ax.set_title("Mean pollutant concentration by city")
    fig.colorbar(im, ax=ax, label="mean concentration")
    return _save(fig, out_dir, "city_pollutant_heatmap")


def _category_pie(df: pl.DataFrame, out_dir: str) -> str:
    counts = df.group_by("aqi_category").agg(pl.len().alias("windows")).sort("windows", descending=True)
    labels = counts["aqi_category"].to_list()
    sizes = counts["windows"].to_list()
    colors = [AQI_COLORS.get(c, "#999999") for c in labels]
    fig, ax = plt.subplots(figsize=(8, 8))
    ax.pie(sizes, labels=labels, colors=colors, autopct="%1.0f%%", startangle=90)
    ax.set_title("Share of windows by AQI health category")
    return _save(fig, out_dir, "aqi_category_pie")


def _save(fig, out_dir: str, name: str) -> str:
    path = os.path.join(out_dir, f"{name}.png")
    fig.tight_layout()
    fig.savefig(path, dpi=120)
    plt.close(fig)
    return path
