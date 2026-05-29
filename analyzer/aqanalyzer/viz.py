"""Visualisations of air-quality aggregates (Plotly, saved as HTML + PNG)."""

from __future__ import annotations

import os

import plotly.express as px
import plotly.graph_objects as go
import polars as pl

# AQI category → colour (US EPA palette) for consistent charts.
AQI_COLORS = {
    "Good": "#00e400",
    "Moderate": "#ffff00",
    "Unhealthy for Sensitive Groups": "#ff7e00",
    "Unhealthy": "#ff0000",
    "Very Unhealthy": "#8f3f97",
    "Hazardous": "#7e0023",
    "Unknown": "#cccccc",
}


def _save(fig: go.Figure, out_dir: str, name: str) -> list[str]:
    """Write the interactive HTML version. Static PNGs are produced separately
    by viz_static (Matplotlib), which is more reliable than Plotly/kaleido."""
    os.makedirs(out_dir, exist_ok=True)
    html = os.path.join(out_dir, f"{name}.html")
    fig.write_html(html, include_plotlyjs="cdn")
    return [html]


def aqi_timeseries(df: pl.DataFrame, out_dir: str) -> list[str]:
    """PM2.5 AQI over time, one line per city."""
    pm = df.filter(pl.col("parameter") == "pm25").sort("window_end")
    fig = px.line(
        pm.to_pandas(),
        x="window_end",
        y="aqi",
        color="city",
        markers=True,
        title="PM2.5 Air Quality Index over time, by city",
        labels={"window_end": "Window end (UTC)", "aqi": "AQI"},
    )
    return _save(fig, out_dir, "aqi_timeseries")


def pollutant_distribution(df: pl.DataFrame, out_dir: str) -> list[str]:
    """Distribution of window-average concentrations per pollutant."""
    fig = px.box(
        df.to_pandas(),
        x="parameter",
        y="avg",
        color="parameter",
        title="Distribution of pollutant concentrations (window averages)",
        labels={"parameter": "Pollutant", "avg": "Concentration"},
    )
    fig.update_yaxes(type="log")
    return _save(fig, out_dir, "pollutant_distribution")


def city_pollutant_heatmap(df: pl.DataFrame, out_dir: str) -> list[str]:
    """Heatmap of mean concentration per city × pollutant."""
    pivot = (
        df.group_by("city", "parameter")
        .agg(pl.col("avg").mean().alias("mean"))
        .pivot(values="mean", index="city", on="parameter")
        .fill_null(0)
    )
    cities = pivot["city"].to_list()
    params = [c for c in pivot.columns if c != "city"]
    z = pivot.select(params).to_numpy()
    fig = go.Figure(
        go.Heatmap(z=z, x=params, y=cities, colorscale="YlOrRd", colorbar_title="mean")
    )
    fig.update_layout(
        title="Mean pollutant concentration by city",
        xaxis_title="Pollutant",
        yaxis_title="City",
    )
    return _save(fig, out_dir, "city_pollutant_heatmap")


def aqi_category_pie(df: pl.DataFrame, out_dir: str) -> list[str]:
    """Share of windows in each AQI health category."""
    counts = df.group_by("aqi_category").agg(pl.len().alias("windows")).to_pandas()
    fig = px.pie(
        counts,
        names="aqi_category",
        values="windows",
        title="Share of windows by AQI health category",
        color="aqi_category",
        color_discrete_map=AQI_COLORS,
    )
    return _save(fig, out_dir, "aqi_category_pie")


def render_all(df: pl.DataFrame, out_dir: str) -> list[str]:
    """Render every figure; returns the list of written files."""
    paths: list[str] = []
    paths += aqi_timeseries(df, out_dir)
    paths += pollutant_distribution(df, out_dir)
    paths += city_pollutant_heatmap(df, out_dir)
    paths += aqi_category_pie(df, out_dir)
    return paths
