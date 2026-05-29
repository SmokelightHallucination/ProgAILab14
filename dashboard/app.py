"""Real-time air-quality dashboard (advanced task #6).

A Streamlit app that consumes the Go collector's Kafka topic in a background
thread, keeps a sliding window of aggregates, and refreshes live charts: an AQI
map, per-city rolling AQI, and the current health-category breakdown.

Run:  streamlit run dashboard/app.py
Env:  KAFKA_BROKERS, KAFKA_TOPIC, WINDOW_MINUTES
"""

from __future__ import annotations

import os
import sys
import threading
import time

# Make the analyzer package importable when running from the repo root.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "analyzer"))

import plotly.express as px
import polars as pl
import streamlit as st

from aqanalyzer.stream import SlidingWindow

KAFKA_BROKERS = [b.strip() for b in os.getenv("KAFKA_BROKERS", "localhost:9092").split(",")]
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "air-quality.aggregates")
WINDOW_MINUTES = int(os.getenv("WINDOW_MINUTES", "5"))
REFRESH_SECONDS = int(os.getenv("REFRESH_SECONDS", "3"))

AQI_COLORS = {
    "Good": "#00e400",
    "Moderate": "#ffff00",
    "Unhealthy for Sensitive Groups": "#ff7e00",
    "Unhealthy": "#ff0000",
    "Very Unhealthy": "#8f3f97",
    "Hazardous": "#7e0023",
    "Unknown": "#cccccc",
}


@st.cache_resource
def _start_consumer() -> SlidingWindow:
    """Start one background Kafka consumer for the whole app session."""
    window = SlidingWindow(minutes=WINDOW_MINUTES)

    def _run() -> None:
        from kafka import KafkaConsumer
        import json

        while True:
            try:
                consumer = KafkaConsumer(
                    KAFKA_TOPIC,
                    bootstrap_servers=KAFKA_BROKERS,
                    auto_offset_reset="latest",
                    value_deserializer=lambda b: json.loads(b.decode("utf-8")),
                )
                for message in consumer:
                    window.add(message.value)
            except Exception as exc:  # broker not up yet → retry
                print(f"[dashboard] consumer retry: {exc}")
                time.sleep(3)

    threading.Thread(target=_run, daemon=True).start()
    return window


def main() -> None:
    st.set_page_config(page_title="Air Quality Monitor", page_icon="🌫️", layout="wide")
    st.title("🌫️ Real-time Air Quality Monitoring — variant 20")
    st.caption(
        f"Sliding window: {WINDOW_MINUTES} min · topic: {KAFKA_TOPIC} · "
        f"brokers: {', '.join(KAFKA_BROKERS)}"
    )

    window = _start_consumer()
    df = window.frame()

    if df.is_empty():
        st.info("Waiting for data from the collector… (is the Kafka pipeline running?)")
        time.sleep(REFRESH_SECONDS)
        st.rerun()
        return

    # Most recent aggregate per (station, pollutant).
    latest = (
        df.sort("window_end")
        .group_by("location_id", "location", "city", "country", "parameter")
        .last()
    )
    pm = latest.filter(pl.col("parameter") == "pm25")

    col1, col2, col3 = st.columns(3)
    col1.metric("Active stations", df["location_id"].n_unique())
    col2.metric("Aggregates in window", df.height)
    if not pm.is_empty():
        worst = pm.sort("aqi", descending=True).row(0, named=True)
        col3.metric("Worst PM2.5 AQI", int(worst["aqi"]), f"{worst['city']}")

    # --- Map of current PM2.5 AQI ---
    if not pm.is_empty():
        st.subheader("Current PM2.5 AQI by station")
        fig_map = px.scatter_geo(
            pm.to_pandas(),
            lat="latitude",
            lon="longitude",
            color="aqi_category",
            size="aqi",
            hover_name="location",
            hover_data={"aqi": True, "avg": True, "latitude": False, "longitude": False},
            color_discrete_map=AQI_COLORS,
            projection="natural earth",
        )
        st.plotly_chart(fig_map, use_container_width=True)

    left, right = st.columns(2)

    with left:
        st.subheader("Rolling PM2.5 AQI over the window")
        pm_series = df.filter(pl.col("parameter") == "pm25").sort("window_end")
        if not pm_series.is_empty():
            fig = px.line(pm_series.to_pandas(), x="window_end", y="aqi", color="city", markers=True)
            st.plotly_chart(fig, use_container_width=True)

    with right:
        st.subheader("Health-category breakdown")
        cats = df.group_by("aqi_category").agg(pl.len().alias("windows")).to_pandas()
        fig = px.pie(cats, names="aqi_category", values="windows",
                     color="aqi_category", color_discrete_map=AQI_COLORS)
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("Rolling per-city / pollutant summary")
    st.dataframe(window.summary().to_pandas(), use_container_width=True)

    time.sleep(REFRESH_SECONDS)
    st.rerun()


if __name__ == "__main__":
    main()
