"""Real-time Kafka consumer with a sliding window (advanced task: Kafka).

The Go collector publishes tumbling-window aggregates to a Kafka topic; this
module keeps a 5-minute (configurable) sliding window over them and recomputes
per-city / per-pollutant statistics on every batch. Both the CLI consumer and
the Streamlit dashboard build on ``SlidingWindow``.
"""

from __future__ import annotations

import json
import time
from collections import deque
from datetime import datetime, timedelta, timezone

import polars as pl

try:
    from kafka import KafkaConsumer  # type: ignore

    _KAFKA_IMPORT_ERROR = None
except ImportError as exc:  # pragma: no cover
    KafkaConsumer = None  # allows importing the module without kafka installed
    _KAFKA_IMPORT_ERROR = exc


class SlidingWindow:
    """Keeps aggregates whose window_end is within the last ``minutes``."""

    def __init__(self, minutes: int = 5):
        self.span = timedelta(minutes=minutes)
        self._items: deque[dict] = deque()

    def add(self, agg: dict) -> None:
        self._items.append(agg)
        self._evict()

    def _evict(self) -> None:
        cutoff = datetime.now(timezone.utc) - self.span
        while self._items:
            ts = _parse_ts(self._items[0].get("window_end"))
            if ts is not None and ts < cutoff:
                self._items.popleft()
            else:
                break

    def frame(self) -> pl.DataFrame:
        self._evict()
        if not self._items:
            return pl.DataFrame()
        return pl.DataFrame(list(self._items))

    def summary(self) -> pl.DataFrame:
        """Per-city/pollutant rolling mean over the current window."""
        df = self.frame()
        if df.is_empty():
            return df
        return (
            df.group_by("city", "parameter")
            .agg(
                pl.col("avg").mean().round(2).alias("rolling_mean"),
                pl.col("aqi").mean().round(1).alias("rolling_aqi"),
                pl.col("max").max().alias("peak"),
                pl.len().alias("windows"),
            )
            .sort(["city", "parameter"])
        )


def _parse_ts(value) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None


def consume(brokers: list[str], topic: str, group: str, window: SlidingWindow):
    """Yield ``(aggregate, window)`` for each message read from Kafka."""
    if KafkaConsumer is None:
        raise RuntimeError(
            f"kafka client unavailable (install kafka-python-ng): {_KAFKA_IMPORT_ERROR}"
        )
    # Retry connecting: under docker-compose/k8s the broker may not be ready the
    # instant we start, so we wait it out instead of crash-looping.
    from kafka.errors import NoBrokersAvailable

    consumer = None
    for attempt in range(60):
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=brokers,
                group_id=group,
                auto_offset_reset="earliest",  # don't miss aggregates produced before we joined
                value_deserializer=lambda b: json.loads(b.decode("utf-8")),
                # No consumer_timeout_ms → block indefinitely waiting for messages
                # (timeout=0 would make the iterator stop the instant the queue is empty).
            )
            break
        except NoBrokersAvailable:
            print(f"[stream] broker not ready (attempt {attempt + 1}), retrying in 3s…")
            time.sleep(3)
    if consumer is None:
        raise RuntimeError(f"could not reach Kafka brokers {brokers} after retries")
    for message in consumer:
        agg = message.value
        window.add(agg)
        yield agg, window
