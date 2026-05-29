"""Environment-driven configuration for the analyzer (mirrors the Go side)."""

from __future__ import annotations

import os
from dataclasses import dataclass, field


def _list(name: str, default: list[str]) -> list[str]:
    raw = os.getenv(name)
    if not raw:
        return default
    return [p.strip() for p in raw.split(",") if p.strip()]


@dataclass
class Config:
    parquet_path: str = os.getenv("PARQUET_PATH", "data/aggregates.parquet")
    figures_dir: str = os.getenv("FIGURES_DIR", "docs/figures")

    kafka_brokers: list[str] = field(default_factory=lambda: _list("KAFKA_BROKERS", ["localhost:9092"]))
    kafka_topic: str = os.getenv("KAFKA_TOPIC", "air-quality.aggregates")
    kafka_group: str = os.getenv("KAFKA_GROUP", "aq-analyzer")

    flight_host: str = os.getenv("FLIGHT_HOST", "localhost")
    flight_port: int = int(os.getenv("FLIGHT_PORT", "8815"))

    # Sliding-window length for the streaming analysis (advanced task: Kafka).
    window_minutes: int = int(os.getenv("WINDOW_MINUTES", "5"))


CONFIG = Config()
