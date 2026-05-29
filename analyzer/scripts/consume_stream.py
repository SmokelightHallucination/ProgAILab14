#!/usr/bin/env python3
"""Real-time Kafka consumer with a 5-minute sliding window.

    python -m scripts.consume_stream

Reads aggregates from the Kafka topic and prints the rolling per-city/pollutant
summary every few messages.
"""

from __future__ import annotations

from aqanalyzer.config import CONFIG
from aqanalyzer.stream import SlidingWindow, consume


def main() -> int:
    window = SlidingWindow(minutes=CONFIG.window_minutes)
    print(
        f"consuming topic={CONFIG.kafka_topic} brokers={CONFIG.kafka_brokers} "
        f"sliding window={CONFIG.window_minutes}m"
    )
    seen = 0
    for agg, win in consume(CONFIG.kafka_brokers, CONFIG.kafka_topic, CONFIG.kafka_group, window):
        seen += 1
        print(
            f"[{seen:5d}] {agg['city']:<14} {agg['parameter']:<5} "
            f"avg={agg['avg']:>7.2f} aqi={agg['aqi']:>3} ({agg['aqi_category']})"
        )
        if seen % 12 == 0:
            print("\n== rolling sliding-window summary ==")
            print(win.summary())
            print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
