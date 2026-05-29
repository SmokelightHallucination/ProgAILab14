#!/usr/bin/env python3
"""Go vs Python collector benchmark (advanced task: performance comparison).

Runs each collector as a subprocess under an identical synthetic workload for a
fixed duration, sampling CPU% and peak RSS with psutil and reading throughput
(measurements/second). Writes docs/benchmark.md and a bar chart.

    python -m scripts.benchmark --duration 20

The Go collector is measured via its Prometheus /metrics endpoint
(aq_measurements_total); the Python collector reports throughput on stdout.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
import urllib.request

import psutil


def _sample_process(proc: psutil.Process, stop_at: float, interval: float = 0.2):
    """Return (peak_rss_mb, avg_cpu_pct) while sampling until stop_at."""
    peak_rss = 0
    cpu_samples = []
    proc.cpu_percent(None)  # prime
    while time.perf_counter() < stop_at:
        try:
            peak_rss = max(peak_rss, proc.memory_info().rss)
            cpu_samples.append(proc.cpu_percent(None))
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            break
        time.sleep(interval)
    avg_cpu = sum(cpu_samples) / len(cpu_samples) if cpu_samples else 0.0
    return peak_rss / (1024 * 1024), avg_cpu


def _scrape_counter(url: str, name: str) -> float:
    try:
        with urllib.request.urlopen(url, timeout=2) as resp:
            for line in resp.read().decode().splitlines():
                if line.startswith(name + " "):
                    return float(line.split()[1])
    except Exception:
        return 0.0
    return 0.0


def bench_go(duration: float, repo_root: str) -> dict | None:
    """Run the Go collector binary and measure it."""
    binary = os.path.join(repo_root, "collector", "collector.exe")
    if not os.path.exists(binary):
        binary = os.path.join(repo_root, "collector", "collector")
    if not os.path.exists(binary):
        print("[bench] Go binary not found; build it with `go build -o collector ./collector`")
        return None

    env = {**os.environ, "PARQUET_ENABLED": "false", "METRICS_ADDR": "127.0.0.1:9123",
           "POLL_INTERVAL": "200ms", "WINDOW_SIZE": "5s"}
    proc = subprocess.Popen([binary], cwd=os.path.join(repo_root, "collector"), env=env,
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(1.5)  # let metrics server come up
    metrics_url = "http://127.0.0.1:9123/metrics"
    start_count = _scrape_counter(metrics_url, "aq_measurements_total")
    p = psutil.Process(proc.pid)
    rss, cpu = _sample_process(p, time.perf_counter() + duration)
    end_count = _scrape_counter(metrics_url, "aq_measurements_total")
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()

    measured = end_count - start_count
    return {"impl": "Go", "throughput_msg_s": round(measured / duration, 1),
            "peak_rss_mb": round(rss, 1), "avg_cpu_pct": round(cpu, 1)}


def bench_python(duration: float, repo_root: str) -> dict:
    """Run the Python collector subprocess and measure it."""
    cmd = [sys.executable, "-m", "scripts.run", "--duration", str(duration), "--poll", "0.2", "--window", "5"]
    proc = subprocess.Popen(cmd, cwd=os.path.join(repo_root, "collector_py"),
                            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
    p = psutil.Process(proc.pid)
    rss, cpu = _sample_process(p, time.perf_counter() + duration + 1)
    out, _ = proc.communicate(timeout=duration + 30)
    stats = json.loads(out.strip().splitlines()[-1])
    return {"impl": "Python", "throughput_msg_s": stats["throughput_msg_s"],
            "peak_rss_mb": round(rss, 1), "avg_cpu_pct": round(cpu, 1)}


def write_report(results: list[dict], repo_root: str) -> None:
    docs = os.path.join(repo_root, "docs")
    os.makedirs(os.path.join(docs, "figures"), exist_ok=True)
    md = os.path.join(docs, "benchmark.md")
    with open(md, "w", encoding="utf-8") as f:
        f.write("# Go vs Python collector benchmark\n\n")
        f.write("Identical synthetic air-quality workload, measured as subprocesses.\n\n")
        f.write("| Implementation | Throughput (msg/s) | Peak RSS (MB) | Avg CPU (%) |\n")
        f.write("|---|---:|---:|---:|\n")
        for r in results:
            f.write(f"| {r['impl']} | {r['throughput_msg_s']} | {r['peak_rss_mb']} | {r['avg_cpu_pct']} |\n")
        if len(results) == 2:
            go, py = results[0], results[1]
            if py["throughput_msg_s"]:
                f.write(f"\nGo throughput is {go['throughput_msg_s'] / py['throughput_msg_s']:.1f}× Python's; ")
            if go["peak_rss_mb"]:
                f.write(f"Python RSS is {py['peak_rss_mb'] / go['peak_rss_mb']:.1f}× Go's.\n")
        f.write("\n![benchmark](figures/benchmark.png)\n")
    print(f"[bench] wrote {md}")

    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        labels = [r["impl"] for r in results]
        fig, axes = plt.subplots(1, 3, figsize=(12, 4))
        for ax, key, title in zip(
            axes,
            ["throughput_msg_s", "peak_rss_mb", "avg_cpu_pct"],
            ["Throughput (msg/s)", "Peak RSS (MB)", "Avg CPU (%)"],
        ):
            ax.bar(labels, [r[key] for r in results], color=["#1f77b4", "#ff7f0e"])
            ax.set_title(title)
        fig.suptitle("Go vs Python collector")
        fig.tight_layout()
        out = os.path.join(docs, "figures", "benchmark.png")
        fig.savefig(out, dpi=120)
        print(f"[bench] wrote {out}")
    except Exception as exc:
        print(f"[bench] chart skipped: {exc}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--duration", type=float, default=20.0)
    args = ap.parse_args()
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

    print(f"[bench] benchmarking for {args.duration}s each ...")
    results = []
    go = bench_go(args.duration, repo_root)
    if go:
        results.append(go)
        print(f"[bench] Go     : {go}")
    py = bench_python(args.duration, repo_root)
    results.append(py)
    print(f"[bench] Python : {py}")

    # Keep Go first for the ratio math in the report.
    results.sort(key=lambda r: r["impl"] != "Go")
    write_report(results, repo_root)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
