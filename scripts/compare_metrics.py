#!/usr/bin/env python3
"""
compare_metrics.py — Read the metrics CSV produced by TopologyRunner and
print comparative summary tables for the three topology variants:
  1) Basic            (scheduler=Basic)
  2) RA-Stream        (scheduler=RaStream)
  3) RA-Stream FT     (scheduler=RaStreamFT)

Usage:
  python3 scripts/compare_metrics.py [metrics_csv_path]

Default path (if omitted):
  ~/ra-stream-metrics/local/taxi/metrics.csv
"""

import csv
import os
import sys
from collections import defaultdict


def load_metrics(path: str) -> list[dict]:
    if not os.path.exists(path):
        print(f"[ERROR] Metrics file not found: {path}")
        print("  Run the topology first:")
        print("    java -jar target/ra-stream-1.0-SNAPSHOT.jar --mode basic")
        print("    java -jar target/ra-stream-1.0-SNAPSHOT.jar --mode rastream")
        print("    java -jar target/ra-stream-1.0-SNAPSHOT.jar --mode rastream-ft")
        sys.exit(1)
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def fmt_f(v: str, digits: int = 2) -> str:
    try:
        return f"{float(v):.{digits}f}"
    except (ValueError, TypeError):
        return v or "—"


def summarise(rows: list[dict]) -> dict:
    """Return per-scheduler aggregated stats."""
    by_sched: dict[str, list] = defaultdict(list)
    for r in rows:
        by_sched[r["scheduler"]].append(r)

    summary = {}
    for sched, entries in by_sched.items():
        avg_lat  = [float(e["avg_latency_ms"])  for e in entries if e["avg_latency_ms"]]
        max_lat  = [float(e["max_latency_ms"])  for e in entries if e["max_latency_ms"]]
        tput     = [float(e["throughput_tps"])  for e in entries if e["throughput_tps"]]
        cnt      = [int(e["tuple_count"])        for e in entries if e["tuple_count"]]
        summary[sched] = {
            "runs":          len(entries),
            "avg_lat_ms":    sum(avg_lat)  / len(avg_lat)  if avg_lat  else 0,
            "peak_lat_ms":   max(max_lat)                  if max_lat  else 0,
            "avg_tput_tps":  sum(tput)     / len(tput)     if tput     else 0,
            "total_tuples":  sum(cnt),
        }
    return summary


def print_comparison(summary: dict) -> None:
    schedulers = ["Basic", "RaStream", "RaStreamFT"]
    present    = [s for s in schedulers if s in summary]
    if not present:
        print("[WARN] No recognised scheduler names found in CSV.")
        present = list(summary.keys())

    col_w = 16
    sep   = "+" + (("-" * col_w + "+") * (len(present) + 1))
    header_row = "|" + f"{'Metric':<{col_w}}|" + "".join(
        f"{s:^{col_w}}|" for s in present
    )

    print("\n╔══════════════════════════════════════════════════════════════╗")
    print("║         RA-Stream Topology Variant Comparison               ║")
    print("╚══════════════════════════════════════════════════════════════╝")
    print(sep)
    print(header_row)
    print(sep)

    metrics = [
        ("Runs recorded",  "runs",           lambda v: str(int(v))),
        ("Avg latency ms", "avg_lat_ms",      lambda v: f"{v:.2f}"),
        ("Peak latency ms","peak_lat_ms",     lambda v: f"{v:.2f}"),
        ("Avg throughput", "avg_tput_tps",    lambda v: f"{v:.0f} t/s"),
        ("Total tuples",   "total_tuples",    lambda v: str(int(v))),
    ]

    for label, key, fmt in metrics:
        cells = ""
        for s in present:
            val = summary[s].get(key, 0)
            cells += f"{fmt(val):^{col_w}}|"
        print(f"|{label:<{col_w}}|{cells}")

    print(sep)

    # Improvement vs Basic (if present)
    if "Basic" in summary and len(present) > 1:
        base_lat  = summary["Basic"]["avg_lat_ms"]  or 1
        base_tput = summary["Basic"]["avg_tput_tps"] or 1
        print(f"\n  Improvement over Basic (lower latency / higher throughput):")
        for s in present:
            if s == "Basic":
                continue
            lat_imp  = (base_lat  - summary[s]["avg_lat_ms"])  / base_lat  * 100
            tput_imp = (summary[s]["avg_tput_tps"] - base_tput) / base_tput * 100
            print(f"    {s:<14}: latency {lat_imp:+.1f}%   throughput {tput_imp:+.1f}%")


def print_raw(rows: list[dict]) -> None:
    if not rows:
        return
    keys = rows[0].keys()
    col  = 14
    header = " | ".join(f"{k:<{col}}" for k in keys)
    print("\n--- Raw rows ---")
    print(header)
    print("-" * len(header))
    for r in rows:
        print(" | ".join(f"{r[k]:<{col}}" for k in keys))


if __name__ == "__main__":
    default_path = os.path.expanduser(
        "~/ra-stream-metrics/local/taxi/metrics.csv"
    )
    csv_path = sys.argv[1] if len(sys.argv) > 1 else default_path

    print(f"Reading metrics from: {csv_path}")
    rows = load_metrics(csv_path)
    print(f"Loaded {len(rows)} rows.\n")

    if rows:
        summary = summarise(rows)
        print_comparison(summary)
        if "--raw" in sys.argv:
            print_raw(rows)
    else:
        print("[WARN] Metrics file is empty — run the topology first.")
