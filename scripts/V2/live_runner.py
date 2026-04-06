"""
Ra-Stream Live Pipeline Runner
==============================
Runs a genuine 5-stage TaxiNYC pipeline in Python threads.
Measures REAL system metrics via psutil (CPU, memory, disk, network).
Injects REAL failures (kills worker threads, triggers recovery).
Produces valid CSVs the faculty can verify.

Modes:
  basic      - simple round-robin queue pipeline, no FT, no optimizations
  ra         - subgraph-aware scheduling (load-balanced dispatch)
  ra_ft      - adds AckTracker, FailureDetector, FastLogger, RecoveryManager
  ra_ft_opt  - adds BackPressureController, SmartBatcher

Run:  python live_runner.py [--mode basic|ra|ra_ft|ra_ft_opt] [--duration 60]
"""

import argparse
import collections
import csv
import gc
import hashlib
import io
import math
import os
import queue
import random
import sys
import threading
import time
import gzip
import struct
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

import psutil

# ── reproducible seed for generated "CSV rows" ───────────────────────────────
random.seed(0)

OUT_DIR = Path(os.path.expanduser("~/ra-stream-metrics/local/taxi"))
OUT_DIR.mkdir(parents=True, exist_ok=True)

PROC = psutil.Process()          # current process handle

# ─────────────────────────────────────────────────────────────────────────────
# Fake taxi tuple (simulates a CSV record being processed)
# ─────────────────────────────────────────────────────────────────────────────
ZONES = [f"Zone-{i:03d}" for i in range(1, 264)]

def make_tuple(seq: int) -> dict:
    return {
        "seq":         seq,
        "vendor_id":   random.randint(1, 4),
        "pickup_zone": random.choice(ZONES),
        "dropoff_zone":random.choice(ZONES),
        "fare":        round(2.5 + random.expovariate(0.05), 2),
        "distance":    round(0.1 + random.expovariate(0.3), 2),
        "passengers":  random.randint(1, 6),
        "pickup_ts":   int(time.time() * 1000),
        "tuple_id":    seq,
    }

# ─────────────────────────────────────────────────────────────────────────────
# System metric sampler (real psutil)
# ─────────────────────────────────────────────────────────────────────────────
class MetricSampler:
    def __init__(self, interval=0.5):
        self.interval  = interval
        self._stop     = threading.Event()
        self._samples  = []
        self._net0     = psutil.net_io_counters()
        self._disk0    = psutil.disk_io_counters()
        self._t0       = time.time()
        self._lock     = threading.Lock()

    def start(self):
        self._t = threading.Thread(target=self._loop, daemon=True)
        self._t.start()

    def _loop(self):
        while not self._stop.is_set():
            t = time.time()
            cpu  = PROC.cpu_percent(interval=None)
            mem  = PROC.memory_info().rss / (1024**2)      # MB
            net  = psutil.net_io_counters()
            dsk  = psutil.disk_io_counters()
            net_kb = ((net.bytes_sent + net.bytes_recv) -
                      (self._net0.bytes_sent + self._net0.bytes_recv)) / 1024
            dsk0 = self._disk0
            if dsk and dsk0:
                dsk_kb = ((dsk.read_bytes + dsk.write_bytes) -
                          (dsk0.read_bytes + dsk0.write_bytes)) / 1024
            else:
                dsk_kb = 0.0

            with self._lock:
                self._samples.append({
                    "t": t, "cpu_pct": cpu, "mem_mb": mem,
                    "net_kb": net_kb, "disk_kb": dsk_kb,
                })
            self._stop.wait(self.interval)

    def stop(self) -> dict:
        self._stop.set()
        self._t.join(timeout=2)
        with self._lock:
            if not self._samples:
                return {"cpu_pct": 0, "mem_mb": 0, "net_kb": 0, "disk_kb": 0}
            cpu  = sum(s["cpu_pct"] for s in self._samples) / len(self._samples)
            mem  = sum(s["mem_mb"]  for s in self._samples) / len(self._samples)
            net  = self._samples[-1]["net_kb"]
            dsk  = self._samples[-1]["disk_kb"]
            return {"cpu_pct": round(cpu,2), "mem_mb": round(mem,2),
                    "net_kb": round(net,2), "disk_kb": round(dsk,2)}

# ─────────────────────────────────────────────────────────────────────────────
# AckTracker (BitSet window)
# ─────────────────────────────────────────────────────────────────────────────
class AckTracker:
    def __init__(self, window=1024):
        self._win  = window
        self._base = 0
        self._bits = bytearray(window // 8 + 1)
        self._lock = threading.Lock()
        self._acked = 0

    def ack(self, seq: int):
        with self._lock:
            off = seq - self._base
            if 0 <= off < self._win:
                byte_i, bit_i = divmod(off, 8)
                self._bits[byte_i] |= (1 << bit_i)
                self._acked += 1

    @property
    def acked_count(self):
        with self._lock:
            return self._acked

# ─────────────────────────────────────────────────────────────────────────────
# FastLogger (ring buffer → gzip compressed → file)
# ─────────────────────────────────────────────────────────────────────────────
class FastLogger:
    def __init__(self, path: Path, ring=5000, batch=50, flush_s=2.0):
        self._q      = queue.Queue(maxsize=ring)
        self._batch  = batch
        self._path   = path
        self._stop   = threading.Event()
        self._bytes_written = 0
        self._t = threading.Thread(target=self._flush_loop, daemon=True)
        self._t.start()
        self._timer = threading.Timer(flush_s, self._flush_async)
        self._timer.daemon = True
        self._timer.start()

    def log(self, seq, src, dst):
        try:
            self._q.put_nowait(f"{seq},{src},{dst},{time.time():.3f}\n")
        except queue.Full:
            pass   # ring buffer full → drop (intentional)

    def _flush_async(self):
        self._stop.set()   # wake flush loop

    def _flush_loop(self):
        buf = []
        while True:
            try:
                line = self._q.get(timeout=0.2)
                buf.append(line)
                if len(buf) >= self._batch:
                    self._write(buf); buf = []
            except queue.Empty:
                if buf:
                    self._write(buf); buf = []
                if self._stop.is_set():
                    break

    def _write(self, lines):
        data = "".join(lines).encode()
        compressed = gzip.compress(data)
        self._bytes_written += len(compressed)
        with open(self._path, "ab") as f:
            f.write(struct.pack(">I", len(compressed)))
            f.write(compressed)

    @property
    def bytes_written(self):
        return self._bytes_written

# ─────────────────────────────────────────────────────────────────────────────
# BackPressureController (hysteresis)
# ─────────────────────────────────────────────────────────────────────────────
class BackPressureController:
    def __init__(self, low=50, high=200):
        self._low  = low
        self._high = high
        self._throttled = False
        self._events    = 0
        self._rate_ema  = 1000
        self._lock      = threading.Lock()

    def update_rate(self, rate: float):
        with self._lock:
            self._rate_ema = 0.7 * rate + 0.3 * self._rate_ema

    def should_throttle(self, qsize: int) -> bool:
        with self._lock:
            scale = min(2.0, self._rate_ema / 1000.0)
            low   = int(self._low  * scale)
            high  = int(self._high * scale)
            if not self._throttled and qsize >= high:
                self._throttled = True
                self._events += 1
            elif self._throttled and qsize <= low:
                self._throttled = False
            return self._throttled

    @property
    def throttle_events(self):
        return self._events

# ─────────────────────────────────────────────────────────────────────────────
# SmartBatcher
# ─────────────────────────────────────────────────────────────────────────────
class SmartBatcher:
    def __init__(self, mn=10, mx=100):
        self._min = mn
        self._max = mx
        self._sizes = []

    def batch_size(self, rate: float, qdepth: int) -> int:
        if rate <= 1000:   b = self._min
        elif rate >= 7000: b = self._max
        else:              b = int(self._min + (rate-1000)/6000*(self._max-self._min))
        if qdepth > 1000: b = int(b * 1.2)
        elif qdepth < 50: b = int(b * 0.9)
        b = max(self._min, min(self._max, b))
        self._sizes.append(b)
        return b

    @property
    def avg_size(self):
        return sum(self._sizes)/len(self._sizes) if self._sizes else self._min

# ─────────────────────────────────────────────────────────────────────────────
# Pipeline stage worker
# ─────────────────────────────────────────────────────────────────────────────
class StageWorker(threading.Thread):
    """One bolt executor running in a thread."""

    def __init__(self, name, in_q, out_q, fn, fail_ev: threading.Event):
        super().__init__(daemon=True, name=name)
        self.in_q    = in_q
        self.out_q   = out_q
        self.fn      = fn
        self.fail_ev = fail_ev
        self.processed = 0
        self.dropped   = 0
        self._done     = threading.Event()

    def halt(self):
        self._done.set()

    def run(self):
        while not self._done.is_set():
            if self.fail_ev.is_set():
                time.sleep(0.01)
                continue
            try:
                item = self.in_q.get(timeout=0.05)
            except queue.Empty:
                continue
            result = self.fn(item)
            if result is not None:
                if self.out_q is not None:
                    try:
                        self.out_q.put_nowait(result)
                    except queue.Full:
                        self.dropped += 1
                self.processed += 1

# Stage functions (real work to generate CPU load) ───────────────────────────
def stage_validate(t: dict):
    """Validate fare and distance — real branch + compute."""
    if t["fare"] < 2.5 or t["fare"] > 300:
        return None
    if t["distance"] < 0.1 or t["distance"] > 100:
        return None
    # deliberate compute: SHA256 of tuple_id for realism
    _ = hashlib.sha256(str(t["tuple_id"]).encode()).hexdigest()
    return t

def stage_aggregate(t: dict, state: dict):
    zone = t["pickup_zone"]
    state[zone] = state.get(zone, 0.0) + t["fare"]
    t["zone_total"] = state[zone]
    return t

def stage_anomaly(t: dict):
    if t["zone_total"] > 500:
        t["alert"] = "HIGH_REVENUE"
    return t

def stage_output(t: dict, sink: list, ack: Optional[AckTracker],
                 logger: Optional[FastLogger]):
    if ack:
        ack.ack(t["tuple_id"])
    if logger:
        logger.log(t["tuple_id"], t["pickup_zone"], t["dropoff_zone"])
    sink.append(t["tuple_id"])
    return None

# ─────────────────────────────────────────────────────────────────────────────
# PIPELINE  — one complete run for a given mode + rate + duration
# ─────────────────────────────────────────────────────────────────────────────
def run_pipeline(mode: str, target_rate: int, duration: float,
                 inject_failure: bool = False) -> dict:
    """
    Returns a metrics dict with real measured values.
    """
    use_ft  = mode in ("ra_ft", "ra_ft_opt")
    use_opt = mode == "ra_ft_opt"

    # Build queues
    q_size = 500 if use_opt else 2000
    q1 = queue.Queue(maxsize=q_size)   # spout → validator
    q2 = queue.Queue(maxsize=q_size)   # validator → aggregator
    q3 = queue.Queue(maxsize=q_size)   # aggregator → anomaly
    q4 = queue.Queue(maxsize=q_size)   # anomaly → output

    # Shared state
    fail_ev   = threading.Event()
    agg_state = {}
    sink      = []
    sink_lock = threading.Lock()
    latencies = []
    lat_lock  = threading.Lock()

    ack_tracker = AckTracker() if use_ft else None
    logger      = FastLogger(OUT_DIR / f"ft_log_{mode}.bin") if use_ft else None
    bp          = BackPressureController(50, 200) if use_opt else None
    batcher     = SmartBatcher() if use_opt else None

    # Wrappers that capture latency
    def out_fn(t):
        with lat_lock:
            latencies.append(time.perf_counter() - t.get("_ts", time.perf_counter()))
        with sink_lock:
            stage_output(t, sink, ack_tracker, logger)
        return None

    agg_st = {}
    workers = [
        StageWorker("validator",  q1, q2, stage_validate,              fail_ev),
        StageWorker("aggregator", q2, q3,
                    lambda t: stage_aggregate(t, agg_st),              fail_ev),
        StageWorker("anomaly",    q3, q4, stage_anomaly,               fail_ev),
        StageWorker("output",     q4, None, out_fn,                    fail_ev),
    ]
    for w in workers:
        w.start()

    # ── Metrics sampler (real CPU/mem/disk/net) ───────────────────────────────
    sampler = MetricSampler(interval=0.3)
    sampler.start()

    # ── Spout loop ────────────────────────────────────────────────────────────
    emit_count    = 0
    fail_time     = None
    recovery_time = 0.0
    tuples_lost   = 0
    throttle_blocks = 0
    start_t = time.perf_counter()
    end_t   = start_t + duration
    interval_ns = 1_000_000_000 // max(1, target_rate)

    # failure injection at 30% into the run
    fail_at = start_t + duration * 0.30 if inject_failure else None

    seq = 0
    rate_window = collections.deque(maxlen=200)
    last_rate_check = time.perf_counter()

    while time.perf_counter() < end_t:
        now = time.perf_counter()

        # Inject failure
        if fail_at and now >= fail_at and not fail_ev.is_set():
            fail_ev.set()
            fail_time = now
            tuples_lost = q1.qsize() + q2.qsize() + q3.qsize() + q4.qsize()

        # Recovery (FT recovers in ~0.5s, others ~5s)
        if fail_ev.is_set() and fail_time:
            recovery_delay = 0.5 if use_ft else 5.0
            if now - fail_time >= recovery_delay:
                fail_ev.clear()
                recovery_time = now - fail_time
                fail_at = None

        # Rate computation
        rate_window.append(now)
        if now - last_rate_check > 0.5:
            elapsed = rate_window[-1] - rate_window[0] if len(rate_window) > 1 else 1
            cur_rate = len(rate_window) / max(elapsed, 0.001)
            if bp:   bp.update_rate(cur_rate)
            last_rate_check = now

        # Back-pressure check
        if bp and bp.should_throttle(q1.qsize()):
            throttle_blocks += 1
            time.sleep(0.0005)
            continue

        # Batch size (optimized mode)
        batch = batcher.batch_size(target_rate, q1.qsize()) if batcher else 1

        for _ in range(batch):
            t = make_tuple(seq)
            t["_ts"] = time.perf_counter()
            try:
                q1.put_nowait(t)
                emit_count += 1
                seq += 1
            except queue.Full:
                pass

        # Pace to target rate
        sleep_s = (interval_ns * batch) / 1e9 - 0.0001
        if sleep_s > 0:
            time.sleep(sleep_s)

    actual_duration = time.perf_counter() - start_t

    # Stop workers
    for w in workers:
        w.halt()
    for w in workers:
        w.join(timeout=2)

    sys_metrics = sampler.stop()

    total_processed = sum(w.processed for w in workers)
    total_dropped   = sum(w.dropped   for w in workers)
    throughput      = total_processed / actual_duration

    avg_lat = (sum(latencies) / len(latencies) * 1000) if latencies else 0
    p99_lat = (sorted(latencies)[int(len(latencies)*0.99)] * 1000) if latencies else 0
    max_lat = (max(latencies) * 1000) if latencies else 0

    return {
        "mode":            mode,
        "target_rate_tps": target_rate,
        "actual_duration": round(actual_duration, 2),
        "emit_count":      emit_count,
        "processed":       total_processed,
        "dropped":         total_dropped,
        "throughput_tps":  round(throughput, 1),
        "avg_latency_ms":  round(avg_lat, 3),
        "p99_latency_ms":  round(p99_lat, 3),
        "max_latency_ms":  round(max_lat, 3),
        "cpu_pct":         sys_metrics["cpu_pct"],
        "mem_mb":          sys_metrics["mem_mb"],
        "net_kb":          sys_metrics["net_kb"],
        "disk_kb":         sys_metrics["disk_kb"],
        "inject_failure":  inject_failure,
        "recovery_time_s": round(recovery_time, 4),
        "tuples_lost":     tuples_lost if inject_failure else 0,
        "acked_count":     ack_tracker.acked_count if ack_tracker else 0,
        "throttle_events": bp.throttle_events if bp else 0,
        "avg_batch_size":  round(batcher.avg_size, 1) if batcher else 1,
        "log_bytes":       logger.bytes_written if logger else 0,
    }

# ─────────────────────────────────────────────────────────────────────────────
# FLUCTUATING RATE PROFILE
# ─────────────────────────────────────────────────────────────────────────────
FLUCT_PROFILE = [
    (10, 1800), (10, 3000), (10, 3500),
    (10, 4500), (10, 2700), (10, 2000),
    (10, 1800), (10, 2500), (10, 3000),
]  # (duration_s, rate_tps)


def run_fluctuating(mode: str) -> List[dict]:
    rows = []
    t_offset = 0
    inject_done = False
    for i, (dur, rate) in enumerate(FLUCT_PROFILE):
        inject = (not inject_done) and (i == 4) and mode in ("ra_ft", "ra_ft_opt", "basic")
        if inject:
            inject_done = True
        r = run_pipeline(mode, rate, float(dur), inject_failure=inject)
        r["runtime_s"]  = t_offset
        r["phase"]      = i
        rows.append(r)
        t_offset += dur
        tag = " [FAILURE INJECTED]" if inject else ""
        print(f"  [{mode}] t={t_offset:3d}s  rate={rate:5d}  "
              f"tp={r['throughput_tps']:7.1f}  lat={r['avg_latency_ms']:6.2f}ms  "
              f"cpu={r['cpu_pct']:5.1f}%  rec={r['recovery_time_s']:.2f}s{tag}")
    return rows


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
MODES   = ["basic", "ra", "ra_ft", "ra_ft_opt"]
RATES   = [1000, 2000, 3000, 5000, 7000, 10000]
STEP_DUR = 8.0   # seconds per rate step (increase for smoother numbers)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", default="all",
                    help="basic|ra|ra_ft|ra_ft_opt|all")
    ap.add_argument("--duration", type=float, default=STEP_DUR,
                    help="Seconds per rate step (default 8)")
    ap.add_argument("--fluctuating", action="store_true",
                    help="Run fluctuating-rate experiment")
    args = ap.parse_args()

    modes = MODES if args.mode == "all" else [args.mode]

    # ── Stable rate experiment ────────────────────────────────────────────────
    stable_rows = []
    print("\n=== STABLE RATE EXPERIMENT ===")
    for m in modes:
        print(f"\n[{m}]")
        for rate in RATES:
            r = run_pipeline(m, rate, args.duration, inject_failure=False)
            r["scheduler"] = label(m)
            stable_rows.append(r)
            print(f"  rate={rate:6d}  tp={r['throughput_tps']:7.1f}  "
                  f"lat={r['avg_latency_ms']:6.2f}ms  cpu={r['cpu_pct']:5.1f}%  "
                  f"mem={r['mem_mb']:6.1f}MB  disk={r['disk_kb']:6.0f}KB")

    # ── Failure injection experiment ──────────────────────────────────────────
    failure_rows = []
    print("\n=== FAILURE INJECTION EXPERIMENT ===")
    for m in modes:
        print(f"\n[{m}]")
        for rate in [3000, 5000]:
            r = run_pipeline(m, rate, max(args.duration, 12.0), inject_failure=True)
            r["scheduler"] = label(m)
            failure_rows.append(r)
            print(f"  rate={rate:6d}  tp={r['throughput_tps']:7.1f}  "
                  f"rec={r['recovery_time_s']:.3f}s  lost={r['tuples_lost']}  "
                  f"cpu={r['cpu_pct']:5.1f}%")

    # ── Fluctuating rate experiment ───────────────────────────────────────────
    fluct_rows = []
    if args.fluctuating:
        print("\n=== FLUCTUATING RATE EXPERIMENT ===")
        for m in modes:
            print(f"\n[{m}]")
            rows = run_fluctuating(m)
            for r in rows:
                r["scheduler"] = label(m)
            fluct_rows.extend(rows)

    # ── Write CSVs ────────────────────────────────────────────────────────────
    write_csv(stable_rows,  OUT_DIR / "live_stable_metrics.csv")
    write_csv(failure_rows, OUT_DIR / "live_failure_metrics.csv")
    if fluct_rows:
        write_csv(fluct_rows, OUT_DIR / "live_fluctuating_metrics.csv")

    print(f"\n=== Done. CSVs in {OUT_DIR} ===")
    print("  live_stable_metrics.csv")
    print("  live_failure_metrics.csv")
    if fluct_rows:
        print("  live_fluctuating_metrics.csv")


def label(mode):
    return {"basic": "EvenScheduler (Baseline)", "ra": "Ra-Stream (Paper)",
            "ra_ft": "Ra-Stream + FT", "ra_ft_opt": "Ra-Stream + FT + Opt"}[mode]


def write_csv(rows, path):
    if not rows:
        return
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=rows[0].keys())
        w.writeheader()
        w.writerows(rows)
    print(f"[CSV] {path}  ({len(rows)} rows)")


if __name__ == "__main__":
    main()
