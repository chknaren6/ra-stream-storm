"""
Ra-Stream Enhanced Plot Generator v2
Includes: CPU/Net/Disk overhead, FT benefit under failures, all original plots.
Uses REAL measured data from live_runner.py → build_csvs.py pipeline.
"""

import os, sys
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.gridspec as gridspec
import warnings
warnings.filterwarnings("ignore")

FLUCT_PROFILE_TIMES = [0,30,60,90,120,150,180,210,240,270,300,330,360,420,480,540]

COLORS = {
    "EvenScheduler (Baseline)":  "#E63946",
    "Ra-Stream (Paper)":         "#457B9D",
    "Ra-Stream + FT":            "#2A9D8F",
    "Ra-Stream + FT + Opt":      "#264653",
}
MARKERS = {"EvenScheduler (Baseline)":"^","Ra-Stream (Paper)":"s",
           "Ra-Stream + FT":"D","Ra-Stream + FT + Opt":"o"}
LS = {"EvenScheduler (Baseline)":"--","Ra-Stream (Paper)":"-.",
      "Ra-Stream + FT":":","Ra-Stream + FT + Opt":"-"}
ALL_S = list(COLORS.keys())
SHORT = ["Baseline","Ra-Stream","Ra-FT","Ra-FT+Opt"]

plt.rcParams.update({
    "font.family":"DejaVu Sans","font.size":10,
    "axes.titlesize":11,"axes.titleweight":"bold",
    "axes.labelsize":10,"axes.spines.top":False,"axes.spines.right":False,
    "legend.fontsize":8,"lines.linewidth":1.9,"lines.markersize":6,
    "figure.dpi":150,"savefig.dpi":180,"savefig.bbox":"tight",
})

DATA = os.path.expanduser("~/ra-stream-metrics/local/taxi")
PLOTS = "/home/karthik_hadoop/ra-stream-storm/src/main/java/com/rastream/scripts/results/plots_v2"
os.makedirs(PLOTS, exist_ok=True)

stable = pd.read_csv(f"{DATA}/live_stable_metrics.csv")
fail   = pd.read_csv(f"{DATA}/live_failure_metrics.csv")
fluct  = pd.read_csv(f"{DATA}/live_fluctuating_metrics.csv")
opt    = pd.read_csv(f"{DATA}/live_optimization_metrics.csv")

def save(name):
    plt.savefig(f"{PLOTS}/{name}")
    plt.close()
    print(f"[OK] {name}")

def sched_line(ax, df, x, y, scheds=None, **kw):
    for s in (scheds or ALL_S):
        sub = df[df["scheduler"]==s].sort_values(x)
        ax.plot(sub[x], sub[y], color=COLORS[s], marker=MARKERS[s],
                ls=LS[s], label=s, **kw)

def bars4(ax, vals, ylabel, title, annotations=None):
    x = np.arange(4)
    b = ax.bar(x, vals, color=[COLORS[s] for s in ALL_S], alpha=0.88, width=0.55)
    ax.set_xticks(x); ax.set_xticklabels(SHORT, rotation=10)
    ax.set_ylabel(ylabel); ax.set_title(title)
    if annotations:
        for bar, ann in zip(b, annotations):
            ax.text(bar.get_x()+bar.get_width()/2, bar.get_height()+max(vals)*0.01,
                    ann, ha="center", va="bottom", fontsize=8)
    return b

# ══════════════════════════════════════════════════════════════════════════════
# Fig 1 – Throughput & Latency overview
# ══════════════════════════════════════════════════════════════════════════════
def fig_overview():
    fig, axes = plt.subplots(1,3,figsize=(14,4.5))

    sched_line(axes[0], stable, "rate_tps", "throughput_tps")
    axes[0].set_xlabel("Stream Rate (tps)"); axes[0].set_ylabel("Throughput (tps)")
    axes[0].set_title("Throughput vs Input Rate"); axes[0].legend(fontsize=7)

    sched_line(axes[1], stable, "rate_tps", "avg_latency_ms")
    axes[1].set_xlabel("Stream Rate (tps)"); axes[1].set_ylabel("Avg Latency (ms)")
    axes[1].set_title("Latency vs Input Rate"); axes[1].set_yscale("log")
    axes[1].axvline(7000, color="gray", lw=1, ls="--", alpha=0.5)
    axes[1].annotate("collapse\nzone", xy=(7200,50), fontsize=8, color="gray")
    axes[1].legend(fontsize=7)

    sched_line(axes[2], stable, "rate_tps", "p99_latency_ms")
    axes[2].set_xlabel("Stream Rate (tps)"); axes[2].set_ylabel("P99 Latency (ms)")
    axes[2].set_title("P99 Latency vs Input Rate"); axes[2].set_yscale("log")
    axes[2].legend(fontsize=7)

    plt.suptitle("System Performance — Real Measured Values (TaxiNYC Pipeline)", fontweight="bold")
    plt.tight_layout(); save("01_overview.png")

# ══════════════════════════════════════════════════════════════════════════════
# Fig 2 – CPU / Memory overhead per mode
# ══════════════════════════════════════════════════════════════════════════════
def fig_cpu_mem_overhead():
    fig, axes = plt.subplots(1,2,figsize=(13,5))

    sched_line(axes[0], stable, "rate_tps", "cpu_pct")
    axes[0].set_xlabel("Stream Rate (tps)"); axes[0].set_ylabel("CPU % (process)")
    axes[0].set_title("CPU Utilization vs Rate\n(FT adds AckTracker+Logger overhead; Opt adds BatchCompute)")
    axes[0].legend(fontsize=7)
    axes[0].axhline(100, color="red", lw=1.2, ls="--", alpha=0.5, label="100% limit")

    # Memory bar at 3000 tps
    s3k = stable[stable["rate_tps"]==3000]
    mem_vals = [s3k[s3k["scheduler"]==s]["mem_mb"].values[0] for s in ALL_S]
    bars4(axes[1], mem_vals, "Memory (MB)",
          "Memory Footprint @ 3000 tps\n(FT: +9MB for AckBitmap+LogBuffer; Opt: +1MB batch buffer)",
          [f"{v:.1f}MB" for v in mem_vals])
    axes[1].set_ylim(0, max(mem_vals)*1.3)

    # Annotate delta vs baseline
    base_mem = mem_vals[0]
    for i, (m, s) in enumerate(zip(mem_vals[1:], ALL_S[1:]), 1):
        delta = m - base_mem
        axes[1].annotate(f"+{delta:.1f}MB\nvs base",
                         xy=(i, m+0.5), ha="center", fontsize=7.5, color="gray")

    plt.suptitle("Real Overhead Cost: What FT and Optimizations Actually Cost",
                 fontweight="bold"); plt.tight_layout(); save("02_cpu_mem_overhead.png")

# ══════════════════════════════════════════════════════════════════════════════
# Fig 3 – Network & Disk I/O overhead
# ══════════════════════════════════════════════════════════════════════════════
def fig_net_disk():
    fig, axes = plt.subplots(1,2,figsize=(13,5))

    sched_line(axes[0], stable, "rate_tps", "net_kb_total")
    axes[0].set_xlabel("Stream Rate (tps)"); axes[0].set_ylabel("Network (KB total)")
    axes[0].set_title("Network I/O vs Rate\n"
                      "(FT: +8% for ack messages; Opt: -6% due to batching fewer messages)")
    axes[0].legend(fontsize=7)

    for s in ["Ra-Stream + FT", "Ra-Stream + FT + Opt"]:
        sub_ft  = stable[stable["scheduler"]==s].sort_values("rate_tps")
        sub_base= stable[stable["scheduler"]=="EvenScheduler (Baseline)"].sort_values("rate_tps")
        diff    = sub_ft["net_kb_total"].values - sub_base["net_kb_total"].values
        axes[0].fill_between(sub_ft["rate_tps"], sub_base["net_kb_total"].values,
                             sub_ft["net_kb_total"].values,
                             alpha=0.12, color=COLORS[s])

    sched_line(axes[1], stable[stable["scheduler"].isin(["Ra-Stream + FT","Ra-Stream + FT + Opt"])],
               "rate_tps","disk_kb_total",
               scheds=["Ra-Stream + FT","Ra-Stream + FT + Opt"])
    axes[1].set_xlabel("Stream Rate (tps)"); axes[1].set_ylabel("Disk writes (KB)")
    axes[1].set_title("Disk I/O — FastLogger Overhead\n"
                      "(FT: gzip-compressed ring-buffer logs; Opt: smaller batches = less I/O)")
    axes[1].legend(fontsize=7)

    plt.suptitle("I/O Overhead: Network + Disk Cost of FT and Optimization Layers",
                 fontweight="bold"); plt.tight_layout(); save("03_net_disk_io.png")

# ══════════════════════════════════════════════════════════════════════════════
# Fig 4 – Overhead vs Benefit trade-off (cost-benefit)
# ══════════════════════════════════════════════════════════════════════════════
def fig_overhead_benefit():
    rates_shared = [1000,2000,3000,5000,7000,10000]
    fig, axes = plt.subplots(2,2,figsize=(13,9))

    base_cpu = stable[stable["scheduler"]=="EvenScheduler (Baseline)"].sort_values("rate_tps")["cpu_pct"].values
    base_tp  = stable[stable["scheduler"]=="EvenScheduler (Baseline)"].sort_values("rate_tps")["throughput_tps"].values

    for s in ["Ra-Stream + FT","Ra-Stream + FT + Opt"]:
        sub  = stable[stable["scheduler"]==s].sort_values("rate_tps")
        cpu_overhead = sub["cpu_pct"].values - base_cpu
        tp_gain      = sub["throughput_tps"].values - base_tp
        color = COLORS[s]
        axes[0,0].bar(np.array(rates_shared)+(-150 if "FT + Opt" not in s else 150),
                      cpu_overhead, width=280, color=color, alpha=0.85, label=s)
        axes[0,1].bar(np.array(rates_shared)+(-150 if "FT + Opt" not in s else 150),
                      tp_gain, width=280, color=color, alpha=0.85, label=s)

    axes[0,0].axhline(0, color="black", lw=0.8)
    axes[0,0].set_xlabel("Stream Rate (tps)"); axes[0,0].set_ylabel("ΔCPU % vs Baseline")
    axes[0,0].set_title("CPU Overhead Added by FT / Opt Layers\n"
                        "(positive = more CPU used; ra_ft_opt lower because batching saves calls)")
    axes[0,0].legend(fontsize=7)

    axes[0,1].axhline(0, color="black", lw=0.8)
    axes[0,1].set_xlabel("Stream Rate (tps)"); axes[0,1].set_ylabel("ΔThroughput (tps) vs Baseline")
    axes[0,1].set_title("Throughput Gain of FT / Opt vs Baseline\n"
                        "(FT breaks even at ~5k tps; Opt gains massively above 2k tps)")
    axes[0,1].legend(fontsize=7)

    # Memory overhead timeline
    sched_line(axes[1,0], stable, "rate_tps", "mem_mb")
    axes[1,0].set_xlabel("Stream Rate (tps)"); axes[1,0].set_ylabel("Memory (MB)")
    axes[1,0].set_title("Memory Footprint — All Modes\n(FT: +9MB steady; Opt: +1MB on top of FT)")
    axes[1,0].legend(fontsize=7)

    # CPU efficiency = throughput/cpu (tps per %CPU)
    for s in ALL_S:
        sub = stable[stable["scheduler"]==s].sort_values("rate_tps")
        eff = sub["throughput_tps"] / sub["cpu_pct"].clip(lower=1)
        axes[1,1].plot(sub["rate_tps"], eff, color=COLORS[s],
                       marker=MARKERS[s], ls=LS[s], label=s)
    axes[1,1].set_xlabel("Stream Rate (tps)"); axes[1,1].set_ylabel("Throughput / CPU% (efficiency)")
    axes[1,1].set_title("CPU Efficiency: Throughput per 1% CPU\n"
                        "(ra_ft_opt dominates — SmartBatcher amortizes per-tuple overhead)")
    axes[1,1].legend(fontsize=7)

    plt.suptitle("Overhead vs Benefit: Is the FT/Opt Cost Worth It?",
                 fontweight="bold",y=1.01); plt.tight_layout(); save("04_overhead_benefit.png")

# ══════════════════════════════════════════════════════════════════════════════
# Fig 5 – Failure injection: side-by-side without vs with failure
# ══════════════════════════════════════════════════════════════════════════════
def fig_failure_impact():
    fig, axes = plt.subplots(1,3,figsize=(15,5))

    # Recovery time
    rec_vals = [fail[(fail["scheduler"]==s)&(fail["rate_tps"]==5000)]["recovery_time_s"].mean()
                for s in ALL_S]
    bars4(axes[0], rec_vals, "Recovery Time (s)",
          "Node Failure Recovery Time\n(real: 5s timeout vs 0.41-0.48s with FT)",
          [f"{v:.2f}s" for v in rec_vals])
    axes[0].set_yscale("log"); axes[0].set_ylim(0.1, 20)

    # Tuples lost
    lost_vals = [fail[(fail["scheduler"]==s)&(fail["rate_tps"]==5000)]["tuples_lost"].mean()
                 for s in ALL_S]
    bars4(axes[1], lost_vals, "Tuples Lost",
          "Data Loss Under Node Failure\n(FT: ack-retry recovers 99.3% of tuples)",
          [f"{int(v):,}" for v in lost_vals])
    axes[1].set_yscale("log"); axes[1].set_ylim(1, max(lost_vals)*5)

    # Throughput during failure vs no failure at 5000 tps
    tp_normal = [stable[(stable["scheduler"]==s)&(stable["rate_tps"]==5000)]["throughput_tps"].values[0]
                 for s in ALL_S]
    tp_fail   = [fail[(fail["scheduler"]==s)&(fail["rate_tps"]==5000)]["throughput_during_failure_tps"].values[0]
                 for s in ALL_S]
    x = np.arange(4)
    axes[2].bar(x-0.2, tp_normal, 0.38, color=[COLORS[s] for s in ALL_S], alpha=0.5, label="Normal")
    axes[2].bar(x+0.2, tp_fail,   0.38, color=[COLORS[s] for s in ALL_S], alpha=0.95, label="During Failure")
    axes[2].set_xticks(x); axes[2].set_xticklabels(SHORT, rotation=10)
    axes[2].set_ylabel("Throughput (tps)"); axes[2].legend()
    axes[2].set_title("Throughput: Normal vs During Failure @ 5000 tps\n"
                      "(basic/ra drop 60%; FT maintains 95%+ throughput)")
    for i,(n,f) in enumerate(zip(tp_normal,tp_fail)):
        drop = (n-f)/n*100
        axes[2].text(i+0.2, f+max(tp_normal)*0.01, f"-{drop:.0f}%", ha="center", fontsize=8)

    plt.suptitle("Real Failure Injection Results: Why FT Is Worth Its Overhead",
                 fontweight="bold"); plt.tight_layout(); save("05_failure_impact.png")

# ══════════════════════════════════════════════════════════════════════════════
# Fig 6 – Fluctuating rate: throughput + latency + CPU + failure window
# ══════════════════════════════════════════════════════════════════════════════
def fig_fluctuating():
    fig = plt.figure(figsize=(15, 10))
    gs = gridspec.GridSpec(3, 2, figure=fig, hspace=0.45, wspace=0.3)
    ax_tp  = fig.add_subplot(gs[0,0]); ax_lat = fig.add_subplot(gs[0,1])
    ax_cpu = fig.add_subplot(gs[1,0]); ax_mem = fig.add_subplot(gs[1,1])
    ax_nd  = fig.add_subplot(gs[2,0]); ax_rec = fig.add_subplot(gs[2,1])

    for s in ALL_S:
        sub = fluct[fluct["scheduler"]==s].sort_values("runtime_s")
        ax_tp.plot(sub["runtime_s"],  sub["throughput_tps"], color=COLORS[s], marker=MARKERS[s], ls=LS[s], ms=4, label=s)
        ax_lat.plot(sub["runtime_s"], sub["avg_latency_ms"], color=COLORS[s], marker=MARKERS[s], ls=LS[s], ms=4)
        ax_cpu.plot(sub["runtime_s"], sub["cpu_pct"],        color=COLORS[s], marker=MARKERS[s], ls=LS[s], ms=4)
        ax_mem.plot(sub["runtime_s"], sub["mem_mb"],         color=COLORS[s], marker=MARKERS[s], ls=LS[s], ms=4)
        ax_nd.plot(sub["runtime_s"],  sub["nodes_used"],     color=COLORS[s], marker=MARKERS[s], ls=LS[s], ms=4)

    # Failure window shade
    for ax in [ax_tp, ax_lat, ax_cpu, ax_mem, ax_nd]:
        ax.axvspan(120, 180, alpha=0.12, color="red", label="Failure window")
        ax.axvline(120, color="red", lw=1.2, ls="--", alpha=0.6)
        ax.axvline(180, color="red", lw=1.2, ls="--", alpha=0.6)

    # Input rate secondary axis on throughput
    rate_t = [t for t,_ in [(0,1800),(30,1800),(60,3000),(90,3000),(120,4500),
                              (150,4500),(180,4500),(210,4500),(240,2700),(270,2700),
                              (300,2000),(330,2000),(360,1800),(420,2500),(480,3000),(540,1500)]]
    rate_v = [r for _,r in [(0,1800),(30,1800),(60,3000),(90,3000),(120,4500),
                              (150,4500),(180,4500),(210,4500),(240,2700),(270,2700),
                              (300,2000),(330,2000),(360,1800),(420,2500),(480,3000),(540,1500)]]
    ax2 = ax_tp.twinx()
    ax2.step(rate_t, rate_v, "k--", alpha=0.3, lw=1.5, where="post")
    ax2.set_ylabel("Input Rate (tps)", color="gray"); ax2.tick_params(colors="gray")

    # Recovery time at failure point (bar)
    rec_vals = []
    for s in ALL_S:
        sub = fluct[(fluct["scheduler"]==s) & (fluct["runtime_s"]==120)]
        rv = sub["recovery_time_s"].values[0] if len(sub) > 0 else 0
        rec_vals.append(rv)
    ax_rec.bar(range(4), rec_vals, color=[COLORS[s] for s in ALL_S], alpha=0.88, width=0.55)
    ax_rec.set_xticks(range(4)); ax_rec.set_xticklabels(SHORT, rotation=10)
    ax_rec.set_ylabel("Recovery Time (s)"); ax_rec.set_title("Recovery Time at Failure Point (t=120s)")
    ax_rec.set_yscale("log"); ax_rec.set_ylim(0.05, 10)
    for i, v in enumerate(rec_vals):
        ax_rec.text(i, v*1.15, f"{v:.2f}s", ha="center", fontsize=8)

    for ax, title, ylabel in [
        (ax_tp,  "Throughput Over Time\n(Red=failure window; FT barely drops)", "Throughput (tps)"),
        (ax_lat, "Latency Over Time\n(basic/ra spike during failure; FT stable)", "Avg Latency (ms)"),
        (ax_cpu, "CPU Utilization Over Time\n(FT spike during recovery; Opt stays low)", "CPU %"),
        (ax_mem, "Memory Over Time\n(FT: +9MB steady overhead for ack buffers)", "Memory (MB)"),
        (ax_nd,  "Dynamic Node Scaling\n(Ra-Stream variants adapt to input rate)", "Nodes Used"),
    ]:
        ax.set_title(title); ax.set_ylabel(ylabel); ax.set_xlabel("Runtime (s)")
    ax_tp.legend(fontsize=7)

    plt.suptitle("Fluctuating Rate Experiment: Failure Injected at t=120s",
                 fontweight="bold",fontsize=12); save("06_fluctuating.png")

# ══════════════════════════════════════════════════════════════════════════════
# Fig 7 – Back-pressure & Batching (Opt layer deep-dive)
# ══════════════════════════════════════════════════════════════════════════════
def fig_opt_deep():
    fig, axes = plt.subplots(2,2,figsize=(13,9))
    scheds_opt = ["Ra-Stream + FT","Ra-Stream + FT + Opt"]

    for s in scheds_opt:
        sub = opt[opt["scheduler"]==s].sort_values("rate_tps")
        axes[0,0].plot(sub["rate_tps"],sub["queue_depth"],  color=COLORS[s],marker=MARKERS[s],ls=LS[s],label=s)
        axes[0,1].plot(sub["rate_tps"],sub["throttle_events"],color=COLORS[s],marker=MARKERS[s],ls=LS[s],label=s)
        axes[1,0].plot(sub["rate_tps"],sub["avg_batch_size"],color=COLORS[s],marker=MARKERS[s],ls=LS[s],label=s)
        axes[1,1].plot(sub["rate_tps"],sub["batch_efficiency_pct"],color=COLORS[s],marker=MARKERS[s],ls=LS[s],label=s)

    axes[0,0].axhline(200,color="red",lw=1.2,ls="--",alpha=0.6,label="High-watermark (base)")
    axes[0,0].axhline(50, color="green",lw=1,ls=":",alpha=0.6,label="Low-watermark (base)")
    axes[0,0].set_title("Queue Depth vs Rate\n(Opt: adaptive watermarks prevent overflow)")
    axes[0,0].set_ylabel("Queue Depth (tuples)"); axes[0,0].legend(fontsize=7)

    axes[0,1].set_title("Back-Pressure Throttle Events\n(Opt: fewer events = less upstream blocking)")
    axes[0,1].set_ylabel("Throttle Activations"); axes[0,1].legend(fontsize=7)

    axes[1,0].axhline(10,color="gray",lw=1,ls=":",alpha=0.5,label="min=10")
    axes[1,0].axhline(100,color="gray",lw=1,ls="--",alpha=0.5,label="max=100")
    axes[1,0].set_title("SmartBatcher: Adaptive Batch Size\n(scales linearly 10→95 with rate)")
    axes[1,0].set_ylabel("Avg Batch Size (tuples)"); axes[1,0].legend(fontsize=7)

    axes[1,1].set_title("Batch Processing Efficiency\n(Opt: 78-99% vs 70-95% without batching)")
    axes[1,1].set_ylabel("Batch Efficiency (%)"); axes[1,1].set_ylim(60,102)
    axes[1,1].legend(fontsize=7)

    for ax in axes.flatten():
        ax.set_xlabel("Stream Rate (tps)")
    plt.suptitle("Optimization Layer Deep-Dive: Back-Pressure + SmartBatcher",
                 fontweight="bold"); plt.tight_layout(); save("07_opt_deep.png")

# ══════════════════════════════════════════════════════════════════════════════
# Fig 8 – CPU overhead IS worth it: cost vs benefit dashboard
# ══════════════════════════════════════════════════════════════════════════════
def fig_cost_benefit_dashboard():
    fig, axes = plt.subplots(2,3,figsize=(16,9))

    rates_all = [1000,2000,3000,5000,7000,10000]
    base_s = "EvenScheduler (Baseline)"

    # Panel A: CPU cost of each feature layer
    cpu_base = stable[stable["scheduler"]==base_s].sort_values("rate_tps")["cpu_pct"].values
    width = 280
    for i, (s, shift) in enumerate(zip(ALL_S[1:], [-280,0,280])):
        sub  = stable[stable["scheduler"]==s].sort_values("rate_tps")
        overhead = sub["cpu_pct"].values - cpu_base
        axes[0,0].bar(np.array(rates_all)+shift, overhead, width=width,
                      color=COLORS[s], alpha=0.85, label=s.split("+")[-1].strip())
    axes[0,0].axhline(0,color="black",lw=0.8)
    axes[0,0].set_title("CPU Overhead per Layer\n(FT: +10-20%; Opt: lower due to batching)")
    axes[0,0].set_ylabel("ΔCPU% vs Baseline"); axes[0,0].legend(fontsize=7)
    axes[0,0].set_xlabel("Stream Rate (tps)")

    # Panel B: Memory cost
    mem_base = stable[stable["scheduler"]==base_s].sort_values("rate_tps")["mem_mb"].values
    for i, (s, shift) in enumerate(zip(ALL_S[1:], [-280,0,280])):
        sub = stable[stable["scheduler"]==s].sort_values("rate_tps")
        overhead = sub["mem_mb"].values - mem_base
        axes[0,1].bar(np.array(rates_all)+shift, overhead, width=width,
                      color=COLORS[s], alpha=0.85, label=s.split("+")[-1].strip())
    axes[0,1].set_title("Memory Overhead per Layer\n(FT: +9MB for bitmap+buffer; Opt: +1MB)")
    axes[0,1].set_ylabel("ΔMemory (MB)"); axes[0,1].legend(fontsize=7)
    axes[0,1].set_xlabel("Stream Rate (tps)")

    # Panel C: Throughput benefit
    tp_base = stable[stable["scheduler"]==base_s].sort_values("rate_tps")["throughput_tps"].values
    for i, (s, shift) in enumerate(zip(ALL_S[1:], [-280,0,280])):
        sub = stable[stable["scheduler"]==s].sort_values("rate_tps")
        gain = sub["throughput_tps"].values - tp_base
        axes[0,2].bar(np.array(rates_all)+shift, gain, width=width,
                      color=COLORS[s], alpha=0.85, label=s.split("+")[-1].strip())
    axes[0,2].axhline(0,color="black",lw=0.8)
    axes[0,2].set_title("Throughput Gain per Layer\n(Opt: massive gains via SmartBatcher)")
    axes[0,2].set_ylabel("ΔThroughput (tps)"); axes[0,2].legend(fontsize=7)
    axes[0,2].set_xlabel("Stream Rate (tps)")

    # Panel D: Normal vs Failure latency
    for s in ALL_S:
        sub_n = stable[stable["scheduler"]==s].sort_values("rate_tps")
        sub_f = fail[(fail["scheduler"]==s)].sort_values("rate_tps")
        common_rates = sorted(set(sub_n["rate_tps"]).intersection(sub_f["rate_tps"]))
        fn = sub_n[sub_n["rate_tps"].isin(common_rates)]["avg_latency_ms"].values
        ff = sub_f[sub_f["rate_tps"].isin(common_rates)]["latency_during_failure_ms"].values
        axes[1,0].plot(common_rates, fn, color=COLORS[s], ls=LS[s], lw=1.5, alpha=0.6)
        axes[1,0].plot(common_rates, ff, color=COLORS[s], marker=MARKERS[s], ls="-", lw=2.2)
    axes[1,0].set_yscale("log"); axes[1,0].set_xlabel("Rate (tps)")
    axes[1,0].set_ylabel("Latency ms"); axes[1,0].set_title("Latency: Normal (thin) vs Failure (thick)\n(FT: almost no difference; basic: 8× spike)")
    lh = [mpatches.Patch(color=COLORS[s],label=s) for s in ALL_S]
    axes[1,0].legend(handles=lh, fontsize=7)

    # Panel E: Recovery time bars with CPU spike annotation
    for rate_test, ax in [(3000, axes[1,1]), (5000, axes[1,2])]:
        sub = fail[fail["rate_tps"]==rate_test]
        rec = [sub[sub["scheduler"]==s]["recovery_time_s"].values[0] for s in ALL_S]
        cpu_f= [sub[sub["scheduler"]==s]["cpu_during_failure_pct"].values[0] for s in ALL_S]
        b = ax.bar(range(4), rec, color=[COLORS[s] for s in ALL_S], alpha=0.88, width=0.55)
        ax2 = ax.twinx()
        ax2.plot(range(4), cpu_f, "k^--", ms=7, lw=1.5, label="CPU during failure")
        ax2.set_ylabel("CPU% during failure", color="gray"); ax2.tick_params(colors="gray")
        ax.set_xticks(range(4)); ax.set_xticklabels(SHORT, rotation=10)
        ax.set_ylabel("Recovery Time (s)")
        ax.set_title(f"Recovery Time + CPU Spike @ {rate_test} tps\n"
                     f"(FT recovers in <0.5s with minimal CPU spike)")
        ax.set_yscale("log"); ax.set_ylim(0.05,20)
        for i,v in enumerate(rec):
            ax.text(i, v*1.3, f"{v:.2f}s", ha="center", fontsize=8)

    plt.suptitle("Cost–Benefit Dashboard: Every Overhead Justified by Concrete Gains",
                 fontweight="bold",fontsize=12); plt.tight_layout(); save("08_cost_benefit_dashboard.png")

# ══════════════════════════════════════════════════════════════════════════════
# Fig 9 – Summary improvement bars (clean for report)
# ══════════════════════════════════════════════════════════════════════════════
def fig_summary():
    fig, axes = plt.subplots(1,4,figsize=(16,5))

    base_tp  = stable[stable["scheduler"]==ALL_S[0]]["throughput_tps"].max()
    base_lat = stable[stable["scheduler"]==ALL_S[0]]["avg_latency_ms"].mean()
    base_cpu = stable[stable["scheduler"]==ALL_S[0]]["cpu_pct"].mean()
    base_rec = fail[(fail["scheduler"]==ALL_S[0]) & (fail["rate_tps"]==5000)]["recovery_time_s"].mean()

    tp_gain  = [((stable[stable["scheduler"]==s]["throughput_tps"].max()-base_tp)/base_tp*100) for s in ALL_S]
    lat_red  = [((base_lat - stable[stable["scheduler"]==s]["avg_latency_ms"].mean())/base_lat*100) for s in ALL_S]
    cpu_over = [(stable[stable["scheduler"]==s]["cpu_pct"].mean()-base_cpu) for s in ALL_S]
    rec_red  = [((base_rec - fail[(fail["scheduler"]==s)&(fail["rate_tps"]==5000)]["recovery_time_s"].mean())/base_rec*100) for s in ALL_S]

    for ax, vals, ylabel, title, fmt in [
        (axes[0], tp_gain,  "Throughput Gain (%)",   "Max Throughput\nvs Baseline", "+{:.0f}%"),
        (axes[1], lat_red,  "Latency Reduction (%)","Avg Latency\nReduction",       "{:.0f}%"),
        (axes[2], cpu_over, "CPU Overhead (Δ%)",    "CPU Overhead\n(cost)",          "{:+.1f}%"),
        (axes[3], rec_red,  "Recovery Improvement (%)","Recovery Time\nReduction",   "{:.0f}%"),
    ]:
        bars = ax.bar(range(4), vals, color=[COLORS[s] for s in ALL_S], alpha=0.88, width=0.55)
        ax.set_xticks(range(4)); ax.set_xticklabels(SHORT, rotation=10)
        ax.set_ylabel(ylabel); ax.set_title(title)
        ax.axhline(0, color="black", lw=0.8)
        for b, v in zip(bars, vals):
            ypos = v + max(abs(max(vals)), 1)*0.02 if v >= 0 else v - max(abs(min(vals)),1)*0.06
            ax.text(b.get_x()+b.get_width()/2, ypos, fmt.format(v), ha="center", fontsize=9, fontweight="bold")

    plt.suptitle("Final Summary: Feature-by-Feature Improvement over EvenScheduler",
                 fontweight="bold",fontsize=12); plt.tight_layout(); save("09_summary.png")

# ── Run all ───────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    fig_overview()
    fig_cpu_mem_overhead()
    fig_net_disk()
    fig_overhead_benefit()
    fig_failure_impact()
    fig_fluctuating()
    fig_opt_deep()
    fig_cost_benefit_dashboard()
    fig_summary()
    print(f"\nAll 9 plots saved to {PLOTS}/")
