import os, glob, math
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from scipy import stats

sns.set_theme(style="whitegrid")
os.makedirs("results/plots", exist_ok=True)
os.makedirs("results/analysis", exist_ok=True)

files = glob.glob("results/raw-data/*.csv")
if not files:
    print("No CSV files in results/raw-data")
    raise SystemExit(0)

df = pd.concat([pd.read_csv(f) for f in files], ignore_index=True)

# Normalize config names (strip _repN suffix)
df["config_raw"] = df["config"].astype(str)
df["rep"] = df["config_raw"].str.extract(r"_rep(\d+)$", expand=False).fillna("1")
df["config"] = df["config_raw"].str.replace(r"_rep\d+$", "", regex=True)

# Required cols
required = ["time_sec","config","data_rate","avg_ms","p50_ms","p95_ms","p99_ms","tps","cpu"]
for c in required:
    if c not in df.columns:
        print(f"Missing column: {c}")
        raise SystemExit(1)

# Derive extras
df["latency_cv"] = df.groupby(["config","data_rate"])["avg_ms"].transform(lambda s: s.std(ddof=0)/(s.mean()+1e-9))
df["throughput_per_cpu"] = df["tps"] / (df["cpu"] + 1e-6)
df["throughput_degradation"] = df.groupby(["config"])["tps"].transform(lambda s: (s.max()-s)/(s.max()+1e-9))*100.0

# 1 Latency vs Time
plt.figure(figsize=(12,6))
sns.lineplot(data=df, x="time_sec", y="avg_ms", hue="config", estimator="mean", errorbar=("ci",95))
plt.title("Latency vs Time (mean ±95% CI)")
plt.tight_layout(); plt.savefig("results/plots/01_latency_vs_time_ci.png"); plt.close()

# 2 Throughput vs Data Rate
g = df.groupby(["config","data_rate"], as_index=False)["tps"].mean()
plt.figure(figsize=(12,6))
sns.lineplot(data=g, x="data_rate", y="tps", hue="config", marker="o")
plt.title("Throughput vs Data Rate")
plt.tight_layout(); plt.savefig("results/plots/02_throughput_vs_rate.png"); plt.close()

# 3 p95 latency vs Data Rate
g = df.groupby(["config","data_rate"], as_index=False)["p95_ms"].mean()
plt.figure(figsize=(12,6))
sns.lineplot(data=g, x="data_rate", y="p95_ms", hue="config", marker="o")
plt.title("P95 Latency vs Data Rate")
plt.tight_layout(); plt.savefig("results/plots/03_p95_latency_vs_rate.png"); plt.close()

# 4 p99 latency vs Data Rate
g = df.groupby(["config","data_rate"], as_index=False)["p99_ms"].mean()
plt.figure(figsize=(12,6))
sns.lineplot(data=g, x="data_rate", y="p99_ms", hue="config", marker="o")
plt.title("P99 Latency vs Data Rate")
plt.tight_layout(); plt.savefig("results/plots/04_p99_latency_vs_rate.png"); plt.close()

# 5 CPU vs Time
plt.figure(figsize=(12,6))
sns.lineplot(data=df, x="time_sec", y="cpu", hue="config", estimator="mean", errorbar=("ci",95))
plt.title("CPU Utilization vs Time (mean ±95% CI)")
plt.tight_layout(); plt.savefig("results/plots/05_cpu_vs_time_ci.png"); plt.close()

# 6 Latency distribution
plt.figure(figsize=(12,6))
sns.boxplot(data=df, x="config", y="avg_ms")
plt.xticks(rotation=20)
plt.title("Average Latency Distribution")
plt.tight_layout(); plt.savefig("results/plots/06_latency_boxplot.png"); plt.close()

# 7 Throughput distribution
plt.figure(figsize=(12,6))
sns.boxplot(data=df, x="config", y="tps")
plt.xticks(rotation=20)
plt.title("Throughput Distribution")
plt.tight_layout(); plt.savefig("results/plots/07_throughput_boxplot.png"); plt.close()

# 8 Heatmap: throughput
pivot_tps = df.groupby(["config","data_rate"], as_index=False)["tps"].mean().pivot(index="config", columns="data_rate", values="tps")
plt.figure(figsize=(10,5))
sns.heatmap(pivot_tps, annot=True, fmt=".0f", cmap="YlGnBu")
plt.title("Throughput Heatmap")
plt.tight_layout(); plt.savefig("results/plots/08_throughput_heatmap.png"); plt.close()

# 9 Heatmap: p95 latency
pivot_p95 = df.groupby(["config","data_rate"], as_index=False)["p95_ms"].mean().pivot(index="config", columns="data_rate", values="p95_ms")
plt.figure(figsize=(10,5))
sns.heatmap(pivot_p95, annot=True, fmt=".2f", cmap="YlOrRd")
plt.title("P95 Latency Heatmap")
plt.tight_layout(); plt.savefig("results/plots/09_p95_heatmap.png"); plt.close()

# 10 Efficiency (tps/cpu) vs rate
g = df.groupby(["config","data_rate"], as_index=False)["throughput_per_cpu"].mean()
plt.figure(figsize=(12,6))
sns.lineplot(data=g, x="data_rate", y="throughput_per_cpu", hue="config", marker="o")
plt.title("Efficiency (TPS per CPU%) vs Data Rate")
plt.tight_layout(); plt.savefig("results/plots/10_efficiency_tps_per_cpu.png"); plt.close()

# 11 Throughput degradation curve
g = df.groupby(["config","data_rate"], as_index=False)["throughput_degradation"].mean()
plt.figure(figsize=(12,6))
sns.lineplot(data=g, x="data_rate", y="throughput_degradation", hue="config", marker="o")
plt.title("Throughput Degradation (%) vs Data Rate")
plt.tight_layout(); plt.savefig("results/plots/11_throughput_degradation.png"); plt.close()

# 12 Pareto: latency vs throughput
g = df.groupby(["config"], as_index=False).agg(avg_latency=("avg_ms","mean"), avg_tps=("tps","mean"))
plt.figure(figsize=(8,6))
sns.scatterplot(data=g, x="avg_latency", y="avg_tps", hue="config", s=120)
for _,r in g.iterrows():
    plt.text(r["avg_latency"], r["avg_tps"], " "+r["config"])
plt.title("Pareto View: Latency vs Throughput")
plt.tight_layout(); plt.savefig("results/plots/12_pareto_latency_throughput.png"); plt.close()

# Summary table
summary = df.groupby("config").agg(
    mean_latency_ms=("avg_ms","mean"),
    median_latency_ms=("avg_ms","median"),
    std_latency_ms=("avg_ms","std"),
    mean_p95_ms=("p95_ms","mean"),
    mean_p99_ms=("p99_ms","mean"),
    mean_tps=("tps","mean"),
    std_tps=("tps","std"),
    mean_cpu=("cpu","mean"),
    mean_efficiency_tps_per_cpu=("throughput_per_cpu","mean")
).reset_index()
summary.to_csv("results/analysis/summary_comparison_table.csv", index=False)

# Improvement % vs EvenScheduler baseline
if "EvenScheduler" in set(summary["config"]):
    base = summary[summary["config"]=="EvenScheduler"].iloc[0]
    imp = summary.copy()
    imp["latency_improvement_%_vs_even"] = (base["mean_latency_ms"]-imp["mean_latency_ms"])/(base["mean_latency_ms"]+1e-9)*100
    imp["throughput_improvement_%_vs_even"] = (imp["mean_tps"]-base["mean_tps"])/(base["mean_tps"]+1e-9)*100
    imp.to_csv("results/analysis/improvement_vs_even_scheduler.csv", index=False)

# t-tests pairwise latency
configs = sorted(df["config"].unique())
rows = []
for i in range(len(configs)):
    for j in range(i+1, len(configs)):
        a = df[df["config"]==configs[i]]["avg_ms"].dropna()
        b = df[df["config"]==configs[j]]["avg_ms"].dropna()
        if len(a)>1 and len(b)>1:
            t,p = stats.ttest_ind(a,b,equal_var=False)
            rows.append([configs[i],configs[j],t,p])
pd.DataFrame(rows, columns=["config_a","config_b","t_stat","p_value"]).to_csv(
    "results/analysis/pairwise_ttest_latency.csv", index=False
)

print("Done. Generated extensive plots + analysis.")