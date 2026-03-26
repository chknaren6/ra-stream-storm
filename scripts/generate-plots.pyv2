import glob, os, pandas as pd, seaborn as sns, matplotlib.pyplot as plt
from scipy import stats

os.makedirs("results/plots", exist_ok=True)
os.makedirs("results/analysis", exist_ok=True)

files = glob.glob("results/raw-data/*.csv")
if not files:
    print("No CSV files found in results/raw-data")
    raise SystemExit(0)

df = pd.concat([pd.read_csv(f) for f in files], ignore_index=True)

required = {"time_sec","config","data_rate","avg_ms","tps","cpu"}
missing = required - set(df.columns)
if missing:
    print(f"Missing required columns: {missing}")
    print("Available columns:", list(df.columns))
    raise SystemExit(1)

# Latency vs Time
plt.figure(figsize=(12,6))
sns.lineplot(data=df, x="time_sec", y="avg_ms", hue="config", estimator="mean", errorbar=None)
plt.title("Latency vs Time (all schedulers)")
plt.tight_layout()
plt.savefig("results/plots/latency_vs_time.png")
plt.close()

# Throughput vs Data Rate
g = df.groupby(["config","data_rate"], as_index=False)["tps"].mean()
plt.figure(figsize=(12,6))
sns.lineplot(data=g, x="data_rate", y="tps", hue="config", marker="o")
plt.title("Throughput vs Data Rate (all schedulers)")
plt.tight_layout()
plt.savefig("results/plots/throughput_vs_data_rate.png")
plt.close()

# Resource Utilization vs Time
plt.figure(figsize=(12,6))
sns.lineplot(data=df, x="time_sec", y="cpu", hue="config", estimator="mean", errorbar=None)
plt.title("Resource Utilization (CPU) vs Time")
plt.tight_layout()
plt.savefig("results/plots/resource_utilization_vs_time.png")
plt.close()

# Latency Distribution boxplot
plt.figure(figsize=(12,6))
sns.boxplot(data=df, x="config", y="avg_ms")
plt.xticks(rotation=20)
plt.title("Latency Distribution (boxplot)")
plt.tight_layout()
plt.savefig("results/plots/latency_distribution_boxplot.png")
plt.close()

# Overhead analysis FT vs non-FT (if both present)
if {"RaStream", "RaStreamFT"}.issubset(set(df["config"].unique())):
    base = df[df["config"]=="RaStream"]["avg_ms"].mean()
    ft   = df[df["config"]=="RaStreamFT"]["avg_ms"].mean()
    overhead_pct = ((ft - base) / base * 100.0) if base else float("nan")
    with open("results/analysis/overhead_analysis.txt","w") as f:
        f.write(f"RaStream avg latency: {base:.6f} ms\n")
        f.write(f"RaStreamFT avg latency: {ft:.6f} ms\n")
        f.write(f"Latency overhead (%): {overhead_pct:.4f}\n")

# Summary table
summary = df.groupby("config").agg(
    mean_latency_ms=("avg_ms","mean"),
    median_latency_ms=("avg_ms","median"),
    std_latency_ms=("avg_ms","std"),
    mean_tps=("tps","mean"),
    mean_cpu=("cpu","mean")
).reset_index()
summary.to_csv("results/analysis/summary_comparison_table.csv", index=False)

# Resource usage table
resource_tbl = df.groupby("config").agg(
    mean_cpu=("cpu","mean"),
    max_cpu=("cpu","max")
).reset_index()
resource_tbl.to_csv("results/analysis/resource_usage_table.csv", index=False)

# Simple t-test first two configs
cfgs = sorted(df["config"].dropna().unique())
if len(cfgs) >= 2:
    a = df[df["config"] == cfgs[0]]["avg_ms"].dropna()
    b = df[df["config"] == cfgs[1]]["avg_ms"].dropna()
    if len(a) > 1 and len(b) > 1:
        t, p = stats.ttest_ind(a, b, equal_var=False)
        with open("results/analysis/t_test_latency.txt","w") as f:
            f.write(f"{cfgs[0]} vs {cfgs[1]} latency t-test\n")
            f.write(f"t_stat={t}\n")
            f.write(f"p_value={p}\n")

print("Generated plots and analysis in results/plots and results/analysis")
