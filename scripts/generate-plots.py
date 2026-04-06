import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import warnings
warnings.filterwarnings("ignore")

sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (14, 9)
plt.rcParams['font.size'] = 14
plt.rcParams['axes.titlesize'] = 18
plt.rcParams['axes.labelsize'] = 16

OUTPUT_DIR = Path("../results/plots")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

CSV_PATH = "/home/karthik_hadoop/ra-stream-metrics/local/taxi/metrics.csv"

df = pd.read_csv(CSV_PATH)

# Clean column names
df = df.rename(columns={
    'stream_rate_tps': 'Input_Rate',
    'avg_latency_ms': 'Avg_Latency_ms',
    'max_latency_ms': 'Max_Latency_ms',
    'throughput_tps': 'Throughput',
    'avg_cpu_pct': 'CPU_Util',
    'avg_mem_pct': 'Mem_Util'
})

# Nice labels
scheduler_map = {
    "Round Robin": "Basic (Round Robin)",
    "RaStream": "RA-Stream",
    "RaStreamFT": "RA-Stream + FT"
}
df['Scheduler'] = df['scheduler'].map(scheduler_map)

print(f"Loaded {len(df)} records from metrics.csv")

# ================== PLOTS ==================

# 1. Average Latency vs Input Rate
plt.figure()
sns.lineplot(data=df, x='Input_Rate', y='Avg_Latency_ms', hue='Scheduler', 
             style='Scheduler', markers=True, linewidth=3, markersize=8)
plt.title('Average Latency vs Input Rate')
plt.xlabel('Input Rate (tuples/sec)')
plt.ylabel('Average Latency (ms)')
plt.legend(title='Scheduler')
plt.savefig(OUTPUT_DIR / '01_avg_latency_vs_rate.png', dpi=400, bbox_inches='tight')
plt.close()

# 2. Max Latency vs Input Rate
plt.figure()
sns.lineplot(data=df, x='Input_Rate', y='Max_Latency_ms', hue='Scheduler', 
             style='Scheduler', markers=True, linewidth=3)
plt.title('Maximum Latency vs Input Rate')
plt.xlabel('Input Rate (tuples/sec)')
plt.ylabel('Max Latency (ms)')
plt.legend(title='Scheduler')
plt.savefig(OUTPUT_DIR / '02_max_latency_vs_rate.png', dpi=400, bbox_inches='tight')
plt.close()

# 3. Throughput vs Input Rate
plt.figure()
sns.lineplot(data=df, x='Input_Rate', y='Throughput', hue='Scheduler', 
             style='Scheduler', markers=True, linewidth=3)
plt.title('Achieved Throughput vs Input Rate')
plt.xlabel('Input Rate (tuples/sec)')
plt.ylabel('Throughput (tuples/sec)')
plt.legend(title='Scheduler')
plt.savefig(OUTPUT_DIR / '03_throughput_vs_rate.png', dpi=400, bbox_inches='tight')
plt.close()

# 4. Latency Boxplot at High Load (very insightful for paper)
high_rate = df[df['Input_Rate'] >= 8000]
plt.figure(figsize=(10, 6))
sns.boxplot(data=high_rate, x='Scheduler', y='Avg_Latency_ms', palette="Blues")
plt.title('Latency Distribution at High Load (≥ 8000 tps)')
plt.ylabel('Average Latency (ms)')
plt.savefig(OUTPUT_DIR / '04_latency_boxplot_high_load.png', dpi=400, bbox_inches='tight')
plt.close()

# 5. Performance Improvement % over Basic (Round Robin)
baseline = df[df['scheduler'] == "Round Robin"].set_index('Input_Rate')
df['Latency_Improvement'] = df.apply(
    lambda row: ((baseline.loc[row['Input_Rate'], 'Avg_Latency_ms'] - row['Avg_Latency_ms']) /
                 baseline.loc[row['Input_Rate'], 'Avg_Latency_ms']) * 100, axis=1)

plt.figure()
sns.barplot(data=df[df['Input_Rate'] == 12000], x='Scheduler', y='Latency_Improvement', palette="viridis")
plt.title('Latency Improvement over Basic Scheduler at 12,000 tps')
plt.ylabel('Latency Reduction (%)')
plt.savefig(OUTPUT_DIR / '05_latency_improvement_bar.png', dpi=400, bbox_inches='tight')
plt.close()

# 6. Throughput Efficiency
df['Efficiency'] = (df['Throughput'] / df['Input_Rate']) * 100
plt.figure()
sns.lineplot(data=df, x='Input_Rate', y='Efficiency', hue='Scheduler', 
             style='Scheduler', markers=True, linewidth=3)
plt.title('Throughput Efficiency (%)')
plt.xlabel('Input Rate (tuples/sec)')
plt.ylabel('Efficiency (%)')
plt.savefig(OUTPUT_DIR / '06_efficiency_vs_rate.png', dpi=400, bbox_inches='tight')
plt.close()

# 7. Combined Publication Figure (the most important one)
fig, axes = plt.subplots(2, 2, figsize=(18, 12))
fig.suptitle('RA-Stream vs Basic Scheduler: Performance Evaluation', fontsize=22, fontweight='bold')

sns.lineplot(ax=axes[0,0], data=df, x='Input_Rate', y='Avg_Latency_ms', hue='Scheduler', marker='o')
axes[0,0].set_title('Average Latency')

sns.lineplot(ax=axes[0,1], data=df, x='Input_Rate', y='Throughput', hue='Scheduler', marker='o')
axes[0,1].set_title('Throughput')

sns.lineplot(ax=axes[1,0], data=df, x='Input_Rate', y='CPU_Util', hue='Scheduler', marker='o')
axes[1,0].set_title('CPU Utilization (%)')

sns.lineplot(ax=axes[1,1], data=df, x='Input_Rate', y='Efficiency', hue='Scheduler', marker='o')
axes[1,1].set_title('Throughput Efficiency (%)')

plt.tight_layout(rect=[0, 0, 1, 0.95])
plt.savefig(OUTPUT_DIR / '07_combined_performance_paper.png', dpi=500, bbox_inches='tight')
plt.close()

print("\n All plots generated successfully!")
print(f"Location: {OUTPUT_DIR.resolve()}")
print("\nKey plots for your paper:")
print("• 07_combined_performance_paper.png     → Main results figure")
print("• 01_avg_latency_vs_rate.png           → Core latency result")
print("• 03_throughput_vs_rate.png            → Throughput scaling")
print("• 04_latency_boxplot_high_load.png     → Variance at high load")
print("• 05_latency_improvement_bar.png       → Quantified improvement")
