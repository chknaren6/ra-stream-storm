import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import warnings
warnings.filterwarnings("ignore")

# ================== SETTINGS ==================
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (12, 8)
plt.rcParams['font.size'] = 14
plt.rcParams['axes.labelsize'] = 16
plt.rcParams['axes.titlesize'] = 18
plt.rcParams['legend.fontsize'] = 12

OUTPUT_DIR = Path("../results/plots")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

CSV_PATH = "/home/karthik_hadoop/ra-stream-metrics/local/taxi/metrics.csv"

# Load data
df = pd.read_csv(CSV_PATH)

# Clean column names for easier use
df = df.rename(columns={
    'stream_rate_tps': 'Input_Rate',
    'avg_latency_ms': 'Avg_Latency_ms',
    'max_latency_ms': 'Max_Latency_ms',
    'throughput_tps': 'Throughput',
    'avg_cpu_pct': 'CPU_Util',
    'avg_mem_pct': 'Mem_Util'
})

# Nice scheduler names for legend
scheduler_map = {
    "Round Robin": "Basic (Round Robin)",
    "RaStream": "RA-Stream",
    "RaStreamFT": "RA-Stream + FT"
}
df['Scheduler'] = df['scheduler'].map(scheduler_map)

print(f"Loaded {len(df)} records")
print("Schedulers:", df['Scheduler'].unique())

# ================== PLOT 1: Average Latency vs Input Rate ==================
plt.figure()
sns.lineplot(data=df, x='Input_Rate', y='Avg_Latency_ms',
             hue='Scheduler', style='Scheduler', markers=True, linewidth=2.5, markersize=8)
plt.title('Average Latency vs Input Rate')
plt.xlabel('Input Rate (tuples/sec)')
plt.ylabel('Average Latency (ms)')
plt.legend(title='Scheduling Approach')
plt.tight_layout()
plt.savefig(OUTPUT_DIR / '01_avg_latency_vs_rate.png', dpi=400, bbox_inches='tight')
plt.close()

# ================== PLOT 2: Max Latency vs Input Rate ==================
plt.figure()
sns.lineplot(data=df, x='Input_Rate', y='Max_Latency_ms',
             hue='Scheduler', style='Scheduler', markers=True, linewidth=2.5)
plt.title('Maximum Latency vs Input Rate')
plt.xlabel('Input Rate (tuples/sec)')
plt.ylabel('Max Latency (ms)')
plt.legend(title='Scheduling Approach')
plt.tight_layout()
plt.savefig(OUTPUT_DIR / '02_max_latency_vs_rate.png', dpi=400, bbox_inches='tight')
plt.close()

# ================== PLOT 3: Throughput vs Input Rate ==================
plt.figure()
sns.lineplot(data=df, x='Input_Rate', y='Throughput',
             hue='Scheduler', style='Scheduler', markers=True, linewidth=2.5)
plt.title('Achieved Throughput vs Input Rate')
plt.xlabel('Input Rate (tuples/sec)')
plt.ylabel('Throughput (tuples/sec)')
plt.legend(title='Scheduling Approach')
plt.tight_layout()
plt.savefig(OUTPUT_DIR / '03_throughput_vs_rate.png', dpi=400, bbox_inches='tight')
plt.close()

# ================== PLOT 4: Throughput Efficiency ==================
df['Efficiency'] = (df['Throughput'] / df['Input_Rate']) * 100

plt.figure()
sns.lineplot(data=df, x='Input_Rate', y='Efficiency',
             hue='Scheduler', style='Scheduler', markers=True, linewidth=2.5)
plt.title('Throughput Efficiency (%)')
plt.xlabel('Input Rate (tuples/sec)')
plt.ylabel('Efficiency (%)')
plt.legend(title='Scheduling Approach')
plt.tight_layout()
plt.savefig(OUTPUT_DIR / '04_efficiency_vs_rate.png', dpi=400, bbox_inches='tight')
plt.close()

# ================== PLOT 5: CPU Utilization ==================
plt.figure()
sns.lineplot(data=df, x='Input_Rate', y='CPU_Util',
             hue='Scheduler', style='Scheduler', markers=True, linewidth=2.5)
plt.title('CPU Utilization vs Input Rate')
plt.xlabel('Input Rate (tuples/sec)')
plt.ylabel('Average CPU Utilization (%)')
plt.legend(title='Scheduling Approach')
plt.tight_layout()
plt.savefig(OUTPUT_DIR / '05_cpu_vs_rate.png', dpi=400, bbox_inches='tight')
plt.close()

# ================== PLOT 6: Bar Chart at Max Rate (12,000 tps) ==================
max_rate_df = df[df['Input_Rate'] == 12000].copy()

plt.figure(figsize=(10, 6))
sns.barplot(data=max_rate_df, x='Scheduler', y='Avg_Latency_ms', palette='Blues_d')
plt.title('Average Latency at 12,000 tuples/sec')
plt.ylabel('Avg Latency (ms)')
plt.tight_layout()
plt.savefig(OUTPUT_DIR / '06_bar_latency_at_12k.png', dpi=400, bbox_inches='tight')
plt.close()

plt.figure(figsize=(10, 6))
sns.barplot(data=max_rate_df, x='Scheduler', y='Throughput', palette='Greens_d')
plt.title('Throughput at 12,000 tuples/sec')
plt.ylabel('Throughput (tps)')
plt.tight_layout()
plt.savefig(OUTPUT_DIR / '07_bar_throughput_at_12k.png', dpi=400, bbox_inches='tight')
plt.close()

# ================== PLOT 7: Combined Publication Figure ==================
fig, axes = plt.subplots(2, 2, figsize=(16, 12))
fig.suptitle('Performance Comparison of Basic, RA-Stream, and RA-Stream+FT',
             fontsize=20, fontweight='bold', y=0.98)

sns.lineplot(ax=axes[0,0], data=df, x='Input_Rate', y='Avg_Latency_ms',
             hue='Scheduler', marker='o')
axes[0,0].set_title('Average Latency')

sns.lineplot(ax=axes[0,1], data=df, x='Input_Rate', y='Throughput',
             hue='Scheduler', marker='o')
axes[0,1].set_title('Throughput')

sns.lineplot(ax=axes[1,0], data=df, x='Input_Rate', y='CPU_Util',
             hue='Scheduler', marker='o')
axes[1,0].set_title('CPU Utilization (%)')

sns.lineplot(ax=axes[1,1], data=df, x='Input_Rate', y='Efficiency',
             hue='Scheduler', marker='o')
axes[1,1].set_title('Throughput Efficiency (%)')

plt.tight_layout(rect=[0, 0, 1, 0.95])
plt.savefig(OUTPUT_DIR / '08_combined_performance.png', dpi=400, bbox_inches='tight')
plt.close()

print("\nAll plots generated successfully!")
print(f"Plots saved in: {OUTPUT_DIR.resolve()}")
print("\nRecommended plots for your paper:")
print("1. 08_combined_performance.png          → Main result figure")
print("2. 01_avg_latency_vs_rate.png          → Latency comparison")
print("3. 03_throughput_vs_rate.png           → Throughput scaling")
print("4. 06_bar_latency_at_12k.png           → Highlight at peak load")
print("5. 07_bar_throughput_at_12k.png        → Throughput at peak load")