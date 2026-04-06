import pandas as pd
import matplotlib.pyplot as plt
import os
df = pd.read_csv("/home/karthik_hadoop/ra-stream-metrics/local/taxi/metricsFt.csv")

# Convert timestamp
df['time'] = (df['timestamp'] - df['timestamp'].min()) / 1000

os.makedirs("plots", exist_ok=True)

df['throughput_smooth'] = df['throughput'].rolling(20).mean()
df['latency_smooth'] = df['latency'].rolling(20).mean()
df['cpu_smooth'] = df['cpu'].rolling(20).mean()

# -----------------------------
# 1. THROUGHOUT vs TIME
# -----------------------------
plt.figure()
plt.plot(df['time'], df['throughput_smooth'])
plt.xlabel("Time (s)")
plt.ylabel("Throughput")
plt.title("Throughput Stability (FT)")
plt.grid()
plt.savefig("plots/throughput.png")

# -----------------------------
# 2. LATENCY vs TIME
# -----------------------------
plt.figure()
plt.plot(df['time'], df['latency_smooth'])
plt.xlabel("Time (s)")
plt.ylabel("Latency (ms)")
plt.title("Latency Stability (FT)")
plt.grid()
plt.savefig("plots/latency.png")

# -----------------------------
# 3. CPU USAGE
# -----------------------------
plt.figure()
plt.plot(df['time'], df['cpu_smooth'])
plt.xlabel("Time (s)")
plt.ylabel("CPU %")
plt.title("CPU Usage (Fault Tolerance Overhead)")
plt.grid()
plt.savefig("plots/cpu.png")

# -----------------------------
# 4. MEMORY USAGE
# -----------------------------
plt.figure()
plt.plot(df['time'], df['memory'])
plt.xlabel("Time (s)")
plt.ylabel("Memory (MB)")
plt.title("Memory Usage")
plt.grid()
plt.savefig("plots/memory.png")

# -----------------------------
# 5. FAILURE ANALYSIS (KEY GRAPH 🔥)
# -----------------------------
plt.figure()
plt.plot(df['time'], df['failed'], label="Failed")
plt.plot(df['time'], df['retried'], label="Retried")
plt.plot(df['time'], df['dropped'], label="Dropped")
plt.xlabel("Time (s)")
plt.ylabel("Count")
plt.title("Fault Tolerance Behavior")
plt.legend()
plt.grid()
plt.savefig("plots/fault_behavior.png")

# -----------------------------
# 6. RECOVERY EFFICIENCY (VERY IMPORTANT 🔥)
# -----------------------------
df['recovery_ratio'] = df['retried'] / (df['failed'] + 1)

plt.figure()
plt.plot(df['time'], df['recovery_ratio'])
plt.xlabel("Time (s)")
plt.ylabel("Recovery Ratio")
plt.title("Recovery Efficiency (Retried / Failed)")
plt.grid()
plt.savefig("plots/recovery_ratio.png")

# -----------------------------
# 7. THROUGHPUT vs LATENCY (TRADEOFF)
# -----------------------------
plt.figure()
plt.scatter(df['throughput'], df['latency'])
plt.xlabel("Throughput")
plt.ylabel("Latency")
plt.title("Throughput vs Latency Tradeoff")
plt.grid()
plt.savefig("plots/tradeoff.png")

# -----------------------------
# 8. ZONE LOAD DISTRIBUTION
# -----------------------------
plt.figure()
df.groupby("zone")['tripCount'].sum().nlargest(10).plot(kind='bar')
plt.title("Top 10 Busy Zones")
plt.ylabel("Trips")
plt.savefig("plots/zones.png")

# -----------------------------
# 9. FAILURE RATE OVER TIME
# -----------------------------
df['failure_rate'] = df['failed'] / (df['processed'] + 1)

plt.figure()
plt.plot(df['time'], df['failure_rate'])
plt.xlabel("Time (s)")
plt.ylabel("Failure Rate")
plt.title("Failure Rate Over Time")
plt.grid()
plt.savefig("plots/failure_rate.png")

# -----------------------------
# 10. SYSTEM STABILITY (VERY IMPRESSIVE 🔥)
# -----------------------------
plt.figure()
plt.plot(df['time'], df['throughput_smooth'], label="Throughput")
plt.plot(df['time'], df['latency_smooth'], label="Latency")
plt.legend()
plt.title("System Stability Under Faults")
plt.grid()
plt.savefig("plots/stability.png")

print("✅ ALL FT PLOTS GENERATED in /plots folder")


