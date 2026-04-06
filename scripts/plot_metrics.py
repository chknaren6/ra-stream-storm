import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_csv("/home/karthik_hadoop/ra-stream-metrics/local/taxi/metricsFt.csv")

# Convert timestamp
df['time'] = (df['timestamp'] - df['timestamp'].min()) / 1000

# -------------------------
# 1. Throughput over time
# -------------------------
plt.figure()
plt.plot(df['time'], df['throughput'])
plt.xlabel("Time (s)")
plt.ylabel("Throughput (tuples/sec)")
plt.title("Throughput vs Time")
plt.grid()
plt.savefig("throughput.png")

# -------------------------
# 2. Latency over time
# -------------------------
plt.figure()
plt.plot(df['time'], df['latency'])
plt.xlabel("Time (s)")
plt.ylabel("Latency (ms)")
plt.title("Latency vs Time")
plt.grid()
plt.savefig("latency.png")

# -------------------------
# 3. Zone vs Avg Fare
# -------------------------
plt.figure()
df.groupby("zone")['avgFare'].mean().plot(kind='bar')
plt.title("Average Fare per Zone")
plt.ylabel("Avg Fare")
plt.savefig("avg_fare_zone.png")

# -------------------------
# 4. Trips per Zone
# -------------------------
plt.figure()
df.groupby("zone")['tripCount'].sum().plot(kind='bar')
plt.title("Trips per Zone")
plt.savefig("trips_zone.png")

# -------------------------
# 5. Throughput Distribution
# -------------------------
plt.figure()
plt.hist(df['throughput'], bins=20)
plt.title("Throughput Distribution")
plt.savefig("throughput_hist.png")

# -------------------------
# 6. Latency Distribution
# -------------------------
plt.figure()
plt.hist(df['latency'], bins=20)
plt.title("Latency Distribution")
plt.savefig("latency_hist.png")

print("All plots generated ✅")
