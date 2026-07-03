import pickle
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

# Set beautiful default styles for the plots
plt.style.use('seaborn-v0_8-darkgrid')
sns.set_context("notebook", font_scale=1.1)

# The list of run IDs we want to analyze
rate_runs = ['rate_4mbps', 'rate_16mbps', 'rate_30mbps']
partition_runs = ['partition_1part_rate16', 'partition_3part_rate16']

# Helper function to safely load a pickle file
def load_pkl(filename):
    try:
        with open(filename, 'rb') as f:
            return pickle.load(f)
    except FileNotFoundError:
        print(f"[WARNING] Could not find {filename}. Skipping...")
        return None

# Create a figure with 4 subplots (2x2 grid)
fig, axes = plt.subplots(2, 2, figsize=(18, 12))
fig.suptitle('QUAX Distributed FFT Pipeline: Performance & Bottleneck Analysis', fontsize=20, fontweight='bold', y=0.98)

# =====================================================================
# PLOT 1: Consumer Lag over Time (Finding the Saturation Point)
# =====================================================================
ax1 = axes[0, 0]
for run in rate_runs:
    data = load_pkl(f'processing_metrics_{run}.pkl')
    if data and 'snapshots' in data:
        df = pd.DataFrame(data['snapshots'])
        # Filter out massive initial startup spikes if needed, or just plot raw
        ax1.plot(df['elapsed_s'], df['consumer_lag'], marker='o', markersize=3, linewidth=2, label=run.replace('_', ' ').upper())

ax1.set_title('Queueing Theory: Consumer Lag vs. Time', fontsize=14, fontweight='bold')
ax1.set_xlabel('Elapsed Time (Seconds)')
ax1.set_ylabel('Messages Waiting in Kafka')
ax1.legend()

# =====================================================================
# PLOT 2: Dask Pending Futures (Cluster Load)
# =====================================================================
ax2 = axes[0, 1]
for run in rate_runs:
    data = load_pkl(f'processing_metrics_{run}.pkl')
    if data and 'snapshots' in data:
        df = pd.DataFrame(data['snapshots'])
        ax2.plot(df['elapsed_s'], df['pending_futures'], marker='s', markersize=3, linewidth=2, label=run.replace('_', ' ').upper())

ax2.set_title('Dask Cluster Stress: Pending Futures vs. Time', fontsize=14, fontweight='bold')
ax2.set_xlabel('Elapsed Time (Seconds)')
ax2.set_ylabel('Unassigned FFT Tasks')
ax2.legend()

# =====================================================================
# PLOT 3: Latency Stage Breakdown (Welford Stats)
# =====================================================================
ax3 = axes[1, 0]
latency_stages = ['queue_wait', 'dask_scheduling_overhead', 'service_time', 'result_transit']
latency_data = []

for run in rate_runs:
    data = load_pkl(f'processing_metrics_{run}.pkl')
    if data:
        row = {'Run': run.replace('rate_', '').replace('mbps', ' MB/s')}
        for stage in latency_stages:
            row[stage] = data.get(stage, {}).get('mean_s', 0) * 1000  # Convert to milliseconds
        latency_data.append(row)

df_lat = pd.DataFrame(latency_data).set_index('Run')
if not df_lat.empty:
    df_lat.plot(kind='bar', stacked=True, ax=ax3, colormap='viridis', edgecolor='black')
    ax3.set_title('Micro-Latency Breakdown (End-to-End)', fontsize=14, fontweight='bold')
    ax3.set_xlabel('Data Streaming Rate')
    ax3.set_ylabel('Average Latency (Milliseconds)')
    ax3.legend(title='Pipeline Stage', labels=['Kafka Queue Wait', 'Dask Overhead', 'FFT Math (Service)', 'Transit Back'])
    ax3.tick_params(axis='x', rotation=0)

# =====================================================================
# PLOT 4: Partition Scaling Impact (1 Part vs 3 Parts)
# =====================================================================
ax4 = axes[1, 1]
part_data = []
for run in partition_runs:
    p_data = load_pkl(f'processing_metrics_{run}.pkl')
    if p_data:
        label = "1 Partition" if "1part" in run else "3 Partitions"
        # Grab Dask Overhead and Queue wait specifically
        dask_overhead = p_data.get('dask_scheduling_overhead', {}).get('mean_s', 0) * 1000
        queue_wait = p_data.get('queue_wait', {}).get('mean_s', 0) * 1000
        part_data.append({'Setup': label, 'Queue Wait (ms)': queue_wait, 'Dask Overhead (ms)': dask_overhead})

df_part = pd.DataFrame(part_data).set_index('Setup')
if not df_part.empty:
    df_part.plot(kind='bar', ax=ax4, color=['#e74c3c', '#3498db'], edgecolor='black')
    ax4.set_title('Kafka Partitioning Performance (1 vs 3 Lanes at 16 MB/s)', fontsize=14, fontweight='bold')
    ax4.set_xlabel('Architecture Setup')
    ax4.set_ylabel('Time (Milliseconds)')
    ax4.tick_params(axis='x', rotation=0)

# Adjust layout so labels don't overlap and save the file
plt.tight_layout()
plt.subplots_adjust(top=0.92) # Leave room for the main title

# Save to a high-res image for your report!
output_file = 'QUAX_Pipeline_Analysis.png'
plt.savefig(output_file, dpi=300, bbox_inches='tight')
print(f"[SUCCESS] Beautiful benchmark plots saved successfully to {output_file}!")

# Display the plot if running in a Notebook
plt.show()