import os
import sys
import time
import json
import struct
import scipy.fft
import multiprocessing
import gc
from dask.distributed import Client, LocalCluster, performance_report
import numpy as np
import dask
import dask.array as da
from confluent_kafka import Consumer, Producer, KafkaError
import matplotlib.pyplot
matplotlib.use('Agg')
import matplotlib.pyplot as plt

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', '10.67.22.212:9092')
INPUT_TOPIC = 'topic_stream'     
OUTPUT_TOPIC = 'topic_results'   
TOTAL_FILES_EXPECTED = 31  

FS = 2_000_000
N_BINS = 2048
N_DASK_CHUNKS = 4

OUTPUT_DIR = "fft_results"  
os.makedirs(OUTPUT_DIR, exist_ok=True)

def parse_message(raw_bytes: bytes):
    header_len = struct.unpack('>I', raw_bytes[:4])[0]
    header_json = raw_bytes[4:4 + header_len]
    header = json.loads(header_json.decode('utf-8'))

    n_samples = header["n_samples"]
    payload_start = 4 + header_len
    bytes_per_array = n_samples * 4

    i_bytes = raw_bytes[payload_start: payload_start + bytes_per_array]
    q_bytes = raw_bytes[payload_start + bytes_per_array: payload_start + 2 * bytes_per_array]

    arr_i = np.frombuffer(i_bytes, dtype='<f4')
    arr_q = np.frombuffer(q_bytes, dtype='<f4')

    return header, arr_i, arr_q

def process_file_batch_dask(scans_i: np.ndarray, scans_q: np.ndarray, client: Client):
    signal = (scans_i + 1j * scans_q).astype(np.complex64).reshape(-1, N_BINS)
    new_n_scans = signal.shape[0]

    # Scatter data to worker RAM to prevent graph bloat
    [signal_future] = client.scatter([signal])
    
    signal_da = da.from_delayed(
        dask.delayed(lambda x: x)(signal_future),
        shape=signal.shape,
        dtype=signal.dtype
    ).rechunk((max(1, new_n_scans // N_DASK_CHUNKS), N_BINS))

    spectra_da = signal_da.map_blocks(lambda chunk: scipy.fft.fft(chunk, axis=-1, workers=1), dtype=np.complex64)
    power_da = (da.absolute(spectra_da) ** 2) / (N_BINS ** 2)

    mean_power_da = da.mean(power_da, axis=0)
    std_power_da = da.std(power_da, axis=0)

    mean_power, std_power = dask.compute(mean_power_da, std_power_da)
    freqs = np.fft.fftfreq(N_BINS, d=1 / FS)

    return {
        "mean_power": mean_power.astype(np.float32),
        "std_power": std_power.astype(np.float32),
        "freqs": freqs.astype(np.float32),
        "n_scans": new_n_scans, 
    }

def update_global_average(global_state: dict, result: dict) -> dict:
    M_n = result["n_scans"]
    mean_n = result["mean_power"]
    std_n = result["std_power"]
    M2_n = M_n * std_n**2

    if global_state["mean_power"] is None:
        global_state["mean_power"] = mean_n.copy()
        global_state["M2"] = M2_n.copy()
        global_state["total_scans"] = M_n
    else:
        M_prev = global_state["total_scans"]
        mean_prev = global_state["mean_power"]
        M2_prev = global_state["M2"]
        M_total = M_prev + M_n
        delta = mean_n - mean_prev

        global_state["mean_power"] = (M_prev * mean_prev + M_n * mean_n) / M_total
        global_state["M2"] = M2_prev + M2_n + (delta**2) * (M_prev * M_n / M_total)
        global_state["total_scans"] = M_total

    global_state["files_count"] += 1
    return global_state

def save_benchmark_report(history):
    """Generates an explicit performance summary and plotting map."""
    report_path = os.path.join(OUTPUT_DIR, "benchmark_report.json")
    plot_path = os.path.join(OUTPUT_DIR, "pipeline_performance.png")
    
    # Save raw structured metrics
    with open(report_path, "w") as f:
        json.dump(history, f, indent=4)
    print(f"[INFO] Raw metrics saved successfully to {report_path}")
    
    # Extract structural lists for visualization
    files = [h["file_id"] for h in history]
    total_times = [h["total_time_s"] for h in history]
    throughputs = [h["throughput_mb_s"] for h in history]
    
    # Render Matplotlib Dual Axis Benchmark Figure
    fig, ax1 = plt.subplots(figsize=(14, 6))
    
    color = '#1f77b4'
    ax1.set_xlabel('File Identifier (Chronological Input Window)', fontweight='bold', labelpad=10)
    ax1.set_ylabel('Total Processing Latency (seconds)', color=color, fontweight='bold')
    ax1.plot(files, total_times, color=color, marker='o', linewidth=2, label='Processing Time')
    ax1.tick_params(axis='y', labelcolor=color)
    ax1.axhline(y=4.2, color='r', linestyle='--', alpha=0.7, label='Max Real-Time Target (4.2s)')
    ax1.set_ylim(0, max(total_times + [4.2]) + 0.5)
    
    ax2 = ax1.twinx()  
    color = '#2ca02c'
    ax2.set_ylabel('Effective Throughput (MB/s)', color=color, fontweight='bold')
    ax2.plot(files, throughputs, color=color, marker='s', linestyle=':', linewidth=1.5, label='Throughput')
    ax2.tick_params(axis='y', labelcolor=color)
    ax2.axhline(y=16.0, color='orange', linestyle=':', alpha=0.7, label='Input Stream Speed (16 MB/s)')
    
    plt.title('QUAX Online Data Pipeline - High-Speed Benchmark Report', fontsize=14, fontweight='bold', pad=15)
    ax1.set_xticklabels(files, rotation=45, ha='right')
    fig.tight_layout()
    
    plt.savefig(plot_path, dpi=300)
    plt.close(fig)
    print(f"[INFO] Performance visualization matrix exported to {plot_path}")

def main():
    print("[INFO] Initializing processor I/Q FFT (Dask batch mode)...")
    n_cores = multiprocessing.cpu_count()
    cluster = LocalCluster(n_workers=1, threads_per_worker=n_cores, memory_limit='auto')
    dask_client = Client(cluster)
    
    kafka_servers_str = KAFKA_BOOTSTRAP_SERVERS
    if isinstance(KAFKA_BOOTSTRAP_SERVERS, list):
        kafka_servers_str = ','.join(KAFKA_BOOTSTRAP_SERVERS)

    print(f"[INFO] Using Kafka bootstrap servers: {kafka_servers_str}")
    
    consumer = Consumer({
        'bootstrap.servers': kafka_servers_str,
        'group.id': 'quax-processor-group',
        'auto.offset.reset': 'latest',       
        'fetch.message.max.bytes': 10485760, 
        'queued.max.messages.kbytes': 32768  
    })
    consumer.subscribe([INPUT_TOPIC])

    producer = Producer({
        'bootstrap.servers': kafka_servers_str,
        'message.max.bytes': 10485760
    })
    print("[INFO] Connected to Kafka successfully via confluent-kafka.")

    buffers_i, buffers_q, scan_order, file_metadata = {}, {}, {}, {}
    global_state = {"mean_power": None, "M2": None, "total_scans": 0, "files_count": 0}
    
    # --- ACCUMULATOR LIST FOR BENCHMARK EXPORT ---
    benchmark_history = []

    print("[INFO] Awaiting scans I/Q. Batch+Dask processing ACTIVE.")
    
    try:
        report_html_path = os.path.join(OUTPUT_DIR, "dask_performance_report.html")
        with performance_report(filename=report_html_path):
            while True:
                msg = consumer.poll(timeout=1.0)
                
                if msg is None: 
                    continue
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        print(f"[ERROR] Kafka Error: {msg.error()}")
                    continue

                t_arrival = time.perf_counter()
                raw_bytes = msg.value()

                t_start_parse = time.perf_counter()
                header, arr_i, arr_q = parse_message(raw_bytes)
                t_end_parse = time.perf_counter()

                file_id = header["file_id"]
                scan_id = header["scan_id"]
                total_scans = header["total_scans"]

                if file_id not in buffers_i:
                    buffers_i[file_id] = [None] * total_scans
                    buffers_q[file_id] = [None] * total_scans
                    scan_order[file_id] = 0
                    file_metadata[file_id] = {
                        "total_scans": total_scans,
                        "t_file_start": t_arrival,
                        "bytes_processed": 0,
                        "parse_times": [],
                    }

                buffers_i[file_id][scan_id - 1] = arr_i
                buffers_q[file_id][scan_id - 1] = arr_q
                scan_order[file_id] += 1

                file_metadata[file_id]["parse_times"].append(t_end_parse - t_start_parse)
                file_metadata[file_id]["bytes_processed"] += (len(arr_i) + len(arr_q)) * 4

                if scan_order[file_id] == total_scans:
                    t_start_stack = time.perf_counter()
                    scans_i = np.stack(buffers_i[file_id], axis=0)
                    scans_q = np.stack(buffers_q[file_id], axis=0)
                    t_end_stack = time.perf_counter()

                    t_start_dask = time.perf_counter()
                    result = process_file_batch_dask(scans_i, scans_q, dask_client)
                    t_end_dask = time.perf_counter()

                    t_file_end = time.perf_counter()
                    m = file_metadata[file_id]
                    total_time_latency = t_file_end - m["t_file_start"]
                    total_mb = m["bytes_processed"] / (1024 * 1024)
                    throughput_mb_s = total_mb / total_time_latency

                    print("\n" + "=" * 60)
                    print(f" BENCHMARK REPORT FOR RUN / FILE: {file_id}")
                    print("=" * 60)
                    print(f" Total samples processed      : {result['n_scans'] * N_BINS:,} samples I/Q")
                    print(f" Raw Data Volume              : {total_mb:.2f} MB")
                    print(f" Total Processing Time        : {total_time_latency:.4f} seconds (Target: < 4.2s)")
                    print(f" Effective Throughput         : {throughput_mb_s:.2f} MB/s (Input Target: 16 MB/s)")
                    print("-" * 60)
                    print(" TIMING BREAKDOWN:")
                    print(f"   Avg parsing per scan       : {np.mean(m['parse_times'])*1000:.3f} ms")
                    print(f"   Stack scans -> matrix      : {(t_end_stack - t_start_stack)*1000:.3f} ms")
                    print(f"   Dask FFT batch             : {(t_end_dask - t_start_dask)*1000:.3f} ms")
                    print("=" * 60 + "\n")

                    # --- APPEND LOG TO THE EXPORT HISTORY TREE ---
                    benchmark_history.append({
                        "file_id": file_id,
                        "total_time_s": total_time_latency,
                        "throughput_mb_s": throughput_mb_s,
                        "parsing_ms": float(np.mean(m['parse_times'])*1000),
                        "stacking_ms": float((t_end_stack - t_start_stack)*1000),
                        "dask_fft_ms": float((t_end_dask - t_start_dask)*1000)
                    })

                    global_state = update_global_average(global_state, result)
                    global_std_power = np.sqrt(global_state["M2"] / global_state["total_scans"])
                    is_final = global_state["files_count"] >= TOTAL_FILES_EXPECTED

                    output_payload = {
                        "n_files_processed": global_state["files_count"],
                        "n_scans_total": int(global_state["total_scans"]),
                        "last_file_id": file_id,
                        "averaged_power_spectrum": global_state["mean_power"].tolist(), 
                        "std_power_spectrum": global_std_power.tolist(),                
                        "frequencies": result["freqs"].tolist(),
                        "benchmark_throughput_mbs": throughput_mb_s,
                        "is_final": is_final
                    }
                    
                    producer.produce(OUTPUT_TOPIC, value=json.dumps(output_payload).encode('utf-8'))
                    producer.poll(0)

                    del buffers_i[file_id]
                    del buffers_q[file_id]
                    del scan_order[file_id]
                    del file_metadata[file_id]
                    gc.collect()

                    if is_final:
                        print(f"[INFO] All files processed ({global_state['files_count']}/{TOTAL_FILES_EXPECTED}). Executing automated metric report closure...")
                        save_benchmark_report(benchmark_history)
                        break

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        producer.flush()
        dask_client.close()

if __name__ == "__main__":
    main()