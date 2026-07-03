import json
import sys
import struct
import time
import pickle
import os
import math
import queue
import numpy as np
from functools import partial
from confluent_kafka import Consumer, Producer
from distributed import Client

# ==========================================
# 1. CONFIGURATION & CLUSTER PARAMETERS
# ==========================================
DASK_SCHEDULER = 'tcp://10.67.22.72:8786' #VM3
KAFKA_BROKER = '10.67.22.134:9092'        #VM2
TOPIC_INPUT = 'topic_stream'
TOPIC_RESULTS = 'topic_results'
SAMPLE_RATE = 20e6

RUN_ID = os.environ.get('RUN_ID', 'default_run')
SNAPSHOT_EVERY_N_CHUNKS = int(os.environ.get('SNAPSHOT_EVERY_N_CHUNKS', '50'))


# ==========================================
# 2. REMOTE WORKER COMPUTATION TASK
# ==========================================
def compute_fft(chunk_i, chunk_q):
    """
    This function executes remotely inside the RAM of VM4 and VM5.
    It takes lists of numbers, builds complex numbers, and computes the power spectrum.
    """
    import numpy as np
    import time

    compute_start = time.time()
    # Reconstruct complex signal vector: V = I + j*Q
    complex_signal = chunk_i + 1j * chunk_q
    # Run the raw Fast Fourier Transform
    fft_output = np.fft.fft(complex_signal)
    # Calculate Power Intensity Magnitude Squared
    power_spectrum = np.abs(fft_output) ** 2
    compute_end = time.time()

    return power_spectrum, compute_start, compute_end


# ==========================================
# 3. ONLINE STATISTICS (WELFORD)
# ==========================================
def new_latency_stats():
    return {"count": 0, "mean": 0.0, "M2": 0.0, "max": float("-inf"), "min": float("inf")}

def update_latency_stats(stats, x):
    stats["count"] += 1
    n = stats["count"]
    delta = x - stats["mean"]
    stats["mean"] += delta / n
    delta2 = x - stats["mean"]
    stats["M2"] += delta * delta2
    stats["max"] = max(stats["max"], x)
    stats["min"] = min(stats["min"], x)

def finalize_latency_stats(stats):
    n = stats["count"]
    variance = stats["M2"] / n if n > 0 else None
    return {
        "count": n,
        "mean_s": stats["mean"] if n > 0 else None,
        "std_s": math.sqrt(variance) if variance is not None else None,
        "max_s": stats["max"] if n > 0 else None,
        "min_s": stats["min"] if n > 0 else None,
    }


def compute_consumer_lag(consumer, partition_offsets):
    total_lag = 0
    for tp in consumer.assignment():
        try:
            low, high = consumer.get_watermark_offsets(tp, cached=False, timeout=1.0)
            current = partition_offsets.get(tp.partition, low - 1)
            total_lag += max(0, high - current - 1)
        except Exception:
            pass
    return total_lag


# ==========================================
# 4. CONCLUSION CALLBACK
# ==========================================
def handle_done(future, results_queue, active_futures, send_time, submit_time):
    try:
        psd_result, compute_start, compute_end = future.result()
        harvest_time = time.time()
        results_queue.put((psd_result, send_time, submit_time, compute_start, compute_end, harvest_time))
    except Exception as e:
        print(f"[WARNING] Future failed: {e}")
    finally:
        active_futures.discard(future)


# ==========================================
# 5. CORE ORCHESTRATION PIPELINE
# ==========================================
def main():
    print(f"[INFO] Connecting to Dask Scheduler at {DASK_SCHEDULER}...")
    try:
        client = Client(DASK_SCHEDULER)
        workers_count = len(client.scheduler_info()['workers'])
        print(f"[SUCCESS] Connected! Active worker nodes in cluster: {workers_count}")
    except Exception as e:
        print(f"[ERROR] Dask Master initialization failed: {e}")
        sys.exit(1)

    # Initialize Kafka Consumer
    consumer = Consumer({
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': f'dask-processor-{RUN_ID}',
        'auto.offset.reset': 'earliest',
        'fetch.min.bytes': 1048576,
        'message.max.bytes': 50000000
    })
    consumer.subscribe([TOPIC_INPUT])

    # Initialize Kafka Producer
    producer = Producer({
        'bootstrap.servers': KAFKA_BROKER,
        'linger.ms': 10,
        'message.max.bytes': 50000000
    })

    # Delivery callback to catch delivery errors
    def delivery_report(err, msg):
        if err is not None:
            print(f"[ERROR] Message delivery failed: {err}")

    print(f"[INFO] Connecting to Kafka Broker at {KAFKA_BROKER}...")
    print("[INFO] Pipeline listening for incoming data stream...")

    # Pipeline State Tracking
    active_futures = set()  # Retain references to prevent Dask garbage collection
    results_queue = queue.Queue()

    global_average = None
    total_processed = 0
    partition_offsets = {}
    end_signal_partitions = set()
    stream_ended = False

    end_to_end_stats = new_latency_stats()
    queue_wait_stats = new_latency_stats()
    dask_overhead_stats = new_latency_stats()
    service_time_stats = new_latency_stats()
    transit_stats = new_latency_stats()

    snapshots = []
    pipeline_start_time = time.time()

    try:
        while True:
            # 1. Kafka ingestion
            msg = consumer.poll(0.1)

            if msg is not None and not msg.error():
                try:
                    payload_bytes = msg.value()
                    partition_offsets[msg.partition()] = msg.offset()

                    header_len = struct.unpack('>I', payload_bytes[:4])[0]
                    header_json_bytes = payload_bytes[4:4 + header_len]
                    header_data = json.loads(header_json_bytes.decode('utf-8'))

                    if header_data.get("event") == "END_OF_STREAM":
                        end_signal_partitions.add(msg.partition())
                        print(f"[INFO] END_OF_STREAM in partition {msg.partition()} "
                              f"({len(end_signal_partitions)} partitions signaled)")
                    else:
                        n_samples = header_data["n_samples"]
                        float_bytes_len = n_samples * 4
                        start_i = 4 + header_len
                        end_i = start_i + float_bytes_len
                        start_q = end_i
                        end_q = start_q + float_bytes_len

                        # Arrays numpy float32 straight from the payload, copied to avoid memory issues
                        chunk_i = np.frombuffer(payload_bytes[start_i:end_i], dtype=np.float32).copy()
                        chunk_q = np.frombuffer(payload_bytes[start_q:end_q], dtype=np.float32).copy()

                        submit_time = time.time()
                        future = client.submit(compute_fft, chunk_i, chunk_q)
                        active_futures.add(future)
                        future.add_done_callback(
                            partial(handle_done,
                                    results_queue=results_queue,
                                    active_futures=active_futures,
                                    send_time=header_data["send_time"],
                                    submit_time=submit_time)
                        )

                except Exception as e:
                    print(f"[WARNING] Bypassed an unparseable frame packet: {e}")

            # 2. Drain completed results
            while True:
                try:
                    psd_result, send_time, submit_time, compute_start, compute_end, harvest_time = \
                        results_queue.get_nowait()
                except queue.Empty:
                    break

                update_latency_stats(end_to_end_stats, harvest_time - send_time)
                update_latency_stats(queue_wait_stats, submit_time - send_time)
                update_latency_stats(dask_overhead_stats, compute_start - submit_time)
                update_latency_stats(service_time_stats, compute_end - compute_start)
                update_latency_stats(transit_stats, harvest_time - compute_end)

                if global_average is None:
                    global_average = np.zeros_like(psd_result)

                total_processed += 1
                global_average = global_average + (psd_result - global_average) / total_processed

                if total_processed % 10 == 0:
                    shifted_avg = np.fft.fftshift(global_average)
                    freqs = np.fft.fftfreq(len(global_average), d=1 / SAMPLE_RATE)
                    shifted_freqs = np.fft.fftshift(freqs)
                    decimated_freqs = shifted_freqs[::10]
                    decimated_power = shifted_avg[::10]

                    spectrum_payload = {
                        "frequencies": decimated_freqs.tolist(),
                        "power": decimated_power.tolist(),
                        "total_chunks": total_processed,
                        "max_power_intensity": float(np.max(shifted_avg)),
                        "mean_power_intensity": float(np.mean(shifted_avg))
                    }
                    producer.produce(TOPIC_RESULTS, value=json.dumps(spectrum_payload).encode('utf-8'),
                                      callback=delivery_report)
                    producer.poll(0)
                    print(f"[UPDATE] Aggregated {total_processed} chunks. "
                          f"Pending futures: {len(active_futures)}. Spectrum broadcasted.")

                if total_processed % SNAPSHOT_EVERY_N_CHUNKS == 0:
                    now = time.time()
                    try:
                        dask_active_tasks = sum(len(v) for v in client.processing().values())
                    except Exception:
                        dask_active_tasks = None

                    snapshots.append({
                        "timestamp": now,
                        "elapsed_s": now - pipeline_start_time,
                        "total_processed": total_processed,
                        "pending_futures": len(active_futures),
                        "consumer_lag": compute_consumer_lag(consumer, partition_offsets),
                        "dask_active_tasks": dask_active_tasks,
                    })

            # 3. Stop condition: all partitions signaled end, and nothing pending
            assignment = consumer.assignment()
            if (assignment
                    and len(end_signal_partitions) >= len(assignment)
                    and len(active_futures) == 0
                    and results_queue.empty()):
                stream_ended = True
                print("[INFO] All partitions signaled end of stream; processing queue empty.")
                break

    except KeyboardInterrupt:
        print("\n[INFO] Intercepted termination signal. Powering down pipeline components...")
    finally:
        results = {
            "run_id": RUN_ID,
            "stream_ended_naturally": stream_ended,
            "num_partitions_assigned": len(consumer.assignment()) if consumer.assignment() else None,
            "total_chunks_processed": total_processed,
            "pipeline_start_time": pipeline_start_time,
            "pipeline_end_time": time.time(),
            "wall_clock_duration_s": time.time() - pipeline_start_time,
            "end_to_end_latency": finalize_latency_stats(end_to_end_stats),
            "queue_wait": finalize_latency_stats(queue_wait_stats),
            "dask_scheduling_overhead": finalize_latency_stats(dask_overhead_stats),
            "service_time": finalize_latency_stats(service_time_stats),
            "result_transit": finalize_latency_stats(transit_stats),
            "snapshots": snapshots,
        }

        metrics_filename = f"processing_metrics_{RUN_ID}.pkl"
        with open(metrics_filename, "wb") as f:
            pickle.dump(results, f)

        print(f"\n[METRICS] Processing metrics exported to {metrics_filename}")
        print(json.dumps({k: v for k, v in results.items() if k != 'snapshots'}, indent=2))

        consumer.close()
        producer.flush()
        client.close()
        print("[SUCCESS] Processing orchestrator closed cleanly.")


if __name__ == "__main__":
    main()

