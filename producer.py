import os
import time
import json
import struct
import pickle
import boto3
import numpy as np
import gc
import urllib3
from confluent_kafka import Producer

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

ACCESS_KEY = os.environ.get('S3_ACCESS_KEY')
SECRET_KEY = os.environ.get('S3_SECRET_KEY')
ENDPOINT_URL = os.environ.get('S3_ENDPOINT_URL')
BUCKET_NAME = os.environ.get('S3_BUCKET_NAME')
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', '10.67.22.134:9092')
TOPIC = 'topic_stream'

TARGET_RATE_MB_S = float(os.environ.get('STREAM_RATE_MB_S', '16.0'))
KAFKA_NUM_PARTITIONS = int(os.environ.get('KAFKA_NUM_PARTITIONS', '1'))
RUN_ID = os.environ.get('RUN_ID', 'default_run')

print(f"[INFO] Connecting to CloudVeneto S3 at {ENDPOINT_URL}...")
s3_client = boto3.client(
    's3',
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    endpoint_url=ENDPOINT_URL,
    verify=False
)

print(f"[INFO] Connecting to Kafka via confluent-kafka (C-Optimized)...")

conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'message.max.bytes': 10485760,
    'linger.ms': 5,
    'batch.size': 1048576,
    'acks': 1
}
producer = Producer(conf)

producer_metrics = {
    "run_id": RUN_ID,
    "target_rate_mb_s": TARGET_RATE_MB_S,
    "num_partitions": KAFKA_NUM_PARTITIONS,
    "per_file": [],
    "total_bytes_sent": 0,
    "total_scans_sent": 0,
    "pipeline_start_time": time.time(),
}

msg_counter = 0

for file_index in range(31):
    file_id = f"duck_{file_index:05d}"
    key_i = f"duck_i_{file_index:05d}.dat"
    key_q = f"duck_q_{file_index:05d}.dat"

    print(f"\n[INFO] Fetching {file_id} directly from Cloud S3 bucket '{BUCKET_NAME}'...")

    try:
        t_s3_start = time.perf_counter()
        i_obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=key_i)
        q_obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=key_q)
        raw_bytes_i = i_obj['Body'].read()
        raw_bytes_q = q_obj['Body'].read()
        t_s3_end = time.perf_counter()

        data_i = np.frombuffer(raw_bytes_i, dtype=np.float32)
        data_q = np.frombuffer(raw_bytes_q, dtype=np.float32)

        min_len = min(len(data_i), len(data_q))
        data_i = data_i[:min_len]
        data_q = data_q[:min_len]

        chunk_size = 65536
        total_scans = len(data_i) // chunk_size

        print(f"[METRIC] S3 Download & Parse took: {t_s3_end - t_s3_start:.4f} seconds")
        print(f"[INFO] Total scans to transmit: {total_scans}. Pacing stream at {TARGET_RATE_MB_S} MB/s "
              f"({KAFKA_NUM_PARTITIONS} partition(s))...")

        t_kafka_start = time.perf_counter()
        bytes_sent_file = 0

        for scan_id in range(1, total_scans + 1):
            start_idx = (scan_id - 1) * chunk_size
            end_idx = start_idx + chunk_size

            chunk_i = data_i[start_idx:end_idx]
            chunk_q = data_q[start_idx:end_idx]

            header = {
                "file_id": file_id,
                "scan_id": scan_id,
                "total_scans": total_scans,
                "n_samples": chunk_size,
                "send_time": time.time()
            }

            header_json = json.dumps(header).encode('utf-8')
            header_len = struct.pack('>I', len(header_json))
            payload = header_len + header_json + chunk_i.tobytes() + chunk_q.tobytes()

            target_partition = msg_counter % KAFKA_NUM_PARTITIONS
            producer.produce(TOPIC, value=payload, partition=target_partition)
            producer.poll(0)
            msg_counter += 1

            bytes_sent_file += len(payload)
            expected_time = bytes_sent_file / (TARGET_RATE_MB_S * 1024 * 1024)
            elapsed_time = time.perf_counter() - t_kafka_start
            if elapsed_time < expected_time:
                time.sleep(expected_time - elapsed_time)

            if scan_id % max(1, (total_scans // 10)) == 0:
                print(f"  -> Queued {scan_id / total_scans * 100:.1f}% inside producer buffer...")

        print(f"[INFO] Flushing C-ring buffer directly to VM2 over the network...")
        producer.flush()

        t_kafka_end = time.perf_counter()
        kafka_time = t_kafka_end - t_kafka_start
        print(f"[METRIC] Kafka Network Streaming took: {kafka_time:.4f} seconds")
        print(f"[SUCCESS] {file_id} fully written to broker disk pipeline.")

        producer_metrics["per_file"].append({
            "file_id": file_id,
            "s3_download_time_s": t_s3_end - t_s3_start,
            "kafka_streaming_time_s": kafka_time,
            "bytes_sent": bytes_sent_file,
            "scans_sent": total_scans,
            "achieved_rate_mb_s": (bytes_sent_file / (1024 * 1024)) / kafka_time if kafka_time > 0 else None
        })
        producer_metrics["total_bytes_sent"] += bytes_sent_file
        producer_metrics["total_scans_sent"] += total_scans

    except Exception as e:
        print(f"[ERROR] Failed to execute lifecycle loop for {file_id}: {type(e).__name__}: {e}")

    finally:
        if 'raw_bytes_i' in locals(): del raw_bytes_i
        if 'raw_bytes_q' in locals(): del raw_bytes_q
        if 'data_i' in locals(): del data_i
        if 'data_q' in locals(): del data_q
        gc.collect()

print(f"\n[INFO] Broadcasting END_OF_STREAM marker to {KAFKA_NUM_PARTITIONS} partition(s)...")
for p in range(KAFKA_NUM_PARTITIONS):
    end_header = {"event": "END_OF_STREAM", "run_id": RUN_ID, "send_time": time.time()}
    end_header_json = json.dumps(end_header).encode('utf-8')
    end_payload = struct.pack('>I', len(end_header_json)) + end_header_json
    producer.produce(TOPIC, value=end_payload, partition=p)
producer.flush()

producer_metrics["pipeline_end_time"] = time.time()
producer_metrics["wall_clock_duration_s"] = producer_metrics["pipeline_end_time"] - producer_metrics["pipeline_start_time"]
producer_metrics["achieved_avg_rate_mb_s"] = (
    (producer_metrics["total_bytes_sent"] / (1024 * 1024)) / producer_metrics["wall_clock_duration_s"]
    if producer_metrics["wall_clock_duration_s"] > 0 else None
)

metrics_filename = f"producer_metrics_{RUN_ID}.pkl"
with open(metrics_filename, "wb") as f:
    pickle.dump(producer_metrics, f)

print(f"\n[METRICS] Producer metrics exported to {metrics_filename}")
print(json.dumps({k: v for k, v in producer_metrics.items() if k != 'per_file'}, indent=2))
print("\n[INFO] All files transmitted. Producer shutting down.")