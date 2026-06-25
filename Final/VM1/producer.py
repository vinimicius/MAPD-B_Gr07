import os
import time
import json
import struct
import boto3
import numpy as np
import gc
import urllib3
from confluent_kafka import Producer  # <-- THE C-COMPILED LIBRARY

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

ACCESS_KEY = os.environ.get('S3_ACCESS_KEY')
SECRET_KEY = os.environ.get('S3_SECRET_KEY')
ENDPOINT_URL = os.environ.get('S3_ENDPOINT_URL')
BUCKET_NAME = os.environ.get('S3_BUCKET_NAME')
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', '10.67.22.212:9092')
TOPIC = 'topic_stream'

print(f"[INFO] Connecting to CloudVeneto S3 at {ENDPOINT_URL}...")
s3_client = boto3.client(
    's3',
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    endpoint_url=ENDPOINT_URL,
    verify=False
)

print(f"[INFO] Connecting to Kafka via confluent-kafka (C-Optimized)...")
# Confluent-Kafka uses a dictionary for configuration
conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'message.max.bytes': 10485760,  # 10MB max message
    'linger.ms': 5,
    'batch.size': 1048576,          # 1MB batches
    'acks': 1
}
producer = Producer(conf)

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

        chunk_size = 65536  # 512KB chunks
        total_scans = len(data_i) // chunk_size

        print(f"[METRIC] S3 Download & Parse took: {t_s3_end - t_s3_start:.4f} seconds")
        print(f"[INFO] Total scans to transmit: {total_scans}. Starting high-speed C stream...")

        t_kafka_start = time.perf_counter()
        
        for scan_id in range(1, total_scans + 1):
            start_idx = (scan_id - 1) * chunk_size
            end_idx = start_idx + chunk_size

            chunk_i = data_i[start_idx:end_idx]
            chunk_q = data_q[start_idx:end_idx]

            header = {
                "file_id": file_id,
                "scan_id": scan_id,
                "total_scans": total_scans,
                "n_samples": chunk_size
            }

            header_json = json.dumps(header).encode('utf-8')
            header_len = struct.pack('>I', len(header_json))
            payload = header_len + header_json + chunk_i.tobytes() + chunk_q.tobytes()

            # The C-library produce method
            producer.produce(TOPIC, value=payload)
            producer.poll(0)  # Quickly process background C-thread events

            if scan_id % max(1, (total_scans // 10)) == 0:
                print(f"  -> Queued {scan_id / total_scans * 100:.1f}% inside producer buffer...")

        print(f"[INFO] Flushing C-ring buffer directly to VM2 over the network...")
        producer.flush()
        
        t_kafka_end = time.perf_counter()
        
        print(f"[METRIC] Kafka Network Streaming took: {t_kafka_end - t_kafka_start:.4f} seconds")
        print(f"[SUCCESS] {file_id} fully written to broker disk pipeline.")

    except Exception as e:
        print(f"[ERROR] Failed to execute lifecycle loop for {file_id}: {type(e).__name__}: {e}")

    finally:
        if 'raw_bytes_i' in locals(): del raw_bytes_i
        if 'raw_bytes_q' in locals(): del raw_bytes_q
        if 'data_i' in locals(): del data_i
        if 'data_q' in locals(): del data_q
        gc.collect()

print("\n[INFO] All files transmitted. Producer shutting down.")
