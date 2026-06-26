import json
import sys
import struct  
import numpy as np
from confluent_kafka import Consumer, Producer
from distributed import Client

# ==========================================
# 1. CONFIGURATION & CLUSTER PARAMETERS
# ==========================================
DASK_SCHEDULER = 'tcp://10.67.22.72:8786'      # VM3 (Self)
KAFKA_BROKER = '10.67.22.134:9092'             # VM2
TOPIC_INPUT = 'topic_stream'                   
TOPIC_RESULTS = 'topic_results'                
SAMPLE_RATE = 20e6                             


# ==========================================
# 2. REMOTE WORKER COMPUTATION TASK
# ==========================================
def compute_fft(chunk_i, chunk_q):
    """
    This function executes remotely inside the RAM of VM4 and VM5.
    It takes lists of numbers, builds complex numbers, and computes the power spectrum.
    """
    import numpy as np  # Required context binding for distributed serialization
    
    # Reconstruct complex signal vector: V = I + j*Q
    complex_signal = np.array(chunk_i) + 1j * np.array(chunk_q)
    
    # Run the raw Fast Fourier Transform
    fft_output = np.fft.fft(complex_signal)
    
    # Calculate Power Intensity Magnitude Squared
    power_spectrum = np.abs(fft_output) ** 2
    
    return power_spectrum.tolist()


# ==========================================
# 3. CORE ORCHESTRATION PIPELINE
# ==========================================
def main():
    # --- Connect to Dask Compute Cluster ---
    print(f"[INFO] Connecting to Dask Scheduler at {DASK_SCHEDULER}...")
    try:
        client = Client(DASK_SCHEDULER)
        workers_count = len(client.scheduler_info()['workers'])
        print(f"[SUCCESS] Connected! Active worker nodes in cluster: {workers_count}")
    except Exception as e:
        print(f"[ERROR] Dask Master initialization failed: {e}")
        sys.exit(1)

    # --- Initialize Resilient Kafka Consumer ---
    consumer = Consumer({
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': 'dask-processor-v4',       
        'auto.offset.reset': 'earliest',       
        'fetch.min.bytes': 1048576,            
        'message.max.bytes': 50000000          
    })
    consumer.subscribe([TOPIC_INPUT])

    # --- Initialize Kafka Results Producer ---
    producer = Producer({
        'bootstrap.servers': KAFKA_BROKER,
        'linger.ms': 10,                       
        'message.max.bytes': 50000000
    })

    # --- Delivery Callback to Catch Silent Errors ---
    def delivery_report(err, msg):
        if err is not None:
            print(f"[ERROR] Message delivery failed: {err}")

    print(f"[INFO] Connecting to Kafka Broker at {KAFKA_BROKER}...")
    print("[INFO] Pipeline listening for incoming data stream...")

    # --- Pipeline State Tracking ---
    futures = []
    global_average = None
    total_processed = 0

    try:
        while True:
            # 1. Ingest raw custom binary frames from Kafka buffer
            msg = consumer.poll(0.1)
            
            if msg is not None and not msg.error():
                try:
                    payload_bytes = msg.value()
                    
                    # Read the first 4 bytes to find out how long the JSON header is
                    header_len = struct.unpack('>I', payload_bytes[:4])[0]
                    
                    # Extract the JSON header bytes and decode them to text
                    header_json_bytes = payload_bytes[4 : 4 + header_len]
                    header_data = json.loads(header_json_bytes.decode('utf-8'))
                    
                    # Read sample sizes to calculate precise byte offsets
                    n_samples = header_data["n_samples"]
                    float_bytes_len = n_samples * 4  # float32 = 4 bytes
                    
                    # Compute slicing index coordinates matching producer's layout
                    start_i = 4 + header_len
                    end_i = start_i + float_bytes_len
                    start_q = end_i
                    end_q = start_q + float_bytes_len
                    
                    # Extract raw slices
                    bytes_i = payload_bytes[start_i:end_i]
                    bytes_q = payload_bytes[start_q:end_q]
                    
                    # Convert raw bytes back into mathematical numeric vectors
                    chunk_i = np.frombuffer(bytes_i, dtype=np.float32).tolist()
                    chunk_q = np.frombuffer(bytes_q, dtype=np.float32).tolist()
                    
                    # 2. Deploy compiled task bytecode to available Dask cluster workers
                    future = client.submit(compute_fft, chunk_i, chunk_q)
                    futures.append(future)
                    
                except Exception as e:
                    print(f"[WARNING] Bypassed an unparseable frame packet: {e}")
                    continue

            # 3. Non-blocking asynchronous harvest of completed tasks
            done_futures = [f for f in futures if f.done()]
            
            for future in done_futures:
                futures.remove(future)  
                
                # Retrieve the processing matrix from the worker node RAM
                psd_result = np.array(future.result())
                
                # Instantiate accumulator dynamically on frame 1 structure matching
                if global_average is None:
                    global_average = np.zeros_like(psd_result)
                
                # 4. Numerically stable Iterative Moving Average calculation
                total_processed += 1
                global_average = global_average + (psd_result - global_average) / total_processed
                
                # 5. Periodically compile, center, downsample, and stream data
                if total_processed % 10 == 0:
                    # Apply standard DSP shift to align the 0 Hz DC offset dead-center
                    shifted_avg = np.fft.fftshift(global_average)
                    
                    # Generate symmetric frequency scaling bounds
                    freqs = np.fft.fftfreq(len(global_average), d=1/SAMPLE_RATE)
                    shifted_freqs = np.fft.fftshift(freqs)
                    
                    # --- THE DOWNSAMPLING FIX ---
                    # Compress the array by a factor of 10 to fit inside Kafka's 1MB limit
                    decimated_freqs = shifted_freqs[::10]
                    decimated_power = shifted_avg[::10]
                    
                    # Package structured array vectors and calculated metadata metrics
                    spectrum_payload = {
                        "frequencies": decimated_freqs.tolist(),
                        "power": decimated_power.tolist(),
                        "total_chunks": total_processed,
                        "max_power_intensity": float(np.max(shifted_avg)),
                        "mean_power_intensity": float(np.mean(shifted_avg))
                    }
                    
                    # Push output payload down the results pipeline channel
                    producer.produce(
                        TOPIC_RESULTS, 
                        value=json.dumps(spectrum_payload).encode('utf-8'),
                        callback=delivery_report
                    )
                    producer.poll(0)  
                    
                    print(f"[UPDATE] Successfully aggregated {total_processed} chunks. Centered spectrum broadcasted.")

    except KeyboardInterrupt:
        print("\n[INFO] Intercepted termination signal. Powering down pipeline components...")
    finally:
        # --- Clean Infrastructure Disconnections ---
        consumer.close()
        producer.flush()
        client.close()
        print("[SUCCESS] Processing orchestrator closed cleanly.")

if __name__ == "__main__":
    main()
