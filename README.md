# QUAX Real-Time FFT Distributed Pipeline & Benchmarking Suite

This repository contains the bare-metal distributed processing pipeline for the QUAX experiment. It pulls raw I/Q signal data from CloudVeneto S3, streams it through Apache Kafka, distributes Fast Fourier Transform (FFT) computations across a Dask cluster, and visualizes the zero-centered Power Spectral Density in real-time.

It also includes a comprehensive benchmarking suite that utilizes Little's Law and Welford's online algorithm to mathematically prove pipeline stability and calculate end-to-end latency and effective processing rates.

## Architecture & Node Layout

The infrastructure is distributed across 5 Virtual Machines on the CloudVeneto network.

* **VM1 (10.67.22.42) - The Ingestion & Visualization Node:** * Runs `producer.py`: Pulls raw `float32` binaries from S3, paces the stream (throttling), and partitions the payload into Kafka. Exports `producer_metrics_<run_id>.pkl`.
  * Runs `dashboard.py`: A Streamlit web application that consumes processed JSON arrays and visualizes the live frequency spectrum.
* **VM2 (10.67.22.134) - The Message Broker:** * Runs Apache Kafka (KRaft mode). Acts as the high-throughput buffer between the S3 producer and the Dask orchestrator.
* **VM3 (10.67.22.72) - The Brain & Worker:** * Runs the Dask Scheduler `dask_scheduler`.
  * Runs 1 local Dask Worker to boost compute capability.
  * Runs `processing.py`: The orchestrator that pulls from Kafka, distributes the FFT tasks, calculates global averages, downsamples data, pushes results to the dashboard, and exports `processing_metrics_<run_id>.pkl`.
* **VM4 (10.67.22.202) & VM5 (10.67.22.185) - The Compute Muscle:**
  * Dedicated Dask Worker nodes that receive NumPy arrays in RAM and execute the raw Fast Fourier Transforms.

## Installation Requirements

Ensure Python 3.10+ is installed on your machines. Install the dependencies via:
```bash
pip install -r requirements.txt
```

---

## Phase 1: Infrastructure Master Boot Sequence
*This sequence is run ONCE to turn the cluster on.*

Because of how the streaming data flows, the infrastructure must be turned on in a specific order: Listeners (Kafka/Dask) -> Workers -> Dashboard.

### Step 1: Start the Kafka Data Pipe (VM2)
Log into VM2 and start the KRaft broker:
```bash
cd ~/kafka_2.13-3.7.0
bin/kafka-server-start.sh -daemon config/kraft/server.properties
```

### Step 2: Start the Central Dask Brain (VM3)
Log into VM3 and start the scheduler:
```bash
python3 -m distributed.cli.dask_scheduler --host 0.0.0.0 --port 8786
```

### Step 3: Attach the Compute Muscle (VM3, VM4, VM5)
Wake up the physical compute nodes and attach them to the brain. 
Run this on **VM4** and **VM5**:
```bash
python3 -m distributed.cli.dask_worker tcp://10.67.22.72:8786 --name worker-node
```
Run this in a new terminal on **VM3** (to give the cluster 3 total workers for benchmarking):
```bash
python3 -m distributed.cli.dask_worker tcp://10.67.22.72:8786 --name worker-vm3
```

### Step 4: Launch the Web Dashboard (VM1)
Get the visualizer running so it is ready to catch the processed data. Log into VM1:
```bash
streamlit run dashboard.py --server.port 8501
```
To view this on your local machine, open an SSH tunnel from your local terminal:
```bash
ssh -J <jump_host_user>@gate.cloudveneto.it -i <your_key.pem> -L 8501:localhost:8501 ubuntu@10.67.22.42
```
Open `http://localhost:8501` in your browser.

---

## Phase 2: Running the Benchmark Trials
*This sequence is repeated for every benchmark trial.*

We run specific trials to test partition scaling and system saturation. 
**Note:** `rate_16mbps` and `partition_1part_rate16` are identical; you only need to run this configuration once.

| Trial Name | `RUN_ID` | `KAFKA_NUM_PARTITIONS` | `STREAM_RATE_MB_S` |
|---|---|---|---|
| Partitioning A | `partition_1part_rate16` | 1 | 16 |
| Partitioning B | `partition_3part_rate16` | 3 | 16 |
| Rate 1 | `rate_4mbps` | 1 | 4 |
| Rate 2 | `rate_16mbps` | 1 | 16 |
| Rate 3 | `rate_30mbps` | 1 | 30 |
| Rate 4 | `rate_60mbps` | 1 | 60 |

### Step A: Reset Kafka & Recreate Topics (VM2)
Before *every* trial, Kafka must be factory-reset to ensure zero buffer contamination.
Log into VM2 and run:
```bash
cd ~/kafka_2.13-3.7.0
bin/kafka-server-stop.sh
rm -rf /tmp/kafka-logs
rm -rf /tmp/kraft-combined-logs

KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
bin/kafka-storage.sh format -t $KAFKA_CLUSTER_ID -c config/kraft/server.properties
bin/kafka-server-start.sh -daemon config/kraft/server.properties
```
Wait ~5 seconds, then create the topics matching the `KAFKA_NUM_PARTITIONS` of your current trial:
```bash
bin/kafka-topics.sh --create --topic topic_stream --partitions <N_PARTITIONS_FOR_TRIAL> --replication-factor 1 --bootstrap-server 10.67.22.134:9092

bin/kafka-topics.sh --create --topic topic_results --partitions 1 --replication-factor 1 --bootstrap-server 10.67.22.134:9092
```

### Step B: Start the Orchestrator (VM3)
Log into VM3, set the run ID, and start the processing script. It will wait for the stream:
```bash
export RUN_ID=<run_id_of_trial>
python3 -u processing.py
```

### Step C: Unleash the Firehose (VM1)
Log into VM1, set your S3 credentials, set the trial parameters, and start the producer:
```bash
export S3_ACCESS_KEY="<your_access_key>"
export S3_SECRET_KEY="<your_secret_key>"
export S3_ENDPOINT_URL="[https://cloud-areapd.pd.infn.it:5210](https://cloud-areapd.pd.infn.it:5210)"
export S3_BUCKET_NAME="quax"

export STREAM_RATE_MB_S=<line_rate_for_trial>
export KAFKA_NUM_PARTITIONS=<partitions_for_trial>
export RUN_ID=<run_id_of_trial>

python3 -u producer.py
```

### Step D: Wait for Auto-Shutdown
Do not manually kill the scripts. Wait for `producer.py` to print `All files transmitted` and send the `END_OF_STREAM` marker. `processing.py` will automatically catch this marker, drain the Dask queue, export the `.pkl` files, and gracefully shut down.

---

## Phase 3: Data Harvesting

Once the trial finishes naturally, you must download the exported `.pkl` metric files to your local machine for analysis. 

From a terminal on your **local machine** (e.g., your Mac), run:

**Pulling from VM1 (Producer Metrics):**
```bash
scp -J <your_user>@gate.cloudveneto.it -i ~/<path_to_your_.pem> ubuntu@10.67.22.42:/home/ubuntu/producer_metrics_<run_id>.pkl ./
```

**Pulling from VM3 (Processing/Latency Metrics):**
```bash
scp -J <your_user>@gate.cloudveneto.it -i ~/<path_to_your_.pem> ubuntu@10.67.22.72:/home/ubuntu/processing_metrics_<run_id>.pkl ./
```

*Repeat Phase 2 and Phase 3 for all required trials in the table.*
