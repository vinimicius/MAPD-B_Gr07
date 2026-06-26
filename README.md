# QUAX Real-Time FFT Distributed Pipeline

This repository contains the bare-metal distributed processing pipeline for the QUAX experiment. It pulls raw I/Q signal data from CloudVeneto S3, streams it through Apache Kafka, distributes Fast Fourier Transform (FFT) computations across a Dask cluster, and visualizes the zero-centered Power Spectral Density in real-time.

## Architecture

* **`producer.py` (VM1):** Pulls raw `float32` binaries from S3, packages them into a high-performance C-optimized binary envelope, and paces the stream into Kafka.
* **`processing.py` (VM3):** The Dask orchestrator. Reads the binary firehose from Kafka, unpacks it, deploys the FFT math to worker nodes, computes an iterative moving average, downsamples the zero-centered spectrum to fit network limits, and pushes the JSON results back to Kafka.
* **`dashboard.py` (VM1):** A Streamlit web application that consumes the processed JSON arrays and visualizes the live zero-centered frequency spectrum.

## Installation Requirements

Ensure Python 3.10+ is installed on your machines. Install the dependencies via:

```bash
pip install -r requirements.txt
```

---

## Infrastructure Master Boot Sequence

Because of how the streaming data flows, the infrastructure must be turned on in a specific order: Listeners (Kafka/Dask) -> Workers -> Processor -> Producer.

### Step 1: Start the Kafka Data Pipe (VM2)
Kafka must be online first so the other microservices have a broker to connect to. Log into VM2 and start the KRaft broker:

```bash
cd ~/kafka_2.13-3.7.0
bin/kafka-server-start.sh -daemon config/kraft/server.properties
```

### Step 2: Start the Central Dask Brain (VM3)
Bring the Dask Scheduler online to manage the distributed compute cluster. Log into VM3 and start the scheduler:

```bash
python3 -m distributed.cli.dask_scheduler --host 0.0.0.0 --port 8786
```

### Step 3: Attach the Compute Muscle (VM4, VM5)
Wake up the physical compute nodes and attach them to the brain on VM3. Log into each worker VM and run:

```bash
python3 -m distributed.cli.dask_worker tcp://10.67.22.72:8786 --name worker-node
```

### Step 4: Launch the Web Dashboard (VM1)
Get the visualizer running so it is ready to catch the processed data. Log into VM1 and start Streamlit:

```bash
streamlit run dashboard.py --server.port 8501
```

To view this on your local machine, open an SSH tunnel from your local terminal:

```bash
ssh -J <jump_host_user>@gate.cloudveneto.it -i <your_key.pem> -L 8501:localhost:8501 ubuntu@10.67.22.42
```
Open `http://localhost:8501` in your browser.

### Step 5: Start the Processing Orchestrator (VM3)
Now that the cluster is ready and Kafka is running, start the processor. It will connect to the Dask workers and patiently wait for data. Open a new terminal on VM3 and run:

```bash
python3 -u processing.py
```

### Step 6: Unleash the Firehose (VM1)
The dashboard is waiting, the workers are idling, and the processor is listening. Time to push the raw S3 data into the pipeline. Open a new terminal on VM1, export your S3 credentials, and launch the producer:

```bash
export S3_ACCESS_KEY="<your_access_key>"
export S3_SECRET_KEY="<your_secret_key>"
export S3_ENDPOINT_URL="[https://cloud-areapd.pd.infn.it:5210](https://cloud-areapd.pd.infn.it:5210)"
export S3_BUCKET_NAME="quax"

python3 -u producer.py
```

---

## Maintenance: Resetting Kafka Memory
If you need to clear old data drops and completely factory-reset the Kafka broker, stop all running Python scripts and execute this sequence on VM2:

```bash
# 1. Stop the broker
cd ~/kafka_2.13-3.7.0
bin/kafka-server-stop.sh

# 2. Nuke the old data directories
rm -rf /tmp/kafka-logs
rm -rf /tmp/kraft-combined-logs

# 3. Format a brand new KRaft cluster ID
KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
bin/kafka-storage.sh format -t $KAFKA_CLUSTER_ID -c config/kraft/server.properties

# 4. Restart the clean broker
bin/kafka-server-start.sh -daemon config/kraft/server.properties
```
