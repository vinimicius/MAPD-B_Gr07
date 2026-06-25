# QUAX Live Data Monitoring & Distributed Processing

This repository contains the real-time processing and live monitoring pipeline for the QUAX experiment (QUaerere AXions). 

The goal of the project is to establish an online ETL pipeline that reads I/Q (In-phase and Quadrature) digitizer binary signals from cloud storage, performs parallel Fast Fourier Transforms (FFT) for noise reduction and axion peak identification, and serves the results on a dynamic live-updating dashboard.

## Architecture Overview

```
[Cloud Storage (S3)]
       |  (Public HTTP GET)
[Kafka Stream Emulator (producer.py)]
       |  (topic_stream)
[Distributed Dask Processor (processor.py)]
       |  (FFT & Averaging in parallel)
       |  (topic_results)
[Bokeh Dashboard Server (dashboard.py)]
       |  (Local Port Forwarding: 8888)
[Web Browser (Local Mac)]
```

## Repository Structure

*   `docker-compose.yml`: Launches Apache Zookeeper and a single-broker Apache Kafka instance.
*   `producer.py`: Emulator that pairs the I/Q raw `.dat` binary files and streams metadata to Kafka.
*   `processor.py`: Dask-based distributed processor that reads file pairs, calculates parallel FFTs ($2^{12} = 4096$ scans), averages power spectra, and computes standard deviations (RMS).
*   `dashboard.py`: Interactive web dashboard served via Bokeh to visualize the real-time power spectrum, noise envelope, and cumulative average.
*   `requirements.txt`: Python package dependencies.

## Setup & Deployment Instructions (VM Console)

### 1. Initialize Infrastructure
First, launch the Kafka and Zookeeper Docker containers in the background:
```bash
sudo docker compose up -d
```

Verify that both containers are running successfully:
```bash
sudo docker compose ps
```

### 2. Create Kafka Topics
Execute the following commands inside the Kafka container to initialize the input and output topics:
```bash
sudo docker exec -it kafka kafka-topics --create --topic topic_stream --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
sudo docker exec -it kafka kafka-topics --create --topic topic_results --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

### 3. Setup Virtual Environment
Create and activate a clean Python virtual environment, then install the dependencies:
```bash
sudo apt install -y python3.10-venv
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 4. Execute the Application Pipeline
Start all python components in the background (using unbuffered `-u` mode to allow immediate logging):

1.  **Dask Distributed Processor:**
    ```bash
    python3 -u processor.py > processor.log 2>&1 &
    ```
2.  **Bokeh Dashboard Web Server:**
    ```bash
    bokeh serve --port 8888 --allow-websocket-origin=* dashboard.py > dashboard.log 2>&1 &
    ```
3.  **Stream Emulator (Producer):**
    ```bash
    python3 -u producer.py > producer.log 2>&1 &
    ```

Monitor the logs to verify everything is running:
```bash
tail -f processor.log
tail -f producer.log
```

## Local Browser Access (Your Laptop)

To access the live-updating web interface from your local computer, open a **new terminal tab** on your local machine and establish the SSH port forwarding tunnel:

```bash
ssh -L 8888:10.67.22.230:8888 -J YOUR_CLOUDVENETO_USERNAME@gate.cloudveneto.it ubuntu@10.67.22.230 -i ~/Desktop/EylulCagla.pem
```

Now open your web browser and navigate to:
👉 **http://localhost:8888/dashboard**
