# Step-by-step how to run the 6 trials

## General logic

1. **`RUN_ID`** (env var) — identifies each trial
2. **`send_time`** each chunk's header — allows to measure latency end-to-end (producer → Kafka → Dask → resultado) not only throughput
3. **Karker `END_OF_STREAM`** — producer shoots message explicitly when finished, and `processing.py` only closes and exports the results dictionary when receives that sign in all partitions and Dasks future line is empty.

About **partitioning**: since it's running in **an only `processing.py`**, the `global_average`/`total_processed` it's still a unique acumulator. What changes is how `producer.py` spreads the chunks between partitions (round-robin explicit).


## Changes in `processing.py`

For the latency statistics, I used the same principle as the incremental merge you already use (Welford / Chan–Golub–LeVeque), but for the variance:

$$\mu_n = \mu_{n-1} + \frac{x_n - \mu_{n-1}}{n}, \qquad M_{2,n} = M_{2,n-1} + (x_n-\mu_{n-1})(x_n-\mu_n), \qquad \sigma_n^2 = \frac{M_{2,n}}{n}$$

This avoids the numerical error that occurs when summing $\sum x^2$ directly (which can result in a loss of precision when the values are small and $n$ is large).


## What each snapshot measures (and why)

The stability to look for visually in the `snapshots[“consumer_lag”]` over time:

$$\frac{dL}{dt} = \lambda(t) - \mu_{\text{eff}}(t) \quad\Longrightarrow\quad
\begin{cases} \lambda < \mu_{\text{eff}} & L(t) \text{ oscillates close to a finite value (estable)} \\ \lambda \geq \mu_{\text{eff}} & L(t) \text{ grows without limit (saturation)} \end{cases}$$

And Little's Law relates the average lag $\bar L$ to the average latency $\bar W$ (which you're measuring via `latency_stats`):

$$\bar L = \lambda \, \bar W$$

(Proof intuition: by summing the total time each item spends in the system, $\sum_i W_i$, and dividing by a large interval $T$: $\frac{1}{T}\int_0^T L(t)\, dt \to \bar L$ and $\frac{1}{T}\sum_i W_i = \frac{N(T)}{T}\cdot\frac{\sum_i W_i}{N(T)} \to \lambda \bar W$, so both sides converge to the same quantity.)

This then gives a direct way to **calculate** $\mu_{\text{eff}}$ from the collected data, rather than just visualizing it.

## Execution Script

### Step 0 — base Infraestructure

Follow until step 3 from README file

Plus activate in the VM3, in another terminal afther the scheduler is working:

```bash
python3 -m distributed.cli.dask_worker tcp://10.67.22.72:8786 --name worker-vm3
```
This gives 3 workers instead of 2

### Trials table

| Trial | `RUN_ID` | Partições `topic_stream` | `KAFKA_NUM_PARTITIONS` | `STREAM_RATE_MB_S` |
|---|---|---|---|---|
| Partitioning A | `partition_1part_rate16` | 1 | 1 | 16 |
| Partitioning B | `partition_3part_rate16` | 3 | 3 | 16 |
| Rate 1 | `rate_4mbps` | 1 | 1 | 4 |
| Rate 2 | `rate_16mbps` | 1 | 1 | 16 |
| Rate 3 | `rate_30mbps` | 1 | 1 | 30 |
| Rate 4 | `rate_60mbps` | 1 | 1 | 60 |

`rate_16mbps` e `partition_1part_rate16` are the same trial — use the same result

### Before EACH trial: reset Kafka + recriate topics (run in VM2)

```bash
cd ~/kafka_2.13-3.7.0
bin/kafka-server-stop.sh
rm -rf /tmp/kafka-logs
rm -rf /tmp/kraft-combined-logs
KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
bin/kafka-storage.sh format -t $KAFKA_CLUSTER_ID -c config/kraft/server.properties
bin/kafka-server-start.sh -daemon config/kraft/server.properties

# wait ~5s for broker, then recriate topics with the number of partitions:
bin/kafka-topics.sh --create --topic topic_stream --partitions <N_PARTITIONS> \
  --replication-factor 1 --bootstrap-server 10.67.22.134:9092
bin/kafka-topics.sh --create --topic topic_results --partitions 1 \
  --replication-factor 1 --bootstrap-server 10.67.22.134:9092
```

### For each trial:

```bash
# VM3 — processing.py
export RUN_ID=<run_id_of_trial>
python3 -u processing.py
```

```bash
# VM1 — producer.py (em outro terminal)
export S3_ACCESS_KEY="<your_access_key>"
export S3_SECRET_KEY="<your_secret_key>"
export S3_ENDPOINT_URL="https://cloud-areapd.pd.infn.it:5210"
export S3_BUCKET_NAME="quax"
export STREAM_RATE_MB_S=<line_rate>
export KAFKA_NUM_PARTITIONS=<line_partitions>
export RUN_ID=<run_id_of_trial>
python3 -u producer.py
```

Wait for producer to print: `All files transmitted` and `processing.py` print `[METRICS] Processing metrics exported...` and close **on its own**(`END_OF_STREAM`). 
This will generate the following files:

- VM1: `producer_metrics_<run_id>.pkl`
- VM3: `processing_metrics_<run_id>.pkl`

To send to your local machine, run the following:
```bash
scp -J <your_user>@gate.cloudveneto.it -i ~/<path_to_your_.pem> ubuntu@10.67.22.42:/home/ubuntu/quax-pipeline/producer_metrics_partition_3part_rate16.pkl  /home/ubuntu/MAPD-B_Gr07/
scp -J <your_user>@gate.cloudveneto.it -i ~/<path_to_your_.pem> ubuntu@10.67.22.72:/home/ubuntu/processing_metrics_partition_3part_rate16.pkl 
/home/ubuntu/MAPD-B_Gr07/
```
Repeat Kafka reset + plus the commands for next trial.


