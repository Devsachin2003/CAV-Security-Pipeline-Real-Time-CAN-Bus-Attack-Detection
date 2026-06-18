# CAV Security Pipeline Runbook

This runbook explains how to start, verify, test, and troubleshoot the CAV cybersecurity monitoring pipeline.

## Phase 2 Scope

This guide covers:

- Python environment setup.
- Docker infrastructure startup.
- Kafka, Elasticsearch, and Kibana verification.
- Detector and simulator execution order.
- Expected terminal output.
- Manual Kibana validation.
- Common failures and fixes.

## Prerequisites

Install these before running the project:

| Requirement | Purpose |
| --- | --- |
| Docker Desktop | Runs Zookeeper, Kafka, Elasticsearch, and Kibana. |
| Python 3.10+ | Runs the simulator and detector. The project has been exercised locally with Python 3.14. |
| `pip` | Installs Python dependencies. |
| Terminal | Run simulator, detector, Docker, and curl commands. |
| Browser | Open Kibana and Elasticsearch URLs. |

Required local ports:

| Port | Service |
| --- | --- |
| `2181` | Zookeeper |
| `9092` | Kafka |
| `9200` | Elasticsearch |
| `5601` | Kibana |

No `.env` file is required for the current local setup. Runtime configuration is passed through CLI flags and Docker Compose environment variables.

## Start From The Project Directory

From your current workspace:

```bash
cd "/Users/dev_sachin.sg/Documents/Personal projects/BART inspired Error detection /cav-security-pipeline"
```

## Python Environment

Create a virtual environment if one does not already exist:

```bash
python3 -m venv .venv
```

Activate the project virtual environment:

```bash
source .venv/bin/activate
```

Your shell prompt should show:

```text
(.venv)
```

Install dependencies:

```bash
pip install -r requirements.txt
```

Check the Python version:

```bash
python --version
```

Check installed packages:

```bash
pip show kafka-python-ng scikit-learn elasticsearch numpy pandas
```

Important: activate `.venv` inside `cav-security-pipeline`. Avoid accidentally activating a parent-folder environment such as:

```text
/Users/dev_sachin.sg/Documents/Personal projects/BART inspired Error detection /venv/bin/activate
```

## Startup Order

Use this order for reliable runs:

1. Start Docker Desktop manually.
2. Start the Docker Compose infrastructure.
3. Verify Kafka, Elasticsearch, and Kibana health.
4. Start the detector first.
5. Start normal simulator traffic to warm up the model.
6. Start attack simulator traffic.
7. Verify indexed records in Elasticsearch.
8. Explore results in Kibana.

## Start Infrastructure

Start all Docker services:

```bash
docker compose up -d
```

Check service status:

```bash
docker compose ps
```

Expected services:

```text
cav-zookeeper
cav-kafka
cav-elasticsearch
cav-kibana
```

If a service is still starting, wait 30 to 60 seconds and run:

```bash
docker compose ps
```

## Verify Elasticsearch

Check Elasticsearch is reachable:

```bash
curl http://localhost:9200
```

Expected result includes:

```text
"cluster_name" : "cav-security-cluster"
```

Check cluster health:

```bash
curl "http://localhost:9200/_cluster/health?pretty"
```

Expected result:

```text
"status" : "green"
```

`yellow` can be acceptable for a single-node local cluster, but `green` is the clean target.

## Verify Kibana

Open:

```text
http://localhost:5601
```

Kibana may take a minute after Elasticsearch becomes healthy.

## Verify Kafka

List Kafka topics:

```bash
docker exec cav-kafka kafka-topics --bootstrap-server localhost:9092 --list
```

Expected topic:

```text
can-telematics
```

Describe the topic:

```bash
docker exec cav-kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic can-telematics
```

Expected result includes partitions and leader information.

## Start The Detector

Open a terminal in the project directory and activate the virtual environment:

```bash
cd "/Users/dev_sachin.sg/Documents/Personal projects/BART inspired Error detection /cav-security-pipeline"
source .venv/bin/activate
```

Start the detector:

```bash
python detection/ml_detector.py --warmup-samples 1000 --consumer-group cav-security-detector-live --auto-offset-reset latest
```

Expected detector output:

```text
Connected to Kafka at localhost:9092
Connected to Elasticsearch at http://localhost:9200
Ensured Elasticsearch index can-security-alerts
Starting detector topic=can-telematics index=can-security-alerts
```

The detector should keep running.

## Warm Up With Normal Traffic

Open a second terminal in the project directory:

```bash
cd "/Users/dev_sachin.sg/Documents/Personal projects/BART inspired Error detection /cav-security-pipeline"
source .venv/bin/activate
```

Start normal telemetry:

```bash
python simulator/can_simulator.py --attack-mode normal
```

Expected simulator output:

```text
Connected to Kafka at localhost:9092
Starting CAN simulator vehicle_id=... topic=can-telematics mode=normal rate=100.0Hz
```

After the detector receives enough normal samples, expected detector output includes:

```text
Fitting IsolationForest with 1000 normal samples
Model fitted. Detector entering inference phase.
```

Keep the normal simulator running while testing attacks if you want mixed traffic. Stop it with `Ctrl+C` if you want only attack-mode traffic.

## Run Attack Traffic

Open another terminal or stop the normal simulator and reuse that terminal.

Injection attack:

```bash
python simulator/can_simulator.py --attack-mode injection
```

Fuzzing attack:

```bash
python simulator/can_simulator.py --attack-mode fuzz
```

Replay attack:

```bash
python simulator/can_simulator.py --attack-mode replay
```

DoS flood:

```bash
python simulator/can_simulator.py --attack-mode dos
```

For quick manual tests, run each mode for 15 to 30 seconds and stop with `Ctrl+C`.

## Verify Data In Elasticsearch

Count documents:

```bash
curl "http://localhost:9200/can-security-alerts/_count?pretty"
```

Count only injection records:

```bash
curl -X POST "http://localhost:9200/can-security-alerts/_count?pretty" -H "Content-Type: application/json" -d '{"query":{"term":{"attack_type_label":"injection"}}}'
```

Count anomaly records:

```bash
curl -X POST "http://localhost:9200/can-security-alerts/_count?pretty" -H "Content-Type: application/json" -d '{"query":{"term":{"is_anomaly":-1}}}'
```

Aggregate records by attack label:

```bash
curl -X POST "http://localhost:9200/can-security-alerts/_search?pretty&size=0" -H "Content-Type: application/json" -d '{"aggs":{"by_attack_type":{"terms":{"field":"attack_type_label","size":10}}}}'
```

Aggregate records by attack label and anomaly status:

```bash
curl -X POST "http://localhost:9200/can-security-alerts/_search?pretty&size=0" -H "Content-Type: application/json" -d '{"aggs":{"by_attack_type":{"terms":{"field":"attack_type_label","size":10},"aggs":{"by_anomaly":{"terms":{"field":"is_anomaly","size":2}}}}}}'
```

View the latest records:

```bash
curl -X POST "http://localhost:9200/can-security-alerts/_search?pretty&size=5" -H "Content-Type: application/json" -d '{"sort":[{"timestamp":{"order":"desc"}}]}'
```

## Set Up Kibana Data View

Open:

```text
http://localhost:5601
```

Then:

1. Open the top search box.
2. Search for `Data Views`.
3. Click `Create data view`.
4. Name: `can-security-alerts`.
5. Index pattern: `can-security-alerts*`.
6. Timestamp field: `timestamp`.
7. Save the data view.

## Explore In Kibana Discover

Open Discover and select the `can-security-alerts` data view.

Set the time picker to a range that includes your run. During local testing, use:

```text
Last 15 minutes
```

or:

```text
Last 1 hour
```

Use the large KQL search bar at the top of Discover, not the left field-name search box.

Useful KQL filters:

```text
attack_type_label: "normal"
```

```text
attack_type_label: "injection"
```

```text
is_anomaly: -1
```

```text
attack_type_label: "injection" and is_anomaly: -1
```

```text
detector_phase: "inference"
```

Useful columns to add:

```text
timestamp
attack_type_label
is_anomaly
anomaly_score
pipeline_latency_ms
arbitration_id
payload.speed
payload.brake_pressure
payload.rpm
features.bus_frequency_1s
```

## Create Basic Kibana Visualizations

Detection count over time:

1. Open Visualize Library.
2. Create Lens visualization.
3. Use `timestamp` on the horizontal axis.
4. Use document count on the vertical axis.
5. Break down by `is_anomaly`.

Attack type distribution:

1. Create a Lens visualization.
2. Use Top values of `attack_type_label`.
3. Use document count as metric.

Pipeline latency:

1. Create a Lens visualization.
2. Use average of `pipeline_latency_ms`.
3. Plot over `timestamp`.

Anomaly score:

1. Create a Lens visualization.
2. Use average or median of `anomaly_score`.
3. Break down by `attack_type_label`.

## Evaluation Metrics In Kibana

Because the simulator labels each record with `attack_type_label`, you can estimate detection quality.

Detection Rate for attacks:

```text
attack_type_label != "normal" and is_anomaly: -1
```

Divide that count by:

```text
attack_type_label != "normal"
```

False Positive Rate:

```text
attack_type_label: "normal" and is_anomaly: -1
```

Divide that count by:

```text
attack_type_label: "normal"
```

Pipeline Latency:

```text
Average of pipeline_latency_ms
```

Recommended dashboard panels:

| Panel | Field/Query |
| --- | --- |
| Total events | Count of all documents |
| Anomalies | `is_anomaly: -1` |
| Attack documents | `attack_type_label != "normal"` |
| False positives | `attack_type_label: "normal" and is_anomaly: -1` |
| Average latency | Average `pipeline_latency_ms` |
| Attack mix | Top values of `attack_type_label` |
| Anomaly trend | Count over time split by `is_anomaly` |

## Full Clean Test Sequence

Use this when you want a fresh live test without relying on old Kafka offsets.

Terminal 1:

```bash
cd "/Users/dev_sachin.sg/Documents/Personal projects/BART inspired Error detection /cav-security-pipeline"
source .venv/bin/activate
docker compose up -d
```

Terminal 2:

```bash
cd "/Users/dev_sachin.sg/Documents/Personal projects/BART inspired Error detection /cav-security-pipeline"
source .venv/bin/activate
python detection/ml_detector.py --warmup-samples 1000 --consumer-group cav-security-e2e-live --auto-offset-reset latest --index-name can-security-alerts-e2e
```

Terminal 3:

```bash
cd "/Users/dev_sachin.sg/Documents/Personal projects/BART inspired Error detection /cav-security-pipeline"
source .venv/bin/activate
python simulator/can_simulator.py --attack-mode normal
```

Wait until the detector enters inference phase. Then stop the normal simulator with `Ctrl+C`.

Run each attack for 15 to 30 seconds:

```bash
python simulator/can_simulator.py --attack-mode injection
```

```bash
python simulator/can_simulator.py --attack-mode fuzz
```

```bash
python simulator/can_simulator.py --attack-mode replay
```

```bash
python simulator/can_simulator.py --attack-mode dos
```

Check the e2e index:

```bash
curl "http://localhost:9200/can-security-alerts-e2e/_count?pretty"
```

Run repeatable evaluation metrics:

```bash
python evaluation/evaluate_detection.py --index "can-security-alerts-e2e*"
```

Expected outputs:

```text
reports/detection-evaluation.json
reports/detection-evaluation.csv
```

Create or select a Kibana data view:

```text
can-security-alerts-e2e*
```

## Common Issues And Fixes

### Docker Daemon Not Running

Symptom:

```text
Cannot connect to the Docker daemon
```

Fix:

Start Docker Desktop manually, wait until it says Docker is running, then run:

```bash
docker compose up -d
```

### Kafka Connection Refused

Symptom:

```text
NoBrokersAvailable
Connection refused localhost:9092
```

Fix:

Check Docker services:

```bash
docker compose ps
```

Check Kafka logs:

```bash
docker logs cav-kafka
```

Wait for Kafka to become healthy, then restart the simulator or detector.

### Elasticsearch Client Compatibility Error

Symptom:

```text
Accept version must be either version 8 or 7, but found 9
```

Fix:

Use the pinned Elasticsearch Python client from `requirements.txt`:

```bash
pip install -r requirements.txt
```

Confirm:

```bash
pip show elasticsearch
```

Expected major version:

```text
8.x
```

### Kibana Shows No Results

Common causes:

- The time picker does not include the run time.
- The wrong data view is selected.
- The query was typed into the left field search box instead of the main KQL bar.
- The detector is still warming up or not running.
- You are filtering for an attack mode that was not actually run long enough.

Fix:

1. Set time picker to `Last 1 hour`.
2. Clear the KQL query.
3. Click Refresh.
4. Confirm the selected data view is `can-security-alerts*`.
5. Run:

```bash
curl "http://localhost:9200/can-security-alerts/_count?pretty"
```

### Attack Records Do Not Appear

Fix:

Use a fresh consumer group and latest offsets:

```bash
python detection/ml_detector.py --warmup-samples 1000 --consumer-group cav-security-test-$(date +%s) --auto-offset-reset latest
```

Then run the simulator again:

```bash
python simulator/can_simulator.py --attack-mode injection
```

### Too Many Old Records In Kibana

Use a separate test index:

```bash
python detection/ml_detector.py --index-name can-security-alerts-e2e --consumer-group cav-security-e2e-live --auto-offset-reset latest
```

Then create a Kibana data view:

```text
can-security-alerts-e2e*
```

### Wrong Virtual Environment

Symptom:

The shell shows `(venv)` instead of `(.venv)`, or imports fail.

Fix:

```bash
deactivate
cd "/Users/dev_sachin.sg/Documents/Personal projects/BART inspired Error detection /cav-security-pipeline"
source .venv/bin/activate
pip install -r requirements.txt
```

### Existing Detector Or Simulator Processes

List active project processes:

```bash
ps aux | grep -E "can_simulator.py|ml_detector.py"
```

Stop a specific simulator mode:

```bash
pkill -f "simulator/can_simulator.py --attack-mode injection"
```

Stop all simulator and detector processes:

```bash
pkill -f "simulator/can_simulator.py|detection/ml_detector.py"
```

Use the broad stop command carefully because it stops every live pipeline process.

### Port Already In Use

Check what is listening:

```bash
lsof -i :9092
lsof -i :9200
lsof -i :5601
```

Stop the conflicting service or change ports in `docker-compose.yml`.

## Known Evaluation Note

The current Isolation Forest detector is intentionally simple and unsupervised. It is good for demonstrating the streaming architecture and many obvious anomalies, but it may under-detect some DoS-style traffic or over-flag normal traffic depending on warmup data and contamination settings. Tuning, supervised baselines, and attack-specific rules belong in the next hardening phase.
