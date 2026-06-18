# CAV Cybersecurity Pipeline Learning Guide

This guide teaches the project from beginner concepts through implementation and demo talking points.

## 1. What This Project Does

The project simulates a Connected Autonomous Vehicle security monitoring stack:

```text
CAN Simulator -> Kafka -> ML Detector -> Elasticsearch -> Kibana
```

The simulator emits vehicle telemetry and labeled attack traffic. Kafka carries the stream. The detector learns normal behavior, scores new messages with Isolation Forest, and indexes enriched events. Kibana is used to inspect traffic, anomalies, attack labels, and latency.

## 2. CAN Bus Basics

CAN bus is a message-based network used by vehicle electronic control units. Instead of each ECU wiring directly to every other ECU, they publish compact frames onto a shared bus.

Important ideas:

- `arbitration_id`: Message identifier and priority signal. Lower IDs win arbitration and get bus access first.
- `payload`: Up to 8 bytes in classic CAN. In this simulator, the payload is decoded into readable fields such as `rpm`, `speed`, and `brake_pressure`.
- `DLC`: Data Length Code, the payload length.
- Broadcast model: Nodes generally listen to messages they care about.
- No built-in authentication in classic CAN: A compromised node can often send plausible-looking messages.

## 3. Automotive Cybersecurity

Automotive cybersecurity protects vehicle networks, ECUs, sensors, actuators, cloud services, and update systems from abuse. In the in-vehicle CAN context, attackers may try to:

- Inject fake safety-critical messages.
- Replay previously valid traffic.
- Fuzz message IDs and payloads to trigger faults.
- Flood the bus so legitimate ECUs cannot communicate.

This project focuses on detecting abnormal CAN stream behavior, not preventing it at the bus layer.

## 4. Attack Types

### DoS Attack On CAN

A CAN DoS attack floods the bus, often using a low arbitration ID such as `0x000`. Because lower IDs have higher priority, repeated dominant frames can delay or starve legitimate traffic.

Detection clues:

- Very high bus frequency.
- One ID dominates the one-second window.
- Low ID entropy.
- Very low interarrival time.
- Repeated payloads.
- Reserved or unusual high-priority IDs.

### Replay Attack

A replay attack records legitimate traffic and sends it again later. Payload values may look normal, so detection often needs timing, sequence, and context features.

Detection clues:

- Repeated sequences.
- Timing irregularity.
- Payloads that are valid individually but stale in context.

### Fuzzing Attack

Fuzzing sends randomized IDs and payloads. It is often easy for anomaly detection because values are scattered far outside normal driving ranges.

Detection clues:

- Unknown arbitration IDs.
- Random payload distributions.
- Extreme numeric values.
- High raw byte variance.

### Injection Attack

Injection targets specific IDs with malicious values, such as a sudden speed of `250 km/h` or brake pressure of `0` while moving.

Detection clues:

- Safety-critical field spikes.
- Physically inconsistent values.
- Abnormal speed/brake/RPM combinations.

## 5. Why Kafka

Kafka is used because vehicle telemetry and security monitoring are naturally streaming problems.

Kafka provides:

- Durable event ingestion.
- Topic-based decoupling between producers and consumers.
- Consumer groups for scalable processing.
- Replayable data for testing.
- Backpressure tolerance when downstream systems slow down.

In this repo, the topic is:

```text
can-telematics
```

## 6. Why Elasticsearch

Elasticsearch is used because security operations need fast search and aggregation over event records.

It supports:

- Time-series exploration.
- Fast filtering by attack type, vehicle ID, anomaly flag, and ID.
- Aggregations for detection metrics and dashboards.
- Kibana visualization.

The default index is:

```text
can-security-alerts
```

## 7. Why Isolation Forest

Isolation Forest is an unsupervised anomaly detection model. It works by building random trees that isolate points. Unusual points are often isolated with fewer splits than normal points.

Why it fits this prototype:

- It can train on normal traffic without attack labels.
- It supports real-time scoring after warmup.
- It handles mixed numeric features.
- It is simple enough to explain in interviews and demos.

Important limitation:

Isolation Forest only sees the features you give it. If a DoS attack looks normal in payload values but abnormal in traffic shape, the feature set must include traffic-shape features.

## 8. Module Walkthrough

### `simulator/can_simulator.py`

Purpose:

Generate normal CAN telemetry and four labeled attacks.

Inputs:

- `--attack-mode`
- `--rate-hz`
- `--topic`
- `--bootstrap-servers`

Outputs:

- JSON Kafka messages on `can-telematics`.

Key functions:

- `VehicleState.update`: Evolves realistic speed, RPM, brake pressure, steering, and coolant values.
- `generate_normal_frames`: Emits normal engine, brake, speed, steering, and coolant frames.
- `generate_fuzzing_frames`: Emits random IDs and random payloads.
- `generate_replay_frames`: Replays buffered normal frames.
- `generate_injection_frames`: Injects targeted abnormal values.
- `generate_dos_frames`: Floods `0x000` with zeroed payloads.
- `run`: Sends generated frames to Kafka continuously.

Demo talking points:

- Show normal traffic first.
- Run injection and filter `attack_type_label: "injection"` in Kibana.
- Run DoS and show `features.dominant_id_ratio`, `features.bus_frequency_1s`, and `features.id_entropy_1s`.

Interview questions:

- Why is `0x000` dangerous on CAN?
- Why does the simulator include ground-truth labels?
- Why do replay attacks look harder than fuzzing?

### `detection/ml_detector.py`

Purpose:

Consume Kafka records, extract features, train the model, score new messages, and index enriched events.

Inputs:

- Kafka topic records.
- CLI settings such as `--warmup-samples`, `--contamination`, and `--index-name`.

Outputs:

- Elasticsearch documents with `is_anomaly`, `anomaly_score`, `features`, and `pipeline_latency_ms`.

Key functions:

- `_connect_consumer`: Connects to Kafka.
- `_connect_elasticsearch`: Connects to Elasticsearch.
- `_ensure_index`: Creates the destination index mapping.
- `extract_features`: Converts a CAN frame into numeric model features.
- `_traffic_features`: Builds one-second bus behavior features.
- `fit_model`: Fits scaler and Isolation Forest on warmup vectors.
- `infer`: Scores one feature vector.
- `process_frame`: Enriches each record.
- `flush_bulk`: Writes records to Elasticsearch efficiently.

Demo talking points:

- Explain warmup: the model learns baseline normal traffic before inference.
- Explain `is_anomaly`: `-1` means anomaly, `1` means normal.
- Show `pipeline_latency_ms` as the real-time processing health signal.

Interview questions:

- Why scale features before Isolation Forest?
- Why is warmup data quality important?
- What happens if attack traffic enters the warmup window?
- Why did DoS need traffic-window features?

### `evaluation/evaluate_detection.py`

Purpose:

Compute detection metrics from indexed Elasticsearch records.

Inputs:

- Elasticsearch index pattern.
- Optional start and end timestamps.

Outputs:

- `reports/detection-evaluation.json`
- `reports/detection-evaluation.csv`
- Console summary.

Key metrics:

- True Positive
- True Negative
- False Positive
- False Negative
- Detection Rate
- Precision
- Recall
- F1
- False Positive Rate

Demo talking points:

- Kibana is for visual analysis.
- The evaluation script is for repeatable measurement.
- Per-attack metrics show which threat class needs engineering attention.

## 9. Beginner To Advanced Learning Path

Beginner:

1. Run Docker Compose.
2. Run normal simulator.
3. Run detector.
4. Open Kibana Discover.
5. Filter by `attack_type_label`.

Intermediate:

1. Compare normal, fuzz, injection, replay, and DoS.
2. Add useful columns in Kibana.
3. Run the evaluation script.
4. Explain TP/FP/TN/FN.

Advanced:

1. Tune `--contamination`.
2. Compare payload features vs traffic features.
3. Build separate payload and traffic anomaly models.
4. Create attack-specific dashboards.
5. Add supervised models once enough labeled data exists.

