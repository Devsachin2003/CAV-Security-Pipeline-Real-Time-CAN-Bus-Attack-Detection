# Model Evaluation And DoS Detection Improvement

## Current Performance Observation

The pipeline successfully indexed more than 100,000 records end to end. Initial evaluation showed strong fuzzing detection, but weak replay, injection, and especially DoS detection.

Approximate initial detection:

| Attack | Detection |
| --- | ---: |
| fuzz | 99.96% |
| replay | 26% |
| injection | 25% |
| dos | 0.003% |

## Why DoS Was Missed

The earlier feature set was mostly payload-oriented:

```text
rpm, speed, brake_pressure, coolant_temp, throttle_position,
engine_load, gear, wheel speeds, steering, raw byte mean/std
```

That works well for fuzzing and some injection cases because those attacks change field values. DoS is different. In this simulator, DoS floods the bus with repeated zero payloads on `0x000`. The payload is simple, but the attack signature is traffic dominance:

- One arbitration ID appears too often.
- The bus event rate spikes.
- Interarrival time collapses.
- ID diversity falls.
- Payload repetition rises.
- `0x000` has extreme arbitration priority.

So the model needed traffic-window features.

## New DoS Features

### `id_entropy_1s`

Measures diversity of arbitration IDs in the last second.

Formula:

```text
H = -sum(p_i * log2(p_i))
normalized_H = H / log2(number_of_unique_ids)
```

Why it helps:

A normal bus has several recurring IDs. A DoS flood by one ID drives entropy toward zero.

### `dominant_id_ratio`

Measures the share of the most frequent ID in the last second.

Formula:

```text
dominant_id_ratio = max(count(id_i)) / total_messages
```

Why it helps:

In a DoS flood, one arbitration ID can dominate the bus.

### `rolling_interarrival_mean`

Mean time between messages for the current ID.

Formula:

```text
mean(delta_t_id)
```

Why it helps:

Flooding reduces average interarrival time for the attacking ID.

### `rolling_interarrival_std`

Standard deviation of recent interarrival times for the current ID.

Formula:

```text
std(delta_t_id)
```

Why it helps:

Automated flooding often produces unusually regular timing.

### `bus_utilization`

Approximate bus load from observed events per second.

Formula:

```text
bus_utilization = min(1.0, bus_frequency_1s / 5000)
```

Why it helps:

DoS attacks aim to saturate the bus. A rising utilization estimate is a direct DoS indicator.

### `repeated_id_ratio`

Share of the one-second bus window represented by the current ID.

Formula:

```text
repeated_id_ratio = count(current_id) / total_messages
```

Why it helps:

It gives the current event local context. A `0x000` message is more suspicious when most recent messages are also `0x000`.

### `reserved_id_flag`

Flags unusually low IDs reserved or suspicious in this simulator.

Formula:

```text
reserved_id_flag = 1 if arbitration_id <= 0x00F else 0
```

Why it helps:

The simulator DoS mode uses `0x000`. This feature lets the model learn that very low IDs have special security meaning.

### `arbitration_priority_score`

Converts CAN ID priority into a normalized score.

Formula:

```text
priority = 1 - arbitration_id / 0x7FF
```

Why it helps:

Lower CAN IDs win arbitration. DoS using low IDs is more dangerous than flooding low-priority IDs.

### `unique_ids_per_second`

Counts distinct IDs in the one-second bus window.

Formula:

```text
unique_ids_per_second = cardinality(ids in last second)
```

Why it helps:

DoS often reduces ID diversity while increasing volume.

### `payload_repeat_ratio`

Measures repetition of identical payload signatures in the bus window.

Formula:

```text
payload_repeat_ratio = max(count(payload_signature)) / total_messages
```

Why it helps:

The DoS simulator sends zeroed payloads repeatedly. Repeated payload signatures are a strong flooding clue.

## Model Settings Review

Current Isolation Forest settings:

| Setting | Current | Notes |
| --- | --- | --- |
| `n_estimators` | `200` | Reasonable for stability. |
| `contamination` | CLI default `0.03` | Controls expected anomaly rate. Tune per demo dataset. |
| `max_samples` | `auto` | Good default for unsupervised baseline. |
| `random_state` | `42` | Makes runs reproducible. |
| `n_jobs` | `-1` | Uses available CPU cores. |

Recommended settings to test:

| Scenario | Command Option |
| --- | --- |
| Conservative, fewer false positives | `--contamination 0.01` |
| Balanced demo setting | `--contamination 0.03` |
| Aggressive anomaly detection | `--contamination 0.05` |

## Recommended Model Architecture

The next model-hardening step is a two-model detector:

Payload anomaly model:

- Learns physical signal relationships.
- Good for fuzzing and injection.
- Features: RPM, speed, brakes, steering, coolant, wheel speed, raw byte stats.

Traffic anomaly model:

- Learns bus behavior.
- Good for DoS and replay timing anomalies.
- Features: entropy, dominant ratio, interarrival stats, bus utilization, repeated payloads.

Combined score:

```text
combined_anomaly_score = 0.6 * traffic_score + 0.4 * payload_score
```

For the current repo, the implemented improvement keeps one Isolation Forest but adds the missing traffic features into the shared feature vector. That is the lowest-risk production step before splitting the model.

## Evaluation Command

Run after indexing a test dataset:

```bash
python evaluation/evaluate_detection.py --index "can-security-alerts*"
```

For an isolated e2e test index:

```bash
python evaluation/evaluate_detection.py --index "can-security-alerts-e2e*"
```

Outputs:

```text
reports/detection-evaluation.json
reports/detection-evaluation.csv
```

