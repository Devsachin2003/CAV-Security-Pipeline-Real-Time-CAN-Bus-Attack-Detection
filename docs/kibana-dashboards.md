# Kibana Dashboard Designs

This document defines dashboard panels for the CAV cybersecurity pipeline.

## Data View

Use one of these data views:

```text
can-security-alerts*
can-security-alerts-e2e*
```

Timestamp field:

```text
timestamp
```

## 1. Executive Security Overview

Purpose:

Give a quick operational view of total traffic, anomaly volume, attack mix, and latency.

Recommended controls:

- Time picker: `Last 15 minutes`, `Last 1 hour`, or exact test window.
- Filter dropdown: `vehicle_id`.
- Filter dropdown: `attack_type_label`.

Panels:

| Panel | Visualization | Configuration |
| --- | --- | --- |
| Total Events | Metric | Count of records |
| Total Anomalies | Metric | KQL: `is_anomaly: -1` |
| Attack Events | Metric | KQL: `attack_type_label != "normal"` |
| False Positives | Metric | KQL: `attack_type_label: "normal" and is_anomaly: -1` |
| Average Pipeline Latency | Metric | Average `pipeline_latency_ms` |
| Events Over Time | Bar or area | Count over `timestamp`, split by `attack_type_label` |
| Anomalies Over Time | Bar or area | Count over `timestamp`, KQL `is_anomaly: -1` |
| Attack Mix | Donut or bar | Top values of `attack_type_label` |
| Anomaly Score By Attack | Box plot or bar | Median `anomaly_score`, split by `attack_type_label` |

Useful KQL:

```text
detector_phase: "inference"
```

## 2. DoS Detection Dashboard

Purpose:

Show whether the detector can identify bus flooding and dominant-ID behavior.

Dashboard-level KQL:

```text
attack_type_label: "dos" or arbitration_id: "0x000"
```

Panels:

| Panel | Visualization | Configuration |
| --- | --- | --- |
| DoS Events | Metric | KQL: `attack_type_label: "dos"` |
| DoS Detected | Metric | KQL: `attack_type_label: "dos" and is_anomaly: -1` |
| Dominant ID Ratio | Line | Average `features.dominant_id_ratio` over time |
| ID Entropy | Line | Average `features.id_entropy_1s` over time |
| Bus Frequency | Line | Average `features.bus_frequency_1s` over time |
| Bus Utilization | Line | Average `features.bus_utilization` over time |
| Repeated Payload Ratio | Line | Average `features.payload_repeat_ratio` over time |
| Top Arbitration IDs | Bar | Top values of `arbitration_id` |
| Priority Score | Line | Average `features.arbitration_priority_score` over time |

Expected DoS shape:

- `features.dominant_id_ratio` rises.
- `features.id_entropy_1s` falls.
- `features.bus_frequency_1s` rises.
- `features.payload_repeat_ratio` rises.
- `arbitration_id` is often `0x000`.

## 3. Replay Attack Dashboard

Purpose:

Inspect repeated valid-looking traffic and timing irregularities.

Dashboard-level KQL:

```text
attack_type_label: "replay"
```

Panels:

| Panel | Visualization | Configuration |
| --- | --- | --- |
| Replay Events | Metric | KQL: `attack_type_label: "replay"` |
| Replay Detected | Metric | KQL: `attack_type_label: "replay" and is_anomaly: -1` |
| Replay Detection Trend | Bar | Count over `timestamp`, split by `is_anomaly` |
| Interarrival Mean | Line | Average `features.rolling_interarrival_mean` over time |
| Interarrival Std Dev | Line | Average `features.rolling_interarrival_std` over time |
| Payload Repeat Ratio | Line | Average `features.payload_repeat_ratio` over time |
| Arbitration ID Mix | Bar | Top values of `arbitration_id` |
| Score Distribution | Histogram | `anomaly_score` |

Expected replay shape:

- Payloads may remain plausible.
- Timing and repetition fields become more useful than physical-value fields.
- Replay detection usually improves when sequence-aware features are added.

