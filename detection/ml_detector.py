#!/usr/bin/env python3
"""
Kafka stream processor for CAV CAN anomaly detection.

The detector warms up on an initial window of normal traffic, fits an Isolation
Forest model, then enriches every incoming CAN frame with anomaly metadata and
indexes the result into Elasticsearch.
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import signal
import time
from collections import Counter, defaultdict, deque
from datetime import datetime, timezone
from typing import Any, Deque, Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd
from elasticsearch import Elasticsearch, helpers
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler


DEFAULT_TOPIC = "can-telematics"
DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"
DEFAULT_ELASTICSEARCH_URL = "http://localhost:9200"
DEFAULT_INDEX = "can-security-alerts"
MIN_STABLE_WARMUP_BUS_FREQUENCY = 100.0
BASELINE_MIN_TIMING_SAMPLES = 30
EXACTNESS_CV_RATIO_THRESHOLD = 0.35
IAT_ROBUST_Z_THRESHOLD = 4.0
REPLAY_REPEAT_WINDOW_NS = 3_000_000_000
REPLAY_REPEAT_MIN_COUNT = 4
REPLAY_COMPOSITE_THRESHOLD = 0.30
RARE_REPLAY_COMPOSITE_FLOOR = 0.25

ARBITRATION_ID_MAP = {
    "0x000": 0,
    "0x110": 272,
    "0x220": 544,
    "0x330": 816,
    "0x440": 1088,
    "0x550": 1360,
}
NORMAL_ARBITRATION_IDS = {"0x110", "0x220", "0x330", "0x440", "0x550"}

FEATURE_COLUMNS = [
    "arbitration_id_numeric",
    "rpm",
    "speed",
    "brake_pressure",
    "coolant_temp",
    "throttle_position",
    "engine_load",
    "gear",
    "abs_active",
    "wheel_speed_fl",
    "wheel_speed_fr",
    "acceleration",
    "steering_angle",
    "yaw_rate",
    "lane_assist_active",
    "battery_voltage",
    "ambient_temp",
    "raw_byte_mean",
    "raw_byte_std",
    "interarrival_ms",
    "id_frequency_1s",
    "bus_frequency_1s",
    "id_entropy_1s",
    "dominant_id_ratio",
    "rolling_interarrival_mean",
    "rolling_interarrival_std",
    "bus_utilization",
    "repeated_id_ratio",
    "reserved_id_flag",
    "arbitration_priority_score",
    "unique_ids_per_second",
    "payload_repeat_ratio",
]


class CAVIsolationForestDetector:
    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        elasticsearch_url: str,
        index_name: str,
        warmup_samples: int,
        contamination: float,
        bulk_size: int,
        consumer_group: str,
        auto_offset_reset: str,
    ) -> None:
        self.topic = topic
        self.index_name = index_name
        self.warmup_samples = warmup_samples
        self.bulk_size = bulk_size
        self.running = True

        self.model = IsolationForest(
            n_estimators=200,
            contamination=contamination,
            max_samples="auto",
            random_state=42,
            n_jobs=-1,
        )
        self.scaler = StandardScaler()
        self.is_model_ready = False
        self.warmup_vectors: List[List[float]] = []

        self.last_seen_ns_by_id: Dict[str, int] = {}
        self.arrival_times_by_id: Dict[str, Deque[int]] = defaultdict(lambda: deque(maxlen=3000))
        self.interarrivals_by_id: Dict[str, Deque[float]] = defaultdict(lambda: deque(maxlen=300))
        self.bus_arrival_times: Deque[int] = deque(maxlen=10000)
        self.bus_events: Deque[Tuple[int, str]] = deque(maxlen=10000)
        self.payload_events: Deque[Tuple[int, str]] = deque(maxlen=10000)
        self.payload_seen_by_id: Dict[Tuple[str, str], int] = {}
        self.payload_times_by_id_signature: Dict[Tuple[str, str], Deque[int]] = defaultdict(lambda: deque(maxlen=200))
        self.replay_suspicion_by_id: Dict[str, Deque[int]] = defaultdict(lambda: deque(maxlen=1000))
        self.transition_counts: Dict[str, Counter] = defaultdict(Counter)
        self.transition_totals: Counter = Counter()
        self.warmup_previous_arbitration_id: Optional[str] = None
        self.timing_baseline_samples_by_id: Dict[str, Deque[float]] = defaultdict(lambda: deque(maxlen=1000))
        self.timing_baseline_by_id: Dict[str, Dict[str, float]] = {}
        self.last_signal_state_by_id: Dict[str, Dict[str, Any]] = {}
        self.bulk_buffer: List[Dict[str, Any]] = []
        self.stats = Counter()

        self.consumer = self._connect_consumer(bootstrap_servers, topic, consumer_group, auto_offset_reset)
        self.es = self._connect_elasticsearch(elasticsearch_url)
        self._ensure_index()

    @staticmethod
    def _connect_consumer(
        bootstrap_servers: str,
        topic: str,
        consumer_group: str,
        auto_offset_reset: str,
    ) -> KafkaConsumer:
        last_error: Optional[BaseException] = None
        for attempt in range(1, 31):
            try:
                consumer = KafkaConsumer(
                    topic,
                    bootstrap_servers=bootstrap_servers,
                    group_id=consumer_group,
                    auto_offset_reset=auto_offset_reset,
                    enable_auto_commit=True,
                    value_deserializer=lambda payload: json.loads(payload.decode("utf-8")),
                    consumer_timeout_ms=1000,
                    max_poll_records=500,
                )
                logging.info("Connected Kafka consumer to %s topic=%s", bootstrap_servers, topic)
                return consumer
            except NoBrokersAvailable as exc:
                last_error = exc
                logging.warning("Kafka unavailable, retrying connection %d/30...", attempt)
                time.sleep(2)
        raise RuntimeError(f"Unable to connect to Kafka at {bootstrap_servers}") from last_error

    @staticmethod
    def _connect_elasticsearch(url: str) -> Elasticsearch:
        es = Elasticsearch(url, request_timeout=30, retry_on_timeout=True, max_retries=5)
        for attempt in range(1, 31):
            try:
                if es.ping():
                    logging.info("Connected to Elasticsearch at %s", url)
                    return es
            except Exception as exc:
                logging.warning("Elasticsearch unavailable, retrying connection %d/30: %s", attempt, exc)
            time.sleep(2)
        raise RuntimeError(f"Unable to connect to Elasticsearch at {url}")

    def _ensure_index(self) -> None:
        mappings = {
            "mappings": {
                "properties": {
                    "timestamp": {"type": "date"},
                    "detector_timestamp": {"type": "date"},
                    "vehicle_id": {"type": "keyword"},
                    "session_id": {"type": "keyword"},
                    "arbitration_id": {"type": "keyword"},
                    "attack_type_label": {"type": "keyword"},
                    "detector_phase": {"type": "keyword"},
                    "is_anomaly": {"type": "integer"},
                    "anomaly_score": {"type": "float"},
                    "pipeline_latency_ms": {"type": "float"},
                    "payload": {"type": "object", "enabled": True},
                    "features": {"type": "object", "enabled": True},
                }
            },
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 0,
                "index.refresh_interval": "1s",
            },
        }
        if not self.es.indices.exists(index=self.index_name):
            self.es.indices.create(index=self.index_name, **mappings)
            logging.info("Created Elasticsearch index %s", self.index_name)

    def stop(self, *_: Any) -> None:
        self.running = False

    @staticmethod
    def _parse_arbitration_id(value: Any) -> int:
        if value is None:
            return -1
        text = str(value)
        if text in ARBITRATION_ID_MAP:
            return ARBITRATION_ID_MAP[text]
        try:
            return int(text, 16) if text.lower().startswith("0x") else int(text)
        except ValueError:
            return -1

    @staticmethod
    def _numeric(payload: Dict[str, Any], key: str, default: float = 0.0) -> float:
        value = payload.get(key, default)
        if isinstance(value, bool):
            return float(value)
        try:
            numeric = float(value)
            if math.isnan(numeric) or math.isinf(numeric):
                return default
            return numeric
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _bool(payload: Dict[str, Any], key: str) -> float:
        return 1.0 if bool(payload.get(key, False)) else 0.0

    @staticmethod
    def _raw_byte_stats(payload: Dict[str, Any]) -> Tuple[float, float]:
        raw_bytes = payload.get("raw_bytes")
        if not isinstance(raw_bytes, list) or not raw_bytes:
            return 0.0, 0.0
        values = np.array([float(v) for v in raw_bytes if isinstance(v, (int, float))], dtype=float)
        if values.size == 0:
            return 0.0, 0.0
        return float(values.mean()), float(values.std())

    @staticmethod
    def _payload_signature(payload: Dict[str, Any]) -> str:
        try:
            return json.dumps(payload, sort_keys=True, separators=(",", ":"))
        except (TypeError, ValueError):
            return str(payload)

    @staticmethod
    def _id_entropy(ids: Iterable[str]) -> float:
        counts = Counter(ids)
        total = sum(counts.values())
        if total <= 1:
            return 0.0
        entropy = 0.0
        for count in counts.values():
            probability = count / total
            entropy -= probability * math.log2(probability)
        max_entropy = math.log2(max(2, len(counts)))
        return entropy / max_entropy if max_entropy > 0 else 0.0

    @staticmethod
    def _reserved_id_flag(arbitration_id_numeric: int) -> float:
        return 1.0 if arbitration_id_numeric <= 0x00F else 0.0

    @staticmethod
    def _arbitration_priority_score(arbitration_id_numeric: int) -> float:
        if arbitration_id_numeric < 0:
            return 0.0
        bounded = min(0x7FF, max(0, arbitration_id_numeric))
        return 1.0 - (bounded / 0x7FF)

    @staticmethod
    def _robust_stats(values: Iterable[float]) -> Dict[str, float]:
        array = np.array([value for value in values if value > 0.0], dtype=float)
        if array.size == 0:
            return {"median": 0.0, "mad": 0.0, "mean": 0.0, "std": 0.0, "cv": 0.0}
        median = float(np.median(array))
        mad = float(np.median(np.abs(array - median)))
        mean = float(array.mean())
        std = float(array.std())
        cv = std / mean if mean > 0.0 else 0.0
        return {"median": median, "mad": mad, "mean": mean, "std": std, "cv": cv}

    def _transition_features(self, previous_id: Optional[str], arbitration_id: str) -> Dict[str, float]:
        if not previous_id or not self.is_model_ready:
            return {
                "transition_probability": 1.0,
                "transition_surprise": 0.0,
                "rare_transition_flag": 0.0,
            }

        total = self.transition_totals[previous_id]
        if total < 20:
            return {
                "transition_probability": 1.0,
                "transition_surprise": 0.0,
                "rare_transition_flag": 0.0,
            }

        known_next_ids = max(1, len(self.transition_counts[previous_id]))
        alpha = 0.5
        probability = (self.transition_counts[previous_id][arbitration_id] + alpha) / (total + alpha * known_next_ids)
        surprise = -math.log(max(probability, 1e-9))
        return {
            "transition_probability": float(probability),
            "transition_surprise": float(surprise),
            "rare_transition_flag": 1.0 if probability < 0.02 else 0.0,
        }

    def _timing_rhythm_features(self, arbitration_id: str, interarrival_ms: float) -> Dict[str, float]:
        baseline = self.timing_baseline_by_id.get(arbitration_id)
        interarrivals = np.array(self.interarrivals_by_id[arbitration_id], dtype=float)
        rolling_stats = self._robust_stats(interarrivals)

        if not baseline or baseline["count"] < BASELINE_MIN_TIMING_SAMPLES or interarrival_ms <= 0.0:
            return {
                "iat_robust_z": 0.0,
                "iat_drop_flag": 0.0,
                "iat_spike_flag": 0.0,
                "iat_exactness_ratio": 1.0,
                "iat_exactness_flag": 0.0,
                "timing_anomaly_score": 0.0,
            }

        mad = max(baseline["mad"], 0.5)
        robust_z = (interarrival_ms - baseline["median"]) / (1.4826 * mad)
        exactness_ratio = rolling_stats["cv"] / max(baseline["cv"], 0.01)
        drop_flag = robust_z <= -IAT_ROBUST_Z_THRESHOLD
        spike_flag = robust_z >= IAT_ROBUST_Z_THRESHOLD
        exactness_flag = (
            len(interarrivals) >= BASELINE_MIN_TIMING_SAMPLES
            and baseline["cv"] >= 0.03
            and exactness_ratio <= EXACTNESS_CV_RATIO_THRESHOLD
        )
        timing_anomaly_score = min(
            1.0,
            max(
                abs(robust_z) / 8.0,
                1.0 - min(1.0, exactness_ratio) if exactness_flag else 0.0,
            ),
        )
        return {
            "iat_robust_z": float(robust_z),
            "iat_drop_flag": 1.0 if drop_flag else 0.0,
            "iat_spike_flag": 1.0 if spike_flag else 0.0,
            "iat_exactness_ratio": float(exactness_ratio),
            "iat_exactness_flag": 1.0 if exactness_flag else 0.0,
            "timing_anomaly_score": float(timing_anomaly_score),
        }

    def _payload_repeat_features(
        self,
        arbitration_id: str,
        payload_signature: str,
        now_ns: int,
        timing_features: Dict[str, float],
    ) -> Dict[str, float]:
        signature_key = (arbitration_id, payload_signature)
        signature_times = self.payload_times_by_id_signature[signature_key]
        signature_times.append(now_ns)
        repeat_window_start = now_ns - REPLAY_REPEAT_WINDOW_NS
        while signature_times and signature_times[0] < repeat_window_start:
            signature_times.popleft()

        repeat_count_3s = max(0, len(signature_times) - 1)
        repeat_score = min(1.0, repeat_count_3s / 12.0)
        timing_score = timing_features["timing_anomaly_score"]
        if timing_features["iat_spike_flag"]:
            timing_score = max(timing_score, 0.75)
        if timing_features["iat_exactness_flag"]:
            timing_score = max(timing_score, 0.65)

        composite_score = repeat_score * timing_score
        return {
            "payload_signature_repeats_3s": float(repeat_count_3s),
            "payload_repeat_score": float(repeat_score),
            "payload_timing_replay_score": float(composite_score),
        }

    def _state_transition_features(
        self,
        arbitration_id: str,
        features: Dict[str, float],
        now_ns: int,
    ) -> Dict[str, float]:
        previous = self.last_signal_state_by_id.get(arbitration_id)
        self.last_signal_state_by_id[arbitration_id] = {"features": dict(features), "time_ns": now_ns}

        if not previous:
            return {
                "state_transition_violation_score": 0.0,
                "speed_delta_mps2": 0.0,
                "rpm_delta_per_second": 0.0,
                "coolant_delta_per_second": 0.0,
                "brake_drop_per_second": 0.0,
                "odometer_rollback_flag": 0.0,
            }

        elapsed_seconds = max(1e-3, (now_ns - int(previous["time_ns"])) / 1_000_000_000.0)
        previous_features = previous["features"]

        speed_delta_mps2 = ((features["speed"] - previous_features.get("speed", 0.0)) * 0.277778) / elapsed_seconds
        rpm_delta_per_second = abs(features["rpm"] - previous_features.get("rpm", 0.0)) / elapsed_seconds
        coolant_delta_per_second = abs(features["coolant_temp"] - previous_features.get("coolant_temp", 0.0)) / elapsed_seconds
        brake_delta_per_second = (previous_features.get("brake_pressure", 0.0) - features["brake_pressure"]) / elapsed_seconds
        current_odometer = features.get("odometer_km", -1.0)
        previous_odometer = previous_features.get("odometer_km", -1.0)
        odometer_rollback = (
            current_odometer >= 0.0
            and previous_odometer >= 0.0
            and current_odometer + 0.001 < previous_odometer
        )

        violation_score = 0.0
        if abs(speed_delta_mps2) > 12.0:
            violation_score += 0.45
        if rpm_delta_per_second > 4500.0:
            violation_score += 0.30
        if coolant_delta_per_second > 8.0:
            violation_score += 0.25
        if brake_delta_per_second > 500.0 and features["brake_pressure"] <= 0.1:
            violation_score += 0.30
        if odometer_rollback:
            violation_score += 0.50

        return {
            "state_transition_violation_score": float(min(1.0, violation_score)),
            "speed_delta_mps2": float(speed_delta_mps2),
            "rpm_delta_per_second": float(rpm_delta_per_second),
            "coolant_delta_per_second": float(coolant_delta_per_second),
            "brake_drop_per_second": float(brake_delta_per_second),
            "odometer_rollback_flag": 1.0 if odometer_rollback else 0.0,
        }

    def _update_warmup_baselines(self, arbitration_id: str, features: Dict[str, float]) -> None:
        previous_id = self.warmup_previous_arbitration_id
        if previous_id:
            self.transition_counts[previous_id][arbitration_id] += 1
            self.transition_totals[previous_id] += 1
        self.warmup_previous_arbitration_id = arbitration_id

        interarrival_ms = features.get("interarrival_ms", 0.0)
        if interarrival_ms > 0.0:
            self.timing_baseline_samples_by_id[arbitration_id].append(interarrival_ms)

    def _finalize_stream_baselines(self) -> None:
        for arbitration_id, samples in self.timing_baseline_samples_by_id.items():
            stats = self._robust_stats(samples)
            stats["count"] = float(len(samples))
            self.timing_baseline_by_id[arbitration_id] = stats

    def _traffic_features(
        self,
        arbitration_id: str,
        payload: Dict[str, Any],
        now_ns: int,
    ) -> Dict[str, float]:
        one_second_ago = now_ns - 1_000_000_000
        previous_bus_id = self.bus_events[-1][1] if self.bus_events else None
        by_id = self.arrival_times_by_id[arbitration_id]
        by_id.append(now_ns)
        self.bus_arrival_times.append(now_ns)
        self.bus_events.append((now_ns, arbitration_id))
        payload_signature = self._payload_signature(payload)
        self.payload_events.append((now_ns, payload_signature))

        while by_id and by_id[0] < one_second_ago:
            by_id.popleft()
        while self.bus_arrival_times and self.bus_arrival_times[0] < one_second_ago:
            self.bus_arrival_times.popleft()
        while self.bus_events and self.bus_events[0][0] < one_second_ago:
            self.bus_events.popleft()
        while self.payload_events and self.payload_events[0][0] < one_second_ago:
            self.payload_events.popleft()
        replay_window = self.replay_suspicion_by_id[arbitration_id]
        while replay_window and replay_window[0] < one_second_ago:
            replay_window.popleft()

        previous = self.last_seen_ns_by_id.get(arbitration_id)
        self.last_seen_ns_by_id[arbitration_id] = now_ns
        interarrival_ms = 0.0 if previous is None else (now_ns - previous) / 1_000_000.0
        if previous is not None:
            self.interarrivals_by_id[arbitration_id].append(interarrival_ms)

        previous_payload_seen = self.payload_seen_by_id.get((arbitration_id, payload_signature))
        if previous_payload_seen is not None and (now_ns - previous_payload_seen) > 500_000_000:
            replay_window.append(now_ns)
        self.payload_seen_by_id[(arbitration_id, payload_signature)] = now_ns

        bus_ids = [event_id for _, event_id in self.bus_events]
        bus_total = len(bus_ids)
        id_counts = Counter(bus_ids)
        payload_counts = Counter(signature for _, signature in self.payload_events)
        dominant_count = max(id_counts.values(), default=0)
        payload_repeat_count = max(payload_counts.values(), default=0)
        interarrivals = np.array(self.interarrivals_by_id[arbitration_id], dtype=float)
        arbitration_id_numeric = self._parse_arbitration_id(arbitration_id)
        timing_features = self._timing_rhythm_features(arbitration_id, interarrival_ms)
        payload_repeat_features = self._payload_repeat_features(arbitration_id, payload_signature, now_ns, timing_features)

        features = {
            "interarrival_ms": interarrival_ms,
            "id_frequency_1s": float(len(by_id)),
            "bus_frequency_1s": float(len(self.bus_arrival_times)),
            "id_entropy_1s": self._id_entropy(bus_ids),
            "dominant_id_ratio": dominant_count / bus_total if bus_total else 0.0,
            "rolling_interarrival_mean": float(interarrivals.mean()) if interarrivals.size else 0.0,
            "rolling_interarrival_std": float(interarrivals.std()) if interarrivals.size else 0.0,
            "bus_utilization": min(1.0, len(self.bus_arrival_times) / 5000.0),
            "repeated_id_ratio": len(by_id) / bus_total if bus_total else 0.0,
            "reserved_id_flag": self._reserved_id_flag(arbitration_id_numeric),
            "arbitration_priority_score": self._arbitration_priority_score(arbitration_id_numeric),
            "unique_ids_per_second": float(len(id_counts)),
            "payload_repeat_ratio": payload_repeat_count / bus_total if bus_total else 0.0,
            "replay_signature_repeats_1s": float(len(replay_window)),
        }
        features.update(self._transition_features(previous_bus_id, arbitration_id))
        features.update(timing_features)
        features.update(payload_repeat_features)
        return features

    def extract_features(self, frame: Dict[str, Any]) -> Dict[str, float]:
        payload = frame.get("payload", {})
        if not isinstance(payload, dict):
            payload = {}

        now_ns = time.time_ns()
        arbitration_id = str(frame.get("arbitration_id", ""))
        traffic_features = self._traffic_features(arbitration_id, payload, now_ns)
        raw_byte_mean, raw_byte_std = self._raw_byte_stats(payload)

        features = {
            "arbitration_id_numeric": float(self._parse_arbitration_id(arbitration_id)),
            "rpm": self._numeric(payload, "rpm"),
            "speed": self._numeric(payload, "speed"),
            "brake_pressure": self._numeric(payload, "brake_pressure"),
            "coolant_temp": self._numeric(payload, "coolant_temp"),
            "throttle_position": self._numeric(payload, "throttle_position"),
            "engine_load": self._numeric(payload, "engine_load"),
            "gear": self._numeric(payload, "gear"),
            "abs_active": self._bool(payload, "abs_active"),
            "wheel_speed_fl": self._numeric(payload, "wheel_speed_fl"),
            "wheel_speed_fr": self._numeric(payload, "wheel_speed_fr"),
            "acceleration": self._numeric(payload, "acceleration"),
            "steering_angle": self._numeric(payload, "steering_angle"),
            "yaw_rate": self._numeric(payload, "yaw_rate"),
            "lane_assist_active": self._bool(payload, "lane_assist_active"),
            "battery_voltage": self._numeric(payload, "battery_voltage"),
            "ambient_temp": self._numeric(payload, "ambient_temp"),
            "odometer_km": self._numeric(payload, "odometer_km", default=-1.0),
            "raw_byte_mean": raw_byte_mean,
            "raw_byte_std": raw_byte_std,
        }
        features.update(traffic_features)
        features.update(self._state_transition_features(arbitration_id, features, now_ns))
        return features

    @staticmethod
    def vectorize(features: Dict[str, float]) -> List[float]:
        return [float(features[column]) for column in FEATURE_COLUMNS]

    def fit_model(self) -> None:
        self._finalize_stream_baselines()
        warmup_df = pd.DataFrame(self.warmup_vectors, columns=FEATURE_COLUMNS)
        scaled = self.scaler.fit_transform(warmup_df.values)
        self.model.fit(scaled)
        self.is_model_ready = True
        logging.info("Isolation Forest fitted with %d warmup samples", len(self.warmup_vectors))

    def infer(self, vector: List[float]) -> Tuple[int, float]:
        scaled = self.scaler.transform(np.array(vector, dtype=float).reshape(1, -1))
        prediction = int(self.model.predict(scaled)[0])
        score = float(self.model.decision_function(scaled)[0])
        return prediction, score

    @staticmethod
    def apply_security_rules(frame: Dict[str, Any], features: Dict[str, float]) -> Tuple[List[str], float]:
        payload = frame.get("payload", {})
        if not isinstance(payload, dict):
            payload = {}

        rules: List[str] = []
        arbitration_id = str(frame.get("arbitration_id", ""))

        speed = features["speed"]
        acceleration = features["acceleration"]
        rpm = features["rpm"]
        coolant_temp = features["coolant_temp"]
        brake_pressure = features["brake_pressure"]
        wheel_speed = max(features["wheel_speed_fl"], features["wheel_speed_fr"])
        battery_voltage = features["battery_voltage"]

        if arbitration_id == "0x000" and features["id_frequency_1s"] >= 25:
            rules.append("dominant_low_priority_id_flood")
        if arbitration_id not in NORMAL_ARBITRATION_IDS and "raw_bytes" in payload:
            rules.append("unknown_id_raw_payload")

        if speed >= 180.0:
            rules.append("impossible_speed")
        if abs(acceleration) >= 7.0:
            rules.append("impossible_acceleration")
        if rpm >= 7000.0:
            rules.append("rpm_redline_violation")
        if coolant_temp >= 130.0:
            rules.append("thermal_limit_violation")
        if battery_voltage and battery_voltage <= 11.5:
            rules.append("battery_voltage_drop")
        if brake_pressure <= 0.1 and wheel_speed >= 70.0 and features["brake_drop_per_second"] > 500.0:
            rules.append("brake_suppression")

        if (
            features["payload_signature_repeats_3s"] >= REPLAY_REPEAT_MIN_COUNT
            and features["payload_timing_replay_score"] >= REPLAY_COMPOSITE_THRESHOLD
        ):
            rules.append("replayed_payload_signature_cluster")
        if (
            features["payload_signature_repeats_3s"] >= REPLAY_REPEAT_MIN_COUNT
            and features["rare_transition_flag"]
            and features["timing_anomaly_score"] >= 0.5
            and features["payload_timing_replay_score"] >= RARE_REPLAY_COMPOSITE_FLOOR
        ):
            rules.append("rare_sequence_replay_timing")
        if features["state_transition_violation_score"] >= 0.5:
            rules.append("physics_state_transition_violation")

        rule_score = min(1.0, len(rules) / 3.0)
        return rules, rule_score

    @staticmethod
    def _pipeline_latency_ms(frame: Dict[str, Any]) -> float:
        send_time_ns = frame.get("simulator_send_time_ns")
        if isinstance(send_time_ns, int):
            return max(0.0, (time.time_ns() - send_time_ns) / 1_000_000.0)
        return 0.0

    def process_frame(self, frame: Dict[str, Any]) -> Dict[str, Any]:
        features = self.extract_features(frame)
        vector = self.vectorize(features)

        if not self.is_model_ready:
            is_normal = str(frame.get("attack_type_label", "normal")) == "normal"
            is_stable_window = features["bus_frequency_1s"] >= MIN_STABLE_WARMUP_BUS_FREQUENCY
            if is_normal and is_stable_window:
                self._update_warmup_baselines(str(frame.get("arbitration_id", "")), features)
                self.warmup_vectors.append(vector)
            if len(self.warmup_vectors) >= self.warmup_samples:
                self.fit_model()
            prediction = 1
            score = 0.0
            phase = "warmup"
        else:
            model_prediction, score = self.infer(vector)
            triggered_rules, rule_score = self.apply_security_rules(frame, features)
            prediction = -1 if triggered_rules else 1
            phase = "inference"

        enriched = dict(frame)
        enriched.update(
            {
                "detector_timestamp": datetime.now(timezone.utc).isoformat(),
                "detector_phase": phase,
                "is_anomaly": prediction,
                "model_prediction": prediction if phase == "warmup" else model_prediction,
                "anomaly_score": score,
                "rule_score": 0.0 if phase == "warmup" else rule_score,
                "triggered_rules": [] if phase == "warmup" else triggered_rules,
                "features": features,
                "pipeline_latency_ms": self._pipeline_latency_ms(frame),
            }
        )
        return enriched

    def enqueue_for_indexing(self, record: Dict[str, Any]) -> None:
        document_id = f"{record.get('session_id')}:{record.get('sequence')}:{record.get('arbitration_id')}:{time.time_ns()}"
        self.bulk_buffer.append({"_index": self.index_name, "_id": document_id, "_source": record})
        if len(self.bulk_buffer) >= self.bulk_size:
            self.flush_bulk()

    def flush_bulk(self) -> None:
        if not self.bulk_buffer:
            return
        success, errors = helpers.bulk(self.es, self.bulk_buffer, stats_only=True, raise_on_error=False)
        self.stats["indexed"] += success
        self.stats["index_errors"] += errors
        if errors:
            logging.warning("Elasticsearch bulk completed with %d errors", errors)
        self.bulk_buffer.clear()

    def log_metrics(self, last_log_time: float) -> float:
        if time.monotonic() - last_log_time < 5:
            return last_log_time
        logging.info(
            "processed=%d indexed=%d anomalies=%d warmup=%d model_ready=%s",
            self.stats["processed"],
            self.stats["indexed"],
            self.stats["anomalies"],
            len(self.warmup_vectors),
            self.is_model_ready,
        )
        return time.monotonic()

    def run(self) -> None:
        logging.info("Starting detector topic=%s index=%s warmup_samples=%d", self.topic, self.index_name, self.warmup_samples)
        last_log_time = time.monotonic()
        try:
            while self.running:
                for message in self.consumer:
                    if not self.running:
                        break
                    frame = message.value
                    enriched = self.process_frame(frame)
                    self.enqueue_for_indexing(enriched)

                    self.stats["processed"] += 1
                    if enriched["is_anomaly"] == -1:
                        self.stats["anomalies"] += 1
                    last_log_time = self.log_metrics(last_log_time)
        finally:
            logging.info("Detector stopping; flushing remaining Elasticsearch records.")
            self.flush_bulk()
            self.consumer.close()
            self.es.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Detect CAV CAN anomalies from Kafka and index to Elasticsearch.")
    parser.add_argument("--bootstrap-servers", default=DEFAULT_BOOTSTRAP_SERVERS)
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    parser.add_argument("--elasticsearch-url", default=DEFAULT_ELASTICSEARCH_URL)
    parser.add_argument("--index-name", default=DEFAULT_INDEX)
    parser.add_argument("--warmup-samples", type=int, default=1000)
    parser.add_argument("--contamination", type=float, default=0.03)
    parser.add_argument("--bulk-size", type=int, default=250)
    parser.add_argument("--consumer-group", default="cav-security-detector")
    parser.add_argument("--auto-offset-reset", choices=["earliest", "latest"], default="latest")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

    detector = CAVIsolationForestDetector(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        elasticsearch_url=args.elasticsearch_url,
        index_name=args.index_name,
        warmup_samples=args.warmup_samples,
        contamination=args.contamination,
        bulk_size=args.bulk_size,
        consumer_group=args.consumer_group,
        auto_offset_reset=args.auto_offset_reset,
    )
    signal.signal(signal.SIGINT, detector.stop)
    signal.signal(signal.SIGTERM, detector.stop)
    detector.run()


if __name__ == "__main__":
    main()
