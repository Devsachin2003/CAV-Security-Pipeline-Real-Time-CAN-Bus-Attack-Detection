from __future__ import annotations

from collections import Counter, defaultdict, deque
from datetime import datetime, timezone
from typing import Any, Dict

from detection.ml_detector import CAVIsolationForestDetector, FEATURE_COLUMNS


def make_detector_without_network() -> CAVIsolationForestDetector:
    detector = CAVIsolationForestDetector.__new__(CAVIsolationForestDetector)
    detector.topic = "can-telematics"
    detector.index_name = "test-alerts"
    detector.warmup_samples = 10_000
    detector.bulk_size = 250
    detector.running = True
    detector.is_model_ready = False
    detector.warmup_vectors = []

    detector.last_seen_ns_by_id = {}
    detector.arrival_times_by_id = defaultdict(lambda: deque(maxlen=3000))
    detector.interarrivals_by_id = defaultdict(lambda: deque(maxlen=300))
    detector.bus_arrival_times = deque(maxlen=10000)
    detector.bus_events = deque(maxlen=10000)
    detector.payload_events = deque(maxlen=10000)
    detector.payload_seen_by_id = {}
    detector.payload_times_by_id_signature = defaultdict(lambda: deque(maxlen=200))
    detector.payload_sequence_by_id = defaultdict(lambda: deque(maxlen=3))
    detector.payload_sequence_times_by_id = defaultdict(lambda: deque(maxlen=3))
    detector.payload_ngram_seen_by_id = defaultdict(lambda: deque(maxlen=100))
    detector.replay_suspicion_by_id = defaultdict(lambda: deque(maxlen=1000))
    detector.transition_counts = defaultdict(Counter)
    detector.transition_totals = Counter()
    detector.warmup_previous_arbitration_id = None
    detector.timing_baseline_samples_by_id = defaultdict(lambda: deque(maxlen=1000))
    detector.timing_baseline_by_id = {}
    detector.last_signal_state_by_id = {}
    detector.bulk_buffer = []
    detector.stats = Counter()
    return detector


def can_frame(
    arbitration_id: str = "0x110",
    payload: Dict[str, Any] | None = None,
    send_time_ns: int = 1_700_000_000_000_000_000,
    sequence: int = 1,
) -> Dict[str, Any]:
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "event_time_epoch_ms": send_time_ns // 1_000_000,
        "simulator_send_time_ns": send_time_ns,
        "vehicle_id": "CAV-TEST",
        "session_id": "unit-test-session",
        "sequence": sequence,
        "arbitration_id": arbitration_id,
        "dlc": 8,
        "payload": payload or {"rpm": 1200.0, "throttle_position": 15.0, "engine_load": 20.0, "gear": 2},
        "attack_type_label": "normal",
    }


def test_process_can_frame_outputs_expected_shape() -> None:
    detector = make_detector_without_network()

    enriched = detector.process_can_frame(can_frame())

    assert enriched["detector_phase"] == "warmup"
    assert enriched["is_anomaly"] is False
    assert enriched["triggered_rules"] == []
    assert "features" in enriched
    assert set(FEATURE_COLUMNS).issubset(enriched["features"].keys())
    assert len(detector.vectorize(enriched["features"])) == len(FEATURE_COLUMNS)


def test_high_frequency_dos_rule_triggers_on_mock_window() -> None:
    detector = make_detector_without_network()
    frame = can_frame(
        arbitration_id="0x000",
        payload={
            "rpm": 0,
            "speed": 0,
            "brake_pressure": 0,
            "coolant_temp": 0,
            "raw_bytes": [0, 0, 0, 0, 0, 0, 0, 0],
        },
    )

    features = {}
    for _ in range(25):
        features = detector.extract_features(frame)

    triggered_rules, rule_score = detector.apply_security_rules(frame, features)

    assert "dominant_low_priority_id_flood" in triggered_rules
    assert rule_score > 0.0


def test_replayed_payload_ngram_with_tight_timing_triggers_rule() -> None:
    detector = make_detector_without_network()
    arbitration_id = "0x330"
    baseline_step_ns = 100_000_000
    replay_step_ns = 25_000_000
    start_ns = 1_700_000_000_000_000_000
    payloads = [
        {"speed": 41.0, "acceleration": 0.01, "odometer_km": 10.001},
        {"speed": 41.3, "acceleration": 0.02, "odometer_km": 10.002},
        {"speed": 41.6, "acceleration": 0.01, "odometer_km": 10.003},
    ]

    detector.timing_baseline_by_id[arbitration_id] = {
        "median": 100.0,
        "mad": 5.0,
        "mean": 100.0,
        "std": 8.0,
        "cv": 0.08,
        "count": 50.0,
    }

    sequence = 0
    for cycle in range(2):
        for offset, payload in enumerate(payloads):
            frame = can_frame(
                arbitration_id=arbitration_id,
                payload=payload,
                send_time_ns=start_ns + (cycle * len(payloads) + offset) * baseline_step_ns,
                sequence=sequence,
            )
            detector.extract_features(frame)
            sequence += 1

    features = {}
    replay_start_ns = start_ns + 5_000_000_000
    for cycle in range(2):
        for offset, payload in enumerate(payloads):
            frame = can_frame(
                arbitration_id=arbitration_id,
                payload=payload,
                send_time_ns=replay_start_ns + (cycle * len(payloads) + offset) * replay_step_ns,
                sequence=sequence,
            )
            features = detector.extract_features(frame)
            sequence += 1

    triggered_rules, _ = detector.apply_security_rules(
        can_frame(arbitration_id=arbitration_id, payload=payloads[-1], sequence=sequence),
        features,
    )

    assert features["payload_sequence_repeat_count"] >= 2
    assert features["payload_sequence_tight_timing_flag"] == 1.0
    assert "payload_ngram_replay_tight_timing" in triggered_rules
