#!/usr/bin/env python3
"""
Connected Autonomous Vehicle CAN telematics simulator.

Streams JSON CAN frames into Kafka at a configurable rate. The generator emits
normal vehicle dynamics plus four explicit attack modes: fuzzing, replay,
targeted injection, and DoS/flood.
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import os
import random
import signal
import time
import uuid
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Deque, Dict, Iterable, List, Optional

from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable


DEFAULT_TOPIC = "can-telematics"
DEFAULT_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

ENGINE_ID = "0x110"
BRAKE_ID = "0x220"
SPEED_ID = "0x330"
STEERING_ID = "0x440"
COOLANT_ID = "0x550"
DOMINANT_DOS_ID = "0x000"

NORMAL_IDS = [ENGINE_ID, BRAKE_ID, SPEED_ID, STEERING_ID, COOLANT_ID]
ATTACK_MODES = {"normal", "fuzz", "replay", "injection", "dos"}


@dataclass
class VehicleState:
    """Simple but coherent vehicle dynamics used to populate CAN payloads."""

    rpm: float = 850.0
    speed: float = 0.0
    brake_pressure: float = 8.0
    coolant_temp: float = 86.0
    steering_angle: float = 0.0
    throttle_position: float = 12.0
    odometer_km: float = 0.0

    def update(self, tick: int, dt_seconds: float) -> None:
        cruise_wave = 55.0 + 32.0 * math.sin(tick / 420.0)
        micro_variation = random.gauss(0.0, 1.2)
        braking_event = 1 if (tick // 850) % 7 == 3 and tick % 850 < 130 else 0

        target_speed = max(0.0, cruise_wave + micro_variation)
        if braking_event:
            target_speed = max(0.0, target_speed - 35.0)
            self.brake_pressure = min(95.0, 58.0 + random.gauss(0.0, 8.0))
            self.throttle_position = max(0.0, 4.0 + random.gauss(0.0, 1.5))
        else:
            self.brake_pressure = max(0.0, 8.0 + random.gauss(0.0, 3.0))
            self.throttle_position = min(100.0, max(0.0, 18.0 + target_speed * 0.45 + random.gauss(0.0, 3.0)))

        self.speed = max(0.0, self.speed * 0.92 + target_speed * 0.08)
        self.rpm = max(700.0, min(6500.0, 820.0 + self.speed * 42.0 + self.throttle_position * 13.0 + random.gauss(0.0, 55.0)))
        self.coolant_temp = max(72.0, min(112.0, self.coolant_temp + random.gauss(0.0, 0.05) + (self.rpm - 2300.0) / 250000.0))
        self.steering_angle = max(-42.0, min(42.0, 8.0 * math.sin(tick / 95.0) + random.gauss(0.0, 1.4)))
        self.odometer_km += self.speed * dt_seconds / 3600.0


class CANTelematicsSimulator:
    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        attack_mode: str,
        rate_hz: float,
        replay_buffer_size: int,
        producer_client_id: str,
    ) -> None:
        if attack_mode not in ATTACK_MODES:
            raise ValueError(f"Unsupported attack mode '{attack_mode}'. Choose from {sorted(ATTACK_MODES)}.")

        self.topic = topic
        self.attack_mode = attack_mode
        self.rate_hz = rate_hz
        self.frame_interval = 1.0 / rate_hz
        self.vehicle_id = f"CAV-{uuid.uuid4().hex[:8].upper()}"
        self.session_id = str(uuid.uuid4())
        self.state = VehicleState()
        self.tick = 0
        self.running = True
        self.replay_buffer: Deque[Dict[str, Any]] = deque(maxlen=replay_buffer_size)
        self.replay_cursor = 0

        self.producer = self._connect_producer(bootstrap_servers, producer_client_id)

    @staticmethod
    def _kafka_security_config() -> Dict[str, Any]:
        config: Dict[str, Any] = {
            "security_protocol": "SASL_SSL",
            "sasl_mechanism": "PLAIN",
            "sasl_plain_username": os.getenv("KAFKA_USER"),
            "sasl_plain_password": os.getenv("KAFKA_PASSWORD"),
        }
        ca_cert_path = os.getenv("KAFKA_CA_CERT_PATH")
        if ca_cert_path:
            config["ssl_cafile"] = ca_cert_path

        missing = [key for key in ("sasl_plain_username", "sasl_plain_password") if not config.get(key)]
        if missing:
            raise RuntimeError(
                "Kafka SASL_SSL requires KAFKA_USER and KAFKA_PASSWORD environment variables."
            )
        return config

    @staticmethod
    def _connect_producer(bootstrap_servers: str, client_id: str) -> KafkaProducer:
        last_error: Optional[BaseException] = None
        for attempt in range(1, 31):
            try:
                producer = KafkaProducer(
                    bootstrap_servers=bootstrap_servers,
                    client_id=client_id,
                    value_serializer=lambda payload: json.dumps(payload).encode("utf-8"),
                    key_serializer=lambda key: key.encode("utf-8"),
                    acks="all",
                    linger_ms=5,
                    retries=5,
                    max_in_flight_requests_per_connection=1,
                    **CANTelematicsSimulator._kafka_security_config(),
                )
                logging.info("Connected to Kafka at %s", bootstrap_servers)
                return producer
            except NoBrokersAvailable as exc:
                last_error = exc
                logging.warning("Kafka unavailable, retrying connection %d/30...", attempt)
                time.sleep(2)
        raise RuntimeError(f"Unable to connect to Kafka at {bootstrap_servers}") from last_error

    def stop(self, *_: Any) -> None:
        self.running = False

    def _base_frame(self, arbitration_id: str, payload: Dict[str, Any], attack_type: str = "normal") -> Dict[str, Any]:
        now_ns = time.time_ns()
        return {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "event_time_epoch_ms": now_ns // 1_000_000,
            "simulator_send_time_ns": now_ns,
            "vehicle_id": self.vehicle_id,
            "session_id": self.session_id,
            "sequence": self.tick,
            "arbitration_id": arbitration_id,
            "dlc": 8,
            "payload": payload,
            "attack_type_label": attack_type,
        }

    def generate_normal_frames(self) -> List[Dict[str, Any]]:
        self.state.update(self.tick, self.frame_interval)
        frames = [
            self._base_frame(
                ENGINE_ID,
                {
                    "rpm": round(self.state.rpm, 1),
                    "throttle_position": round(self.state.throttle_position, 1),
                    "engine_load": round(min(100.0, self.state.throttle_position * 1.2 + random.gauss(0.0, 2.0)), 1),
                    "gear": self._estimate_gear(self.state.speed),
                },
            ),
            self._base_frame(
                BRAKE_ID,
                {
                    "brake_pressure": round(self.state.brake_pressure, 1),
                    "abs_active": bool(self.state.brake_pressure > 75.0 and random.random() < 0.08),
                    "wheel_speed_fl": round(max(0.0, self.state.speed + random.gauss(0.0, 0.5)), 1),
                    "wheel_speed_fr": round(max(0.0, self.state.speed + random.gauss(0.0, 0.5)), 1),
                },
            ),
            self._base_frame(
                SPEED_ID,
                {
                    "speed": round(self.state.speed, 1),
                    "acceleration": round(random.gauss(0.0, 0.18), 3),
                    "odometer_km": round(self.state.odometer_km, 4),
                },
            ),
            self._base_frame(
                STEERING_ID,
                {
                    "steering_angle": round(self.state.steering_angle, 2),
                    "yaw_rate": round(self.state.steering_angle * max(self.state.speed, 1.0) / 850.0 + random.gauss(0.0, 0.05), 3),
                    "lane_assist_active": bool(self.state.speed > 35.0 and random.random() < 0.92),
                },
            ),
            self._base_frame(
                COOLANT_ID,
                {
                    "coolant_temp": round(self.state.coolant_temp, 1),
                    "battery_voltage": round(13.8 + random.gauss(0.0, 0.08), 2),
                    "ambient_temp": round(24.0 + 3.0 * math.sin(self.tick / 3000.0), 1),
                },
            ),
        ]
        self.replay_buffer.extend(json.loads(json.dumps(frame)) for frame in frames)
        return frames

    @staticmethod
    def _estimate_gear(speed: float) -> int:
        if speed < 8:
            return 1
        if speed < 25:
            return 2
        if speed < 45:
            return 3
        if speed < 75:
            return 4
        return 5

    def generate_fuzzing_frames(self) -> List[Dict[str, Any]]:
        frame_count = random.randint(8, 20)
        frames = []
        for _ in range(frame_count):
            arbitration_id = f"0x{random.randint(0, 0x7FF):03X}"
            payload = {
                "rpm": random.randint(0, 9000),
                "speed": random.randint(0, 320),
                "brake_pressure": random.randint(0, 140),
                "coolant_temp": random.randint(-40, 180),
                "raw_bytes": [random.randint(0, 255) for _ in range(8)],
            }
            frames.append(self._base_frame(arbitration_id, payload, "fuzz"))
        return frames

    def generate_replay_frames(self) -> List[Dict[str, Any]]:
        if len(self.replay_buffer) < max(25, self.replay_buffer.maxlen // 5):
            return self.generate_normal_frames()

        frames = []
        replay_batch_size = random.randint(5, 10)
        buffered = list(self.replay_buffer)
        for _ in range(replay_batch_size):
            original = json.loads(json.dumps(buffered[self.replay_cursor % len(buffered)]))
            self.replay_cursor += 1
            original_sequence = original.get("sequence")
            original["timestamp"] = datetime.now(timezone.utc).isoformat()
            original["event_time_epoch_ms"] = time.time_ns() // 1_000_000
            original["simulator_send_time_ns"] = time.time_ns()
            original["sequence"] = self.tick
            original["attack_type_label"] = "replay"
            original["replayed_original_sequence"] = original_sequence
            frames.append(original)
        return frames

    def generate_injection_frames(self) -> List[Dict[str, Any]]:
        frames = self.generate_normal_frames()
        attack_choice = random.choice(["speed_spike", "brake_suppression", "rpm_spike", "thermal_spike"])

        if attack_choice == "speed_spike":
            frames.append(
                self._base_frame(
                    SPEED_ID,
                    {"speed": 250.0, "acceleration": 9.5, "odometer_km": round(self.state.odometer_km, 4)},
                    "injection",
                )
            )
        elif attack_choice == "brake_suppression":
            frames.append(
                self._base_frame(
                    BRAKE_ID,
                    {
                        "brake_pressure": 0.0,
                        "abs_active": False,
                        "wheel_speed_fl": round(max(80.0, self.state.speed), 1),
                        "wheel_speed_fr": round(max(80.0, self.state.speed), 1),
                    },
                    "injection",
                )
            )
        elif attack_choice == "rpm_spike":
            frames.append(
                self._base_frame(
                    ENGINE_ID,
                    {"rpm": 7800.0, "throttle_position": 100.0, "engine_load": 100.0, "gear": 1},
                    "injection",
                )
            )
        else:
            frames.append(
                self._base_frame(
                    COOLANT_ID,
                    {"coolant_temp": 155.0, "battery_voltage": 11.2, "ambient_temp": 24.0},
                    "injection",
                )
            )
        return frames

    def generate_dos_frames(self) -> List[Dict[str, Any]]:
        frame_count = random.randint(35, 70)
        return [
            self._base_frame(
                DOMINANT_DOS_ID,
                {
                    "rpm": 0,
                    "speed": 0,
                    "brake_pressure": 0,
                    "coolant_temp": 0,
                    "raw_bytes": [0, 0, 0, 0, 0, 0, 0, 0],
                },
                "dos",
            )
            for _ in range(frame_count)
        ]

    def frames_for_current_mode(self) -> Iterable[Dict[str, Any]]:
        if self.attack_mode == "normal":
            return self.generate_normal_frames()
        if self.attack_mode == "fuzz":
            return self.generate_fuzzing_frames()
        if self.attack_mode == "replay":
            return self.generate_replay_frames()
        if self.attack_mode == "injection":
            return self.generate_injection_frames()
        if self.attack_mode == "dos":
            return self.generate_dos_frames()
        raise AssertionError("attack mode validation failed")

    def publish(self, frame: Dict[str, Any]) -> None:
        key = f"{frame['vehicle_id']}:{frame['arbitration_id']}"
        future = self.producer.send(self.topic, key=key, value=frame)
        future.add_errback(self._log_publish_error)

    @staticmethod
    def _log_publish_error(exc: KafkaError) -> None:
        logging.error("Kafka publish failed: %s", exc)

    def run(self) -> None:
        logging.info(
            "Starting CAN simulator vehicle_id=%s topic=%s mode=%s rate=%.1fHz",
            self.vehicle_id,
            self.topic,
            self.attack_mode,
            self.rate_hz,
        )

        last_log_time = time.monotonic()
        frames_sent = 0
        total_frames_sent = 0

        while self.running:
            cycle_start = time.monotonic()
            for frame in self.frames_for_current_mode():
                self.publish(frame)
                frames_sent += 1
                total_frames_sent += 1
            self.tick += 1

            if time.monotonic() - last_log_time >= 5:
                self.producer.flush(timeout=2)
                logging.info("Published %d frames in last window; mode=%s", frames_sent, self.attack_mode)
                frames_sent = 0
                last_log_time = time.monotonic()

            elapsed = time.monotonic() - cycle_start
            time.sleep(max(0.0, self.frame_interval - elapsed))

        logging.info("Stopping simulator and flushing producer.")
        self.producer.flush(timeout=10)
        logging.info("Total frames published: %d; mode=%s", total_frames_sent, self.attack_mode)
        self.producer.close(timeout=10)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Stream simulated CAV CAN telematics to Kafka.")
    parser.add_argument("--bootstrap-servers", default=DEFAULT_BOOTSTRAP_SERVERS)
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    parser.add_argument("--attack-mode", choices=sorted(ATTACK_MODES), default="normal")
    parser.add_argument("--rate-hz", type=float, default=100.0)
    parser.add_argument("--replay-buffer-size", type=int, default=500)
    parser.add_argument("--client-id", default="cav-can-simulator")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

    simulator = CANTelematicsSimulator(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        attack_mode=args.attack_mode,
        rate_hz=args.rate_hz,
        replay_buffer_size=args.replay_buffer_size,
        producer_client_id=args.client_id,
    )
    signal.signal(signal.SIGINT, simulator.stop)
    signal.signal(signal.SIGTERM, simulator.stop)
    simulator.run()


if __name__ == "__main__":
    main()
