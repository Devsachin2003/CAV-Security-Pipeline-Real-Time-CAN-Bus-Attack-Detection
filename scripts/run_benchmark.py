#!/usr/bin/env python3
"""
Run a local end-to-end benchmark for the CAV Security Pipeline.

The runner expects Docker Compose services to already be running and uses the
same .env-driven SASL_SSL Kafka and authenticated Elasticsearch settings as the
productionized clients.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

from elasticsearch import Elasticsearch
from kafka import KafkaConsumer, TopicPartition
from kafka.admin import KafkaAdminClient
from kafka.errors import KafkaError, NoBrokersAvailable


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ENV_FILE = ROOT / ".env"
REPORTS_DIR = ROOT / "reports"
DEFAULT_TOPIC = "can-telematics"
ATTACK_TYPES = ("fuzz", "replay", "injection", "dos")


def load_env_file(path: Path) -> Dict[str, str]:
    values: Dict[str, str] = {}
    if not path.exists():
        raise FileNotFoundError(f"Missing env file: {path}")

    for line in path.read_text(encoding="utf-8").splitlines():
        text = line.strip()
        if not text or text.startswith("#") or "=" not in text:
            continue
        key, value = text.split("=", 1)
        values[key.strip()] = value.strip().strip('"').strip("'")

    os.environ.update({key: value for key, value in values.items() if key not in os.environ})
    return values


def kafka_config() -> Dict[str, str]:
    required = ["KAFKA_BOOTSTRAP_SERVERS", "KAFKA_USER", "KAFKA_PASSWORD"]
    missing = [key for key in required if not os.getenv(key)]
    if missing:
        raise RuntimeError(f"Missing Kafka environment variables: {', '.join(missing)}")

    config = {
        "bootstrap_servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        "security_protocol": "SASL_SSL",
        "sasl_mechanism": "PLAIN",
        "sasl_plain_username": os.environ["KAFKA_USER"],
        "sasl_plain_password": os.environ["KAFKA_PASSWORD"],
        "request_timeout_ms": 15000,
        "api_version_auto_timeout_ms": 15000,
    }
    ca_cert_path = os.getenv("KAFKA_CA_CERT_PATH")
    if ca_cert_path:
        config["ssl_cafile"] = str((ROOT / ca_cert_path).resolve() if not Path(ca_cert_path).is_absolute() else ca_cert_path)
    return config


def elasticsearch_config() -> Dict[str, object]:
    url = os.getenv("ELASTICSEARCH_URL", "http://localhost:9200")
    config: Dict[str, object] = {"hosts": url, "request_timeout": 30}
    user = os.getenv("ELASTIC_USER")
    password = os.getenv("ELASTIC_PASSWORD")
    if user and password:
        config["basic_auth"] = (user, password)

    ca_cert_path = os.getenv("ES_CA_CERT_PATH")
    if ca_cert_path and url.lower().startswith("https://"):
        config["ca_certs"] = str((ROOT / ca_cert_path).resolve() if not Path(ca_cert_path).is_absolute() else ca_cert_path)
        config["verify_certs"] = True
    elif url.lower().startswith("http://"):
        config["verify_certs"] = False
    return config


def run_checked(command: List[str], *, env: Optional[Dict[str, str]] = None) -> subprocess.CompletedProcess[str]:
    print(f"$ {' '.join(command)}", flush=True)
    return subprocess.run(
        command,
        cwd=ROOT,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=True,
    )


def verify_docker_services() -> None:
    result = run_checked(["docker", "compose", "ps"])
    print(result.stdout.strip(), flush=True)


def verify_kafka(topic: str, timeout_seconds: int = 60, retry_interval_seconds: int = 3) -> None:
    deadline = time.monotonic() + timeout_seconds
    last_error: Optional[BaseException] = None

    while time.monotonic() < deadline:
        admin: Optional[KafkaAdminClient] = None
        try:
            admin = KafkaAdminClient(**kafka_config())
            topics = set(admin.list_topics())
            if topic not in topics:
                raise RuntimeError(
                    f"Kafka is reachable, but topic '{topic}' was not found. Available topics: {sorted(topics)}"
                )
            print(f"Kafka healthy: topic '{topic}' is available.", flush=True)
            return
        except (NoBrokersAvailable, KafkaError, OSError, ConnectionError) as exc:
            last_error = exc
            remaining = max(0, int(deadline - time.monotonic()))
            print(
                f"Kafka not ready yet ({exc.__class__.__name__}: {exc}); "
                f"retrying in {retry_interval_seconds}s, {remaining}s remaining.",
                flush=True,
            )
            time.sleep(retry_interval_seconds)
        finally:
            if admin is not None:
                admin.close()

    raise RuntimeError(f"Kafka did not become ready within {timeout_seconds}s") from last_error


def verify_elasticsearch() -> None:
    es = Elasticsearch(**elasticsearch_config())
    try:
        if not es.ping():
            raise RuntimeError("Elasticsearch ping failed.")
        health = es.cluster.health()
    finally:
        es.close()
    print(f"Elasticsearch healthy: cluster status={health.get('status')}", flush=True)


def clean_index(index_name: str) -> None:
    es = Elasticsearch(**elasticsearch_config())
    try:
        if es.indices.exists(index=index_name):
            es.indices.delete(index=index_name)
            print(f"Deleted previous benchmark index: {index_name}", flush=True)
    finally:
        es.close()


def start_detector(
    index_name: str,
    warmup_samples: int,
    consumer_group: str,
    env: Dict[str, str],
    log_path: Path,
) -> subprocess.Popen[str]:
    command = [
        sys.executable,
        "detection/ml_detector.py",
        "--warmup-samples",
        str(warmup_samples),
        "--consumer-group",
        consumer_group,
        "--auto-offset-reset",
        "latest",
        "--index-name",
        index_name,
    ]
    log_handle = log_path.open("w", encoding="utf-8")
    print(f"Starting detector; logs: {log_path}", flush=True)
    return subprocess.Popen(
        command,
        cwd=ROOT,
        env=env,
        stdout=log_handle,
        stderr=subprocess.STDOUT,
        text=True,
    )


def parse_published_count(log_path: Path) -> int:
    if not log_path.exists():
        return 0
    for line in reversed(log_path.read_text(encoding="utf-8", errors="replace").splitlines()):
        marker = "Total frames published:"
        if marker in line:
            return int(line.split(marker, 1)[1].split(";", 1)[0].strip())
    return 0


def run_simulator(mode: str, rate_hz: float, duration_seconds: int, env: Dict[str, str], log_path: Path) -> int:
    command = [
        sys.executable,
        "simulator/can_simulator.py",
        "--attack-mode",
        mode,
        "--rate-hz",
        str(rate_hz),
        "--client-id",
        f"cav-simulator-{mode}-{int(time.time())}",
    ]
    print(f"Running simulator mode={mode} rate={rate_hz}Hz duration={duration_seconds}s", flush=True)
    with log_path.open("w", encoding="utf-8") as log_handle:
        process = subprocess.Popen(
            command,
            cwd=ROOT,
            env=env,
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            text=True,
        )
        try:
            time.sleep(duration_seconds)
        finally:
            terminate_process(process)
    published_count = parse_published_count(log_path)
    print(f"Simulator mode={mode} published {published_count} frames.", flush=True)
    if published_count <= 0:
        raise RuntimeError(f"Simulator mode={mode} did not report any published frames. See {log_path}")
    return published_count


def terminate_process(process: subprocess.Popen[str], timeout_seconds: int = 10) -> None:
    if process.poll() is not None:
        return
    process.send_signal(signal.SIGTERM)
    try:
        process.wait(timeout=timeout_seconds)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=timeout_seconds)


def run_evaluation(index_name: str, json_out: Path, csv_out: Path, env: Dict[str, str]) -> Dict[str, object]:
    result = run_checked(
        [
            sys.executable,
            "evaluation/evaluate_detection.py",
            "--index",
            index_name,
            "--json-out",
            str(json_out),
            "--csv-out",
            str(csv_out),
        ],
        env=env,
    )
    print(result.stdout.strip(), flush=True)
    return json.loads(json_out.read_text(encoding="utf-8"))


def consumer_lag(topic: str, consumer_group: str) -> Tuple[int, Dict[str, int]]:
    consumer = KafkaConsumer(
        group_id=consumer_group,
        enable_auto_commit=False,
        **kafka_config(),
    )
    try:
        partitions = consumer.partitions_for_topic(topic)
        if not partitions:
            raise RuntimeError(f"Kafka topic '{topic}' has no partitions visible to the lag checker.")

        topic_partitions = [TopicPartition(topic, partition) for partition in sorted(partitions)]
        end_offsets = consumer.end_offsets(topic_partitions)
        lag_by_partition: Dict[str, int] = {}
        total_lag = 0
        for topic_partition in topic_partitions:
            committed = consumer.committed(topic_partition)
            if committed is None:
                committed = 0
            lag = max(0, int(end_offsets[topic_partition]) - int(committed))
            key = f"{topic_partition.topic}-{topic_partition.partition}"
            lag_by_partition[key] = lag
            total_lag += lag
        return total_lag, lag_by_partition
    finally:
        consumer.close()


def label_counts(index_name: str) -> Dict[str, int]:
    es = Elasticsearch(**elasticsearch_config())
    try:
        es.indices.refresh(index=index_name)
        response = es.search(
            index=index_name,
            size=0,
            aggs={"labels": {"terms": {"field": "attack_type_label", "size": 20}}},
        )
        return {
            bucket["key"]: int(bucket["doc_count"])
            for bucket in response["aggregations"]["labels"]["buckets"]
        }
    finally:
        es.close()


def wait_for_benchmark_drain(
    topic: str,
    consumer_group: str,
    index_name: str,
    expected_total: int,
    timeout_seconds: int,
    poll_interval_seconds: int,
) -> Dict[str, int]:
    deadline = time.monotonic() + timeout_seconds
    min_indexed_total = int(expected_total * 0.98)
    last_status = ""

    while time.monotonic() < deadline:
        lag, lag_by_partition = consumer_lag(topic, consumer_group)
        counts = label_counts(index_name)
        indexed_total = sum(counts.values())
        missing_labels = [label for label in ("normal", *ATTACK_TYPES) if counts.get(label, 0) <= 0]
        status = (
            f"lag={lag} indexed={indexed_total}/{expected_total} "
            f"missing_labels={missing_labels} partitions={lag_by_partition}"
        )
        if status != last_status:
            print(f"Benchmark drain check: {status}", flush=True)
            last_status = status

        if lag == 0 and indexed_total >= min_indexed_total and not missing_labels:
            return counts
        time.sleep(poll_interval_seconds)

    raise RuntimeError(f"Benchmark did not drain before evaluation. Last status: {last_status}")


def validate_report(report: Dict[str, object], label_count_map: Dict[str, int], expected_total: int) -> None:
    indexed_total = sum(label_count_map.values())
    lower_bound = int(expected_total * 0.98)
    upper_bound = int(expected_total * 1.02)
    if not lower_bound <= indexed_total <= upper_bound:
        raise RuntimeError(
            f"Indexed document count {indexed_total} is outside expected range "
            f"{lower_bound}..{upper_bound} from {expected_total} produced frames."
        )

    missing = [label for label in ("normal", *ATTACK_TYPES) if label_count_map.get(label, 0) <= 0]
    if missing:
        raise RuntimeError(f"Benchmark validation failed; missing Elasticsearch labels: {missing}")

    per_attack = report["per_attack"]
    zero_total = [label for label in ATTACK_TYPES if per_attack[label]["total"] <= 0]
    if zero_total:
        raise RuntimeError(f"Evaluation report has zero totals for attack classes: {zero_total}")


def print_metrics(report: Dict[str, object]) -> None:
    overall = report["overall"]
    per_attack = report["per_attack"]
    print("\nBenchmark Metrics", flush=True)
    print("=================", flush=True)
    for key in ("precision", "recall", "f1", "false_positive_rate", "accuracy"):
        print(f"overall {key}: {overall[key]:.4%}", flush=True)
    print("", flush=True)
    for attack_type, values in per_attack.items():
        print(
            f"{attack_type:10s} recall={values['recall']:.4%} "
            f"precision={values['precision']:.4%} total={values['total']}",
            flush=True,
        )


def benchmark_modes(args: argparse.Namespace) -> Iterable[tuple[str, float, int]]:
    yield ("normal", args.normal_rate_hz, args.normal_seconds)
    yield ("fuzz", args.attack_rate_hz, args.attack_seconds)
    yield ("replay", args.replay_rate_hz, args.attack_seconds)
    yield ("injection", args.attack_rate_hz, args.attack_seconds)
    yield ("dos", args.dos_rate_hz, args.attack_seconds)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run an end-to-end secured CAV pipeline benchmark.")
    parser.add_argument("--env-file", type=Path, default=DEFAULT_ENV_FILE)
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    parser.add_argument("--index-name", default=f"can-security-alerts-benchmark-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}")
    parser.add_argument("--warmup-samples", type=int, default=1000)
    parser.add_argument("--detector-startup-seconds", type=int, default=8)
    parser.add_argument("--normal-seconds", type=int, default=20)
    parser.add_argument("--attack-seconds", type=int, default=12)
    parser.add_argument("--normal-rate-hz", type=float, default=80.0)
    parser.add_argument("--attack-rate-hz", type=float, default=40.0)
    parser.add_argument("--replay-rate-hz", type=float, default=40.0)
    parser.add_argument("--dos-rate-hz", type=float, default=10.0)
    parser.add_argument("--drain-timeout-seconds", type=int, default=180)
    parser.add_argument("--drain-poll-seconds", type=int, default=3)
    parser.add_argument("--skip-docker-ps", action="store_true")
    parser.add_argument("--keep-index", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    load_env_file(args.env_file)
    env = os.environ.copy()
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    run_id = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    consumer_group = f"cav-security-benchmark-{run_id}"
    detector_log = REPORTS_DIR / f"benchmark-detector-{run_id}.log"
    json_out = REPORTS_DIR / f"benchmark-evaluation-{run_id}.json"
    csv_out = REPORTS_DIR / f"benchmark-evaluation-{run_id}.csv"

    if not args.skip_docker_ps:
        verify_docker_services()
    verify_kafka(args.topic)
    verify_elasticsearch()
    if not args.keep_index:
        clean_index(args.index_name)

    detector = start_detector(args.index_name, args.warmup_samples, consumer_group, env, detector_log)
    try:
        time.sleep(args.detector_startup_seconds)
        produced_counts: Dict[str, int] = {}
        for mode, rate_hz, duration in benchmark_modes(args):
            log_path = REPORTS_DIR / f"benchmark-simulator-{mode}-{run_id}.log"
            produced_counts[mode] = run_simulator(mode, rate_hz, duration, env, log_path)
        expected_total = sum(produced_counts.values())
        print(f"Produced frame counts: {produced_counts}; total={expected_total}", flush=True)
        counts = wait_for_benchmark_drain(
            args.topic,
            consumer_group,
            args.index_name,
            expected_total,
            args.drain_timeout_seconds,
            args.drain_poll_seconds,
        )
        print(f"Validated Elasticsearch label counts before evaluation: {counts}", flush=True)
        report = run_evaluation(args.index_name, json_out, csv_out, env)
        validate_report(report, counts, expected_total)
        print_metrics(report)
    finally:
        terminate_process(detector)

    print(f"\nWrote benchmark JSON: {json_out}", flush=True)
    print(f"Wrote benchmark CSV:  {csv_out}", flush=True)
    print(f"Wrote detector log:   {detector_log}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
