#!/usr/bin/env python3
"""
Evaluate anomaly detection quality from Elasticsearch records.

The simulator provides ground-truth labels in attack_type_label:
normal = benign, all other labels = attack.

The detector prediction is stored in is_anomaly:
true = anomaly, false = normal.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from elasticsearch import Elasticsearch


DEFAULT_ELASTICSEARCH_URL = os.getenv("ELASTICSEARCH_URL", "http://localhost:9200")
DEFAULT_INDEX = "can-security-alerts*"
ATTACK_TYPES = ["fuzz", "replay", "injection", "dos"]


def elasticsearch_config(url: str) -> Dict[str, Any]:
    config: Dict[str, Any] = {"hosts": url, "request_timeout": 30}
    elastic_user = os.getenv("ELASTIC_USER")
    elastic_password = os.getenv("ELASTIC_PASSWORD")
    if elastic_user and elastic_password:
        config["basic_auth"] = (elastic_user, elastic_password)

    ca_cert_path = os.getenv("ES_CA_CERT_PATH")
    if ca_cert_path and url.lower().startswith("https://"):
        config["ca_certs"] = ca_cert_path
        config["verify_certs"] = True
    elif url.lower().startswith("http://"):
        config["verify_certs"] = False

    return config


def count_documents(es: Elasticsearch, index: str, filters: Iterable[Dict[str, Any]]) -> int:
    query = {"bool": {"filter": list(filters)}}
    response = es.count(index=index, query=query)
    return int(response["count"])


def term_filter(field: str, value: Any) -> Dict[str, Any]:
    return {"term": {field: value}}


def range_filter(field: str, gte: Optional[str], lte: Optional[str]) -> List[Dict[str, Any]]:
    if not gte and not lte:
        return []
    bounds: Dict[str, str] = {}
    if gte:
        bounds["gte"] = gte
    if lte:
        bounds["lte"] = lte
    return [{"range": {field: bounds}}]


def safe_divide(numerator: int, denominator: int) -> float:
    return float(numerator / denominator) if denominator else 0.0


def metrics_from_counts(tp: int, tn: int, fp: int, fn: int) -> Dict[str, float]:
    precision = safe_divide(tp, tp + fp)
    recall = safe_divide(tp, tp + fn)
    f1 = safe_divide(2 * precision * recall, precision + recall) if precision + recall else 0.0
    false_positive_rate = safe_divide(fp, fp + tn)
    detection_rate = recall
    accuracy = safe_divide(tp + tn, tp + tn + fp + fn)
    return {
        "detection_rate": detection_rate,
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "false_positive_rate": false_positive_rate,
        "accuracy": accuracy,
    }


def evaluate(es: Elasticsearch, index: str, start_time: Optional[str], end_time: Optional[str]) -> Dict[str, Any]:
    time_filters = range_filter("timestamp", start_time, end_time)

    normal_total = count_documents(es, index, [*time_filters, term_filter("attack_type_label", "normal")])
    normal_anomalies = count_documents(
        es,
        index,
        [*time_filters, term_filter("attack_type_label", "normal"), term_filter("is_anomaly", True)],
    )
    normal_predicted_normal = count_documents(
        es,
        index,
        [*time_filters, term_filter("attack_type_label", "normal"), term_filter("is_anomaly", False)],
    )

    per_attack: Dict[str, Dict[str, Any]] = {}
    attack_tp_total = 0
    attack_fn_total = 0

    for attack_type in ATTACK_TYPES:
        attack_total = count_documents(es, index, [*time_filters, term_filter("attack_type_label", attack_type)])
        true_positive = count_documents(
            es,
            index,
            [*time_filters, term_filter("attack_type_label", attack_type), term_filter("is_anomaly", True)],
        )
        false_negative = count_documents(
            es,
            index,
            [*time_filters, term_filter("attack_type_label", attack_type), term_filter("is_anomaly", False)],
        )
        true_negative = normal_predicted_normal
        false_positive = normal_anomalies
        metrics = metrics_from_counts(true_positive, true_negative, false_positive, false_negative)
        per_attack[attack_type] = {
            "total": attack_total,
            "tp": true_positive,
            "tn": true_negative,
            "fp": false_positive,
            "fn": false_negative,
            **metrics,
        }
        attack_tp_total += true_positive
        attack_fn_total += false_negative

    overall_counts = {
        "tp": attack_tp_total,
        "tn": normal_predicted_normal,
        "fp": normal_anomalies,
        "fn": attack_fn_total,
    }
    overall = {**overall_counts, **metrics_from_counts(**overall_counts)}

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "index": index,
        "time_range": {"gte": start_time, "lte": end_time},
        "normal_total": normal_total,
        "overall": overall,
        "per_attack": per_attack,
    }


def write_json(report: Dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2), encoding="utf-8")


def write_csv(report: Dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "attack_type",
        "total",
        "tp",
        "tn",
        "fp",
        "fn",
        "detection_rate",
        "precision",
        "recall",
        "f1",
        "false_positive_rate",
        "accuracy",
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for attack_type, values in report["per_attack"].items():
            writer.writerow({"attack_type": attack_type, **values})
        writer.writerow({"attack_type": "overall", "total": "", **report["overall"]})


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Evaluate CAV anomaly detection metrics from Elasticsearch.")
    parser.add_argument("--elasticsearch-url", default=DEFAULT_ELASTICSEARCH_URL)
    parser.add_argument("--index", default=DEFAULT_INDEX)
    parser.add_argument("--start-time", help="Optional ISO-8601 lower timestamp bound for timestamp.")
    parser.add_argument("--end-time", help="Optional ISO-8601 upper timestamp bound for timestamp.")
    parser.add_argument("--json-out", default="reports/detection-evaluation.json")
    parser.add_argument("--csv-out", default="reports/detection-evaluation.csv")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    es = Elasticsearch(**elasticsearch_config(args.elasticsearch_url))
    if not es.ping():
        raise RuntimeError(f"Unable to connect to Elasticsearch at {args.elasticsearch_url}")

    report = evaluate(es, args.index, args.start_time, args.end_time)
    write_json(report, Path(args.json_out))
    write_csv(report, Path(args.csv_out))

    print(json.dumps(report["overall"], indent=2))
    print(f"Wrote JSON report to {args.json_out}")
    print(f"Wrote CSV report to {args.csv_out}")


if __name__ == "__main__":
    main()
