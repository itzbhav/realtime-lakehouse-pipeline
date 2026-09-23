"""
producer.py — simulates high-velocity respiratory monitoring events and
publishes them to the Kafka topic `respiratory_events`.

A configurable fraction of records are deliberately corrupted (missing
field / wrong-type / invalid category / garbled JSON) so that
stream_ingest.py has real bad data to route to the dead-letter sink.
"""

import argparse
import json
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer

DIAGNOSES = ["COPD", "Asthma", "Pneumonia", "Bronchitis", "COVID-19", "Normal"]
PROBE_TYPES = ["SpO2", "Spirometer", "Capnograph", "Nasal Cannula"]
SEVERITIES = ["Low", "Medium", "High", "Critical"]


def make_valid_record() -> dict:
    return {
        "event_id": str(uuid.uuid4()),
        "patient_id": f"P-{random.randint(1, 9999):04d}",
        "diagnosis": random.choice(DIAGNOSES),
        "probe_type": random.choice(PROBE_TYPES),
        "severity": random.choice(SEVERITIES),
        "event_ts": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
    }


def corrupt_record(record: dict):
    """Return (payload, corruption_label). payload may be a dict or a raw string."""
    kind = random.choice(
        ["missing_field", "bad_severity", "bad_ts", "bad_diagnosis", "garbled_json"]
    )

    if kind == "missing_field":
        field = random.choice(["event_id", "patient_id", "diagnosis", "probe_type", "severity", "event_ts"])
        bad = dict(record)
        del bad[field]
        return bad, f"missing_field:{field}"

    if kind == "bad_severity":
        bad = dict(record)
        bad["severity"] = random.choice([99, "Extreme", "Mild"])
        return bad, "invalid_severity"

    if kind == "bad_ts":
        bad = dict(record)
        bad["event_ts"] = random.choice(["not-a-timestamp", "yesterday", "13:99:99"])
        return bad, "invalid_event_ts"

    if kind == "bad_diagnosis":
        bad = dict(record)
        bad["diagnosis"] = random.choice(["Flu", "Unknown", "N/A"])
        return bad, "invalid_diagnosis"

    # garbled_json: not valid JSON at all
    return "{not valid json, oops", "garbled_json"


def main():
    parser = argparse.ArgumentParser(description="Simulated respiratory event producer")
    parser.add_argument("--bootstrap", default="localhost:9092", help="Kafka bootstrap servers")
    parser.add_argument("--topic", default="respiratory_events", help="Kafka topic")
    parser.add_argument("--rate", type=float, default=5.0, help="records per second")
    parser.add_argument("--malformed-rate", type=float, default=0.05, help="fraction of records intentionally corrupted (0-1)")
    parser.add_argument("--count", type=int, default=0, help="stop after N records (0 = run forever)")
    args = parser.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        value_serializer=lambda v: v.encode("utf-8"),
    )

    delay = 1.0 / args.rate if args.rate > 0 else 0
    sent = 0
    valid_sent = 0
    bad_sent = 0

    print(f"Producing to topic '{args.topic}' @ {args.rate}/s, malformed rate {args.malformed_rate:.0%}. Ctrl+C to stop.")

    try:
        while args.count == 0 or sent < args.count:
            record = make_valid_record()
            if random.random() < args.malformed_rate:
                payload, label = corrupt_record(record)
                bad_sent += 1
                tag = f"[MALFORMED:{label}]"
            else:
                payload, label = record, None
                valid_sent += 1
                tag = "[ok]"

            if isinstance(payload, dict):
                value = json.dumps(payload)
            else:
                value = payload  # already a raw (garbled) string

            producer.send(args.topic, value=value)
            sent += 1

            if sent % 20 == 0 or label is not None:
                print(f"{tag} sent={sent} valid={valid_sent} malformed={bad_sent} :: {value}")

            if delay:
                time.sleep(delay)
    except KeyboardInterrupt:
        print("\nStopping producer.")
    finally:
        producer.flush()
        producer.close()
        print(f"Total sent={sent} valid={valid_sent} malformed={bad_sent}")


if __name__ == "__main__":
    main()
