"""Replay public MIMIC-IV demo hospital events through Kafka/Redpanda.

Historical records are converted to a normalized event contract and published
in event-time order. The source timestamps are preserved; the replay interval
controls only how quickly records are emitted to the broker.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import random
import time
from datetime import datetime
from pathlib import Path

from confluent_kafka import Producer

from src.event_model import HealthcareEvent
from src.identifiers import deterministic_event_id


def _clean(value: str | None) -> str:
    return (value or "").strip()


def _make_event(
    *,
    subject_id: str,
    hadm_id: str | None,
    event_type: str,
    event_timestamp: str,
    source_table: str,
    source_record_id: str,
    care_unit: str | None = None,
    payload: dict[str, str] | None = None,
) -> HealthcareEvent:
    return HealthcareEvent(
        event_id=deterministic_event_id(
            subject_id=subject_id,
            hadm_id=hadm_id,
            event_type=event_type,
            source_table=source_table,
            event_timestamp=event_timestamp,
            source_record_id=source_record_id,
        ),
        subject_id=subject_id,
        hadm_id=hadm_id or None,
        event_type=event_type,
        event_timestamp=event_timestamp,
        source_table=source_table,
        source_record_id=source_record_id,
        care_unit=care_unit or None,
        payload=payload or {},
    )


def load_admission_events(path: Path) -> list[HealthcareEvent]:
    events: list[HealthcareEvent] = []
    with gzip.open(path, "rt", encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            subject_id = _clean(row["subject_id"])
            hadm_id = _clean(row["hadm_id"])
            admittime = _clean(row["admittime"])
            dischtime = _clean(row["dischtime"])

            events.append(
                _make_event(
                    subject_id=subject_id,
                    hadm_id=hadm_id,
                    event_type="ADMISSION",
                    event_timestamp=admittime,
                    source_table="admissions",
                    source_record_id=f"{hadm_id}:admit",
                    payload={
                        "admission_type": _clean(row.get("admission_type")),
                        "admission_location": _clean(row.get("admission_location")),
                    },
                )
            )
            if dischtime:
                events.append(
                    _make_event(
                        subject_id=subject_id,
                        hadm_id=hadm_id,
                        event_type="DISCHARGE",
                        event_timestamp=dischtime,
                        source_table="admissions",
                        source_record_id=f"{hadm_id}:discharge",
                        payload={
                            "discharge_location": _clean(row.get("discharge_location")),
                            "hospital_expire_flag": _clean(row.get("hospital_expire_flag")),
                        },
                    )
                )
    return events


def load_transfer_events(path: Path) -> list[HealthcareEvent]:
    events: list[HealthcareEvent] = []
    with gzip.open(path, "rt", encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            intime = _clean(row.get("intime"))
            if not intime:
                continue
            transfer_id = _clean(row.get("transfer_id"))
            events.append(
                _make_event(
                    subject_id=_clean(row["subject_id"]),
                    hadm_id=_clean(row.get("hadm_id")) or None,
                    event_type="TRANSFER",
                    event_timestamp=intime,
                    source_table="transfers",
                    source_record_id=transfer_id or f"{row['subject_id']}:{intime}",
                    care_unit=_clean(row.get("careunit")) or None,
                    payload={
                        "transfer_event_type": _clean(row.get("eventtype")),
                        "outtime": _clean(row.get("outtime")),
                    },
                )
            )
    return events


def load_events(data_dir: Path) -> list[HealthcareEvent]:
    events = load_admission_events(data_dir / "admissions.csv.gz")
    events.extend(load_transfer_events(data_dir / "transfers.csv.gz"))
    return sorted(events, key=lambda event: event.event_timestamp)


def publish(producer: Producer, topic: str, event: HealthcareEvent) -> None:
    producer.produce(
        topic,
        key=event.event_id.encode("utf-8"),
        value=event.model_dump_json().encode("utf-8"),
    )
    producer.poll(0)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", default="data/raw/mimic")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", default="healthcare-events")
    parser.add_argument("--max-events", type=int, default=500)
    parser.add_argument("--interval-ms", type=int, default=20)
    parser.add_argument(
        "--duplicate-rate",
        type=float,
        default=0.0,
        help="Optional fault injection: republish a fraction of real source events to exercise deduplication.",
    )
    args = parser.parse_args()

    if not 0 <= args.duplicate_rate <= 1:
        raise ValueError("duplicate-rate must be between 0 and 1")

    events = load_events(Path(args.data_dir))[: args.max_events]
    producer = Producer({"bootstrap.servers": args.bootstrap_servers})
    published = duplicates = 0

    for event in events:
        publish(producer, args.topic, event)
        published += 1
        if args.duplicate_rate and random.random() < args.duplicate_rate:
            publish(producer, args.topic, event)
            published += 1
            duplicates += 1
        if args.interval_ms:
            time.sleep(args.interval_ms / 1000)

    producer.flush()
    print(f"Published {published} messages from {len(events)} public source events ({duplicates} injected duplicates).")


if __name__ == "__main__":
    main()
