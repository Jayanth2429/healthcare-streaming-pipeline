from __future__ import annotations

import argparse
import random
import time
from datetime import datetime, timezone

from confluent_kafka import Producer
from faker import Faker

from src.event_model import HealthcareEvent
from src.identifiers import deterministic_event_id

fake = Faker()
EVENT_TYPES = ["ADT", "ORU", "MDM"]
VISIT_TYPES = ["ER", "Inpatient", "Outpatient", "Urgent Care"]
STATUSES = ["registered", "admitted", "observed", "discharged"]



def build_event() -> HealthcareEvent:
    patient_id = f"pt_{fake.random_int(min=1000, max=9999)}"
    event_type = random.choice(EVENT_TYPES)
    facility_id = f"facility_{fake.random_int(min=1, max=10):02d}"
    timestamp = datetime.now(timezone.utc).isoformat()

    return HealthcareEvent(
        event_id=deterministic_event_id(
            patient_id=patient_id,
            event_type=event_type,
            facility_id=facility_id,
            timestamp=timestamp,
        ),
        patient_id=patient_id,
        event_type=event_type,
        facility_id=facility_id,
        event_timestamp=timestamp,
        source_system="synthetic_ehr",
        payload={
            "visit_type": random.choice(VISIT_TYPES),
            "status": random.choice(STATUSES),
        },
    )


def publish_event(producer: Producer, topic: str, event: HealthcareEvent) -> None:
    producer.produce(
        topic,
        key=event.event_id.encode("utf-8"),
        value=event.model_dump_json().encode("utf-8"),
    )
    producer.poll(0)


def main(
    count: int,
    bootstrap_servers: str,
    topic: str,
    duplicate_rate: float,
) -> None:
    if not 0 <= duplicate_rate <= 1:
        raise ValueError("duplicate_rate must be between 0 and 1")

    producer = Producer({"bootstrap.servers": bootstrap_servers})
    last_event: HealthcareEvent | None = None
    duplicate_count = 0

    for _ in range(count):
        should_duplicate = last_event is not None and random.random() < duplicate_rate
        event = last_event if should_duplicate else build_event()

        if should_duplicate:
            duplicate_count += 1

        publish_event(producer, topic, event)
        last_event = event
        time.sleep(0.02)

    producer.flush()
    print(
        f"Published {count} events to {topic} "
        f"({duplicate_count} intentional duplicates)"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Publish synthetic healthcare-style events to Kafka/Redpanda."
    )
    parser.add_argument("--count", type=int, default=100)
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", default="healthcare-events")
    parser.add_argument(
        "--duplicate-rate",
        type=float,
        default=0.10,
        help="Fraction of events intentionally repeated to demonstrate deduplication.",
    )
    args = parser.parse_args()
    main(args.count, args.bootstrap_servers, args.topic, args.duplicate_rate)
