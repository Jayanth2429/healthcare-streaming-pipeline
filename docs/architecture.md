# Architecture and Design Decisions

## Goal

Demonstrate a small but production-minded event-processing pipeline using fully synthetic healthcare-style data. The project favors correctness, observability, and recoverability over raw throughput.

## Flow

1. A Python producer creates normalized synthetic events.
2. Events are published to Redpanda using the Kafka protocol.
3. PySpark Structured Streaming parses each message with an explicit schema.
4. Records are classified as valid or invalid.
5. Valid records use event-time watermarking and `event_id` deduplication before being written to a curated Parquet layer.
6. Invalid records are retained in a quarantine JSON layer with the original payload for inspection.
7. Separate checkpoints preserve progress for curated and quarantine streams.

## Design decisions

### Deterministic event identifiers

The event identifier is derived from stable event attributes rather than generated randomly. Reprocessing the same logical event therefore produces the same identifier, which makes downstream idempotency easier to reason about.

### Event-time watermarking

Streaming systems receive late records. A watermark places a bounded state window around deduplication rather than keeping every identifier forever.

### Quarantine instead of silent filtering

Invalid records are written to a separate location instead of being discarded. This keeps the curated layer clean without losing evidence needed for troubleshooting.

### Checkpoints are separated by sink

Curated and quarantine outputs have independent checkpoint locations. Each streaming query can recover its own progress after a restart.

### Synthetic normalized events, not real HL7

The project intentionally uses a small normalized event contract. It demonstrates healthcare interoperability concepts without publishing employer schemas, proprietary mappings, or patient data.

## Production extensions

A production deployment could add:

- Schema Registry and explicit versioned contracts
- Delta Lake or Iceberg tables
- MERGE-based curated serving tables
- Dead-letter Kafka topic in addition to filesystem quarantine
- OpenTelemetry/Prometheus metrics and alerting
- Great Expectations or Deequ-style data-quality suites
- Terraform and managed cloud streaming infrastructure
- Secret management and workload identity
- Fine-grained access controls and PHI-safe logging
