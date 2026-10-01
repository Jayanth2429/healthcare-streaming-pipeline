# Architecture

The project turns historical rows from the public MIMIC-IV Clinical Database Demo into a controlled streaming workload.

1. `scripts/download_mimic_demo.py` downloads `admissions.csv.gz` and `transfers.csv.gz` from PhysioNet.
2. `src/replay_mimic_events.py` normalizes admission, discharge, and transfer rows into one event contract and sorts them by source event time.
3. The replay publishes those normalized events through Redpanda's Kafka-compatible API.
4. PySpark Structured Streaming parses a fixed schema, separates valid and invalid records, applies an event-time watermark, and deduplicates by deterministic `event_id`.
5. Valid records are written to curated Parquet; invalid records are written to quarantine JSON. Each sink has an independent checkpoint.

The source is intentionally treated as a **historical replay**. MIMIC itself is not presented as a native Kafka feed.

## Reliability boundary

The main reliability boundary is after Kafka ingestion. The pipeline keeps the original raw JSON long enough to quarantine malformed records rather than silently dropping them. Deterministic identifiers make intentionally replayed duplicates safe to recognize.
