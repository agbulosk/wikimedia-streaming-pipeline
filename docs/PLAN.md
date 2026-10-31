# Streaming Pipeline Plan: Wikimedia → Kafka → Parquet

A self-study project to learn streaming data engineering by building a real-time pipeline from Wikimedia's public EventStreams feed, packaged with `uv`.

## Goals

- Learn the core streaming concepts that batch pipelines never force you to deal with: offsets, checkpointing, delivery semantics, flushing, late data, backpressure.
- Learn Kafka fundamentals hands-on.
- Ship a clean, tested Python package in a public GitHub repo with documented design decisions.

## Architecture (v2)

```
┌──────────────────────┐
│ Wikimedia            │
│ EventStreams (SSE)   │
└──────────┬───────────┘
           │ internet
┌──────────▼───────────┐      ┌────────────── Docker ──────────────┐
│ Producer             │      │ ┌────────────────┐  ┌────────────┐ │
│ (Python + Pywikibot) ├─────►│ │ Kafka (KRaft)  │  │ Kafka UI   │ │
│ runs via uv          │      │ │ topic: raw     │  │ (optional) │ │
└──────────────────────┘      │ └───────┬────────┘  └────────────┘ │
                              │         │ named volume              │
                              └─────────┼───────────────────────────┘
┌──────────────────────┐                │
│ Consumer             │◄───────────────┘
│ (Python) runs via uv │
└──────────┬───────────┘
           │ micro-batches (flush)
┌──────────▼───────────┐      ┌──────────────────────┐
│ ./data/ Parquet,     ├─────►│ DuckDB (queries)     │
│ partitioned by date  │      └──────────────────────┘
└──────────────────────┘
```

### Components

| Component | Tool | Runs where | Role |
|---|---|---|---|
| Source | Wikimedia EventStreams (SSE) | Wikimedia servers | Live feed of edits and other changes |
| Producer | Python + Pywikibot + Kafka client | Host, via `uv run` | Reads the stream, writes events to a Kafka topic |
| Broker | Apache Kafka (KRaft mode, no ZooKeeper) | Docker | Durable buffer between ingest and processing |
| Kafka UI | Any Kafka web UI (optional) | Docker | Browse topics and messages while learning |
| Consumer | Python + Kafka client | Host, via `uv run` | Reads from Kafka, buffers, flushes Parquet files |
| Sink | Parquet files in `./data/` | Host disk | Final storage, partitioned by date |
| Query | DuckDB (Python library) | Host, in-process | Inspect and validate output |

### Storage notes

- Kafka topic data lives in a **named Docker volume** so it survives `docker compose down`. (Know what `-v` does before running it.)
- Parquet output is written directly to `./data/` on the host. **`data/` is in `.gitignore`.**
- Disk usage is bounded by: Kafka retention settings, deleting old date partitions, filtering early, keeping only needed fields, and not running 24/7.
- DuckDB is a library (`uv add duckdb`), not a server — no Docker needed.

## Roadmap

- **v1:** Wikimedia → consumer → local files (learn the stream mechanics, no broker)
- **v2:** Wikimedia → producer → Kafka → consumer → Parquet (this plan)
- **v3:** Wikimedia → producer → Kafka → Flink or Spark Structured Streaming → Iceberg/Delta table (industry stack)

## Milestones

| # | Milestone | Done when |
|---|---|---|
| 1 | Explore the source | I can describe the event schema, events/sec, and the event ID / resume mechanism without looking it up |
| 2 | Infra | `docker-compose.yml` runs Kafka in KRaft mode with a volume; I can create a topic and send/read a test message from my host |
| 3 | Producer | Pywikibot events land in the Kafka topic (verified in the UI) |
| 4 | Consumer | Parquet files appear in `./data/` via a flush policy; DuckDB can query them |
| 5 | Reliability | I can kill either process at random, restart it, and explain exactly what was lost or duplicated |
| 6 | Package | CLI commands for producer and consumer, config, tests using recorded events; clone → `docker compose up` → `uv run` works |
| 7 | README | Diagram, design decisions with reasoning, known limitations |

## Open design decisions

Answer each with reasoning before writing the code for it.

1. **Topic design:** How many partitions? What is the message key, and what ordering guarantee does it give?
2. **Filtering:** Filter in the producer (e.g. by wiki, bot vs. human), or send everything raw and filter downstream? What do I lose with each?
3. **Flush policy:** What triggers a Parquet write — count, time, size, or a combination? What happens at 3 a.m. when traffic is low?
4. **Disk partitioning:** By date or hour? Event time or arrival time? What happens to late events?
5. **Offset commits:** Commit before or after the Parquet file is durably written? Walk through a crash in each case.
6. **Duplicates:** If events are replayed after a crash, how do I detect or prevent duplicates?
7. **Delivery semantics:** At-most-once, at-least-once, or exactly-once — which can I realistically achieve, and where?
8. **Graceful shutdown:** What must happen on Ctrl+C so nothing in memory is lost?
9. **Bad data:** Drop, crash, or dead-letter events that fail validation?
10. **Retention:** What Kafka retention fits my laptop, based on measured events/sec × event size?
11. **Pywikibot setup:** What config does it need just for EventStreams, and how do I keep that clean in the package?
12. **Querying:** Should DuckDB read the Parquet files directly or load them into a `.duckdb` file? (Consider: one writer per database file.)

## Package design checklist

- Source, transform, and sink behind interfaces so any one can be swapped (e.g. Pywikibot → raw SSE, Parquet → Iceberg).
- Recorded sample of real events used as a replay source in tests — no live network in unit tests.
- Configuration via env vars / config file / CLI flags (stream, topic, flush thresholds, output path, filters).
- Separate CLI entry points for producer and consumer.
- Logging and basic metrics: events/sec, consumer lag, errors, last committed offset.
- Identify the client properly when calling Wikimedia (check their API etiquette guidelines).

## Concepts to be able to explain

Offsets · consumer groups · partitions and keys · producer acks and idempotence · retention · flushing and buffering · the small-files problem · event time vs. processing time · windowing · watermarks and late data · delivery semantics · idempotent writes · backpressure · schema evolution
