# InsightNode

A simplified observability platform built to learn system design, distributed systems, and telemetry pipelines. Inspired by Datadog — not a clone.

**Current stage: Phase 3 Day 3 — aggregate queries on ClickHouse**  
**Next: Phase 3 Day 4 — compare PostgreSQL vs ClickHouse at scale**

Phase 2 delivers a durable Kafka ingest bus. Phase 3 dual-writes to PostgreSQL + ClickHouse; `GET /metrics/aggregate` now reads from ClickHouse while raw `GET /metrics` stays on PostgreSQL.

---

## What it does

InsightNode collects host metrics (CPU, memory, disk) from a local agent, ingests them through a FastAPI backend, stores them in PostgreSQL, and exposes raw and aggregated query APIs.

```
Agent (psutil + spool)
    │  POST /metrics {event_id, ...}
    ▼
FastAPI (rate limit) ──produce──► Kafka ──workers──► PostgreSQL
  │ 202 / 429 / 503                 │            └─► ClickHouse
  ├── GET /metrics (PG)             └─ DLQ topic
  ├── GET /metrics/aggregate (CH)
  ├── GET /health          (+ clickhouse_ok)
  ├── GET /pipeline
  └── GET /dlq
```

### Features (Phase 1 + Phase 2)

| Component | Capability |
|-----------|------------|
| **Agent** | Collects CPU, memory, and disk gauges every 5 seconds |
| **Ingestion API** | Validates payloads, rate-limits, produces to Kafka |
| **Kafka bus** | Partitioned ingest + DLQ; idempotent producer |
| **Workers** | Standalone consumers; dual-write PG + ClickHouse; commit after both |
| **Query API** | Raw points (PG) + time-bucket aggregations (ClickHouse) |
| **Idempotency** | `event_id` unique index in PostgreSQL |
| **Ops** | `/health` (Kafka + ClickHouse), `/pipeline` (partition lag), `/dlq` |
| **Agent resilience** | Retries + on-disk spool |
| **ClickHouse (Day 3)** | Dual-write + `/metrics/aggregate` read path |

---

## Project structure

```
InsightNode/
├── agent/
│   ├── main.py
│   ├── spool.py
│   └── data/
├── backend/
│   ├── main.py              # FastAPI — ingest, query, pipeline, dlq
│   ├── worker.py            # Kafka consumer → PostgreSQL + ClickHouse
│   ├── kafka_client.py      # Phase 2 Day 5–6 Kafka helpers
│   ├── clickhouse_client.py # Phase 3 — connect, schema, insert_metrics
│   ├── rate_limit.py        # Phase 2 Day 6 ingest rate limit
│   ├── redis_client.py      # Phase 2 Days 1–4 (history)
│   ├── database.py
│   └── models.py
├── docs/
│   ├── architecture.md
│   ├── phase-1-graduation.md
│   ├── phase-2-architecture.md
│   ├── phase-2-graduation.md
│   ├── phase-3-architecture.md
│   └── ...
├── docker-compose.yml       # Redpanda (:9092) + ClickHouse (:8123)
├── sql/
│   ├── schema.sql           # PostgreSQL
│   └── clickhouse/
│       └── schema.sql       # ClickHouse MergeTree metrics
└── requirements.txt
```

---

## Prerequisites

- Python 3.11+
- PostgreSQL 14+

---

## Setup

### 1. Clone and install dependencies

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2. Create the database

```bash
createdb insightnode

# Or via psql:
# CREATE USER insightnode WITH PASSWORD 'insightnode';
# CREATE DATABASE insightnode OWNER insightnode;

psql -U insightnode -d insightnode -f sql/schema.sql
psql -U insightnode -d insightnode -f sql/migrations/001_add_event_id.sql
```

### 3. Configure database URL (optional)

Default connection string:

```
postgresql://insightnode:insightnode@localhost:5432/insightnode
```

Override with an environment variable:

```bash
export DATABASE_URL="postgresql://user:password@localhost:5432/insightnode"
```

---

## Running

Run from the **project root** (`InsightNode/`).

### Start Kafka + ClickHouse (Phase 2 / Phase 3 Day 1)

```bash
docker compose up -d
# Kafka API on localhost:9092
# ClickHouse HTTP on localhost:8123  (user/pass: insightnode / insightnode)
```

Install Python deps (includes `kafka-python` + `clickhouse-connect`):

```bash
pip install -r requirements.txt
```

### Start the API (accepts metrics only)

```bash
uvicorn backend.main:app --reload --port 8001
```

API docs: http://127.0.0.1:8001/docs

### Start one or more workers (Kafka consumer group)

```bash
# Terminal A
python -m backend.worker

# Terminal B (optional — scales ingest across partitions)
WORKER_NAME=worker-b python -m backend.worker
```

Workers share the Kafka consumer group `insightnode-ingest-workers`.

Optional: run a worker inside the API (dev only):

```bash
EMBEDDED_WORKER=1 uvicorn backend.main:app --reload --port 8001
```

### Start the agent

In another terminal:

```bash
cd agent
python main.py
```

Expected steady-state output:

```
[SENT] 202 → {'status': 'accepted', 'machine_id': '...', 'metric_count': 3, 'queued': 0}
```

Check pipeline health:

```bash
curl http://127.0.0.1:8001/health
# queue_backend: kafka, kafka_ok: true, clickhouse_ok: true, queue_size (lag) near 0

# Per-partition lag (Phase 2 Day 6)
curl http://127.0.0.1:8001/pipeline

# Inspect poison messages (DLQ topic)
curl "http://127.0.0.1:8001/dlq?limit=10"
```

> See [docs/phase-3-architecture.md](docs/phase-3-architecture.md) for ClickHouse (Days 1–3).
> See [docs/phase-2-architecture.md](docs/phase-2-architecture.md) and [docs/phase-2-graduation.md](docs/phase-2-graduation.md).
> Redis Streams code (`backend/redis_client.py`) remains as Days 1–4 learning history.

---

## API reference

### `POST /metrics` — Ingest metrics

Accepts a JSON payload and returns `202 Accepted` immediately. Persistence happens asynchronously via the background worker.

```json
{
  "event_id": "550e8400-e29b-41d4-a716-446655440000",
  "machine_id": "my-machine",
  "timestamp": "2026-06-19T06:56:00.843013+00:00",
  "metrics": [
    { "name": "cpu_usage", "value": 45.2, "unit": "percent" },
    { "name": "memory_usage", "value": 62.1, "unit": "percent" },
    { "name": "disk_usage", "value": 78.0, "unit": "percent" }
  ]
}
```

- Returns `503` if the ingest queue is full (backpressure)
- Duplicate `event_id` + `metric_name` for the same machine are ignored at storage (idempotent)

### `GET /metrics` — Query stored metrics (raw)

| Parameter | Type | Description |
|-----------|------|-------------|
| `machine_id` | string | Filter by host |
| `metric_name` | string | Filter by metric (e.g. `cpu_usage`) |
| `start_time` | ISO 8601 | Inclusive lower bound |
| `end_time` | ISO 8601 | Inclusive upper bound |
| `limit` | int | Max rows (default 100, max 1000) |

Example:

```bash
curl "http://127.0.0.1:8001/metrics?machine_id=my-machine&metric_name=cpu_usage&limit=10"
```

### `GET /metrics/aggregate` — Query aggregated buckets

Served from **ClickHouse** (Phase 3 Day 3). Raw `GET /metrics` remains on PostgreSQL.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `machine_id` | string | Yes | Filter by host |
| `metric_name` | string | Yes | Filter by metric |
| `start_time` | ISO 8601 | Yes | Range start |
| `end_time` | ISO 8601 | Yes | Range end (exclusive) |
| `interval` | string | No | `1m`, `5m`, `15m`, `1h`, `3h`, `6h`, `12h`, `24h`, `1d` (default `5m`) |

Example:

```bash
curl "http://127.0.0.1:8001/metrics/aggregate?machine_id=my-machine&metric_name=cpu_usage&start_time=2026-07-10T05:00:00%2B00:00&end_time=2026-07-10T06:00:00%2B00:00&interval=5m"
```

### `GET /health` — Pipeline health

```json
{
  "status": "ok",
  "queue_size": 0,
  "queue_maxsize": 10000,
  "worker_alive": true
}
```

---

## Database schema

Table: `metrics` (one row per metric sample)

| Column | Type | Description |
|--------|------|-------------|
| `id` | BIGSERIAL | Primary key |
| `machine_id` | VARCHAR(255) | Source host |
| `metric_name` | VARCHAR(255) | e.g. `cpu_usage` |
| `value` | DOUBLE PRECISION | Observed value |
| `unit` | VARCHAR(50) | e.g. `percent` |
| `timestamp` | TIMESTAMPTZ | Agent observation time (UTC) |
| `event_id` | UUID | Idempotency key (nullable for pre-Day-11 rows) |
| `created_at` | TIMESTAMPTZ | Server insert time |

Indexes:

- `idx_metrics_timestamp` — time-range scans
- `idx_metrics_machine_metric_time` — filtered queries by host + metric + time
- `idx_metrics_dedup` — unique `(machine_id, event_id, metric_name)` where `event_id IS NOT NULL`

See [docs/database-schema.md](docs/database-schema.md) for full details.

---

## Architecture

See [docs/architecture.md](docs/architecture.md) for system diagrams and design decisions.

### Ingestion flow

1. Agent samples OS metrics via `psutil` (5-second CPU window).
2. Agent assigns a unique `event_id` (UUID) per collection cycle.
3. Agent POSTs JSON to `/metrics`.
4. FastAPI validates the payload and enqueues it (non-blocking).
5. API responds `202` immediately.
6. Background worker dequeues payloads in batches and idempotently inserts into PostgreSQL.

### Failure handling

**Agent side**

- Retries failed POSTs up to 5 times with exponential backoff (1s, 2s, 4s, 8s).
- After all retries fail, appends the payload to `agent/data/spool.ndjson`.
- On each loop, replays buffered payloads before collecting new metrics.

**API side**

- If PostgreSQL is temporarily unavailable, the worker re-queues failed payloads (up to 3 attempts).
- If the in-memory queue is full, the API returns `503` so agents back off or spool.
- Duplicate payloads (same `event_id`) are silently skipped at insert.

See [docs/request-flows.md](docs/request-flows.md) for sequence diagrams.

---

## Known limitations (by design)

These intentional Phase 1 constraints motivate Phase 2+.

| Limitation | Impact |
|------------|--------|
| In-memory queue | Metrics in the queue are lost if the API process crashes |
| PostgreSQL for time-series | Slow at very high cardinality and volume; aggregation scans get expensive |
| Single API process | No horizontal scaling |
| Retry storm | Agent spends a long time retrying when API is down for extended periods |
| No retention policy | Table grows indefinitely |
| No percentiles (p95) | Deferred to a later phase |

See [docs/bottlenecks-and-roadmap.md](docs/bottlenecks-and-roadmap.md) for scale analysis.

---

## Roadmap

### Phase 2 (Day 13+) — Durable buffering

- Redis replaces in-memory queue
- Survives API restarts; foundation for horizontal scaling

### Later phases

| Phase | Focus |
|-------|-------|
| 3 | ClickHouse — Day 1 schema/health → Day 2 dual-write → Day 3 aggregate routing |
| 4 | OpenSearch — centralized log search |
| 5 | OpenTelemetry — distributed tracing |
| 6 | Sharding, multi-tenancy, rate limiting, usage metering |

---

## Learning goals (Phase 1)

- [x] Understand metrics vs gauges and push-based collection
- [x] Build a telemetry agent with structured payloads
- [x] Design an ingestion API with request validation
- [x] Store append-only time-series data in PostgreSQL
- [x] Query metrics by host, name, and time range
- [x] Decouple ingestion from storage (queue + worker)
- [x] Handle failures with retries, disk spool, and worker re-queue
- [x] Aggregate metrics into time buckets (Day 10)
- [x] Implement idempotency keys for at-least-once delivery (Day 11)
- [x] Document architecture, flows, schema, and bottlenecks (Day 12)

**Phase 1 graduation:** [docs/phase-1-graduation.md](docs/phase-1-graduation.md)

---

## License

Learning project — no license specified.
