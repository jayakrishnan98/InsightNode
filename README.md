# InsightNode

A simplified observability platform built to learn system design, distributed systems, and telemetry pipelines. Inspired by Datadog — not a clone.

**Current stage: Phase 6 Day 2 — tenant storage isolation**  
**Next: Phase 6 Day 3 — per-tenant rate limits**

Phase 6 Day 2 persists `tenant_id` in PostgreSQL, ClickHouse, and OpenSearch, and scopes all query APIs to the authenticated tenant.

---

## What it does

InsightNode collects host metrics and structured logs from a local agent, ingests them through a FastAPI backend, stores metrics in PostgreSQL + ClickHouse, indexes logs in OpenSearch, and exposes query/search APIs.

```
Agent (psutil + spool + logship + OTEL)
    │  POST /metrics {event_id, …}  (+ traceparent)
    │  POST /logs    {structured events}
    ▼
FastAPI (rate limit, HTTP spans) ──produce──► Kafka ──workers──► PostgreSQL
  │ 202 / 429 / 503                    │  headers     └─► ClickHouse
  ├── GET /metrics (PG)                └─ DLQ         (+ dual_write spans)
  ├── GET /metrics/aggregate (CH)
  ├── GET /metrics/aggregate/compare
  ├── POST /logs / GET /logs/search / GET /logs/{id}  (OpenSearch)
  ├── GET /health          (+ clickhouse_ok, opensearch_ok, jaeger_ok)
  ├── GET /pipeline
  └── GET /dlq
         │
         └── OTLP :4318 ──► Jaeger UI :16686
```

### Features (Phase 1–5)

| Component | Capability |
|-----------|------------|
| **Agent** | Collects CPU, memory, and disk gauges every 5 seconds |
| **Ingestion API** | Validates payloads, rate-limits, produces to Kafka |
| **Kafka bus** | Partitioned ingest + DLQ; idempotent producer |
| **Workers** | Standalone consumers; dual-write PG + ClickHouse; commit after both |
| **Query API** | Raw points (PG) + aggregations (CH) + timed compare |
| **Idempotency** | `event_id` unique index in PostgreSQL |
| **Ops** | `/health` (Kafka + CH + OS + Jaeger), `/pipeline`, `/dlq` |
| **Agent resilience** | Retries + on-disk spool |
| **ClickHouse** | Columnar analytics store (Phase 3 complete) |
| **OpenSearch** | Centralized logs — ingest, search, shipping (Phase 4 complete) |
| **Jaeger / OTEL** | Linked ingest traces; dual-write + logship spans; log `attrs.trace_id` |
| **Tenancy (Day 2)** | `tenant_id` on PG/CH/OS; queries scoped by `X-API-Key` |

---

## Project structure

```
InsightNode/
├── agent/
│   ├── main.py
│   ├── spool.py
│   ├── tracing.py           # Phase 5 — CLIENT spans + HTTP inject
│   ├── logship.py           # Phase 4/5 — POST /logs (+ trace_id attrs)
│   └── data/
├── backend/
│   ├── main.py              # FastAPI — ingest, query, pipeline, dlq
│   ├── worker.py            # Kafka consumer → PostgreSQL + ClickHouse
│   ├── kafka_client.py      # Phase 2 Day 5–6 Kafka helpers (+ Day 3 headers)
│   ├── clickhouse_client.py # Phase 3 — connect, insert, aggregate
│   ├── opensearch_client.py # Phase 4 — index, get, search
│   ├── tracing.py           # Phase 5 — OTEL, FastAPI, Kafka, manual spans
│   ├── tenancy.py           # Phase 6 Day 1 — tenants + X-API-Key resolve
│   ├── logship.py           # Phase 4/5 — API/worker → OpenSearch (+ spans)
│   ├── postgres_aggregate.py# Phase 3 Day 4 — PG aggregate for compare
│   ├── rate_limit.py        # Phase 2 Day 6 ingest rate limit
│   ├── redis_client.py      # Phase 2 Days 1–4 (history)
│   ├── database.py
│   └── models.py
├── docs/
│   ├── phase-1-architecture.md
│   ├── phase-1-graduation.md
│   ├── phase-2-architecture.md
│   ├── phase-2-graduation.md
│   ├── phase-3-architecture.md
│   ├── phase-3-graduation.md
│   ├── phase-4-architecture.md
│   ├── phase-4-graduation.md
│   ├── phase-5-architecture.md
│   ├── phase-5-graduation.md
│   ├── phase-6-architecture.md
│   └── bottlenecks-and-roadmap.md
├── docker-compose.yml       # Redpanda + ClickHouse + OpenSearch + Jaeger
├── opensearch/
│   └── logs_index.json      # insightnode-logs mapping
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

### Start Kafka + ClickHouse + OpenSearch + Jaeger

```bash
docker compose up -d
# Kafka API on localhost:9092
# ClickHouse HTTP on localhost:8123  (user/pass: insightnode / insightnode)
# OpenSearch HTTP on localhost:9200  (security disabled for local learning)
# Jaeger UI on localhost:16686  (OTLP HTTP :4318)
```

Install Python deps (includes Kafka, ClickHouse, OpenSearch, OpenTelemetry):

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
# kafka_ok / clickhouse_ok / opensearch_ok / jaeger_ok: true

# Per-partition lag (Phase 2 Day 6)
curl http://127.0.0.1:8001/pipeline

# Inspect poison messages (DLQ topic)
curl "http://127.0.0.1:8001/dlq?limit=10"
```

> See [docs/phase-6-architecture.md](docs/phase-6-architecture.md) for multi-tenancy (Day 1+).
> See [docs/phase-5-architecture.md](docs/phase-5-architecture.md) and [docs/phase-5-graduation.md](docs/phase-5-graduation.md).
> See [docs/phase-4-architecture.md](docs/phase-4-architecture.md) and [docs/phase-4-graduation.md](docs/phase-4-graduation.md).
> See [docs/phase-3-architecture.md](docs/phase-3-architecture.md) and [docs/phase-3-graduation.md](docs/phase-3-graduation.md).
> See [docs/phase-2-architecture.md](docs/phase-2-architecture.md) and [docs/phase-2-graduation.md](docs/phase-2-graduation.md).
> See [docs/phase-1-architecture.md](docs/phase-1-architecture.md) and [docs/phase-1-graduation.md](docs/phase-1-graduation.md).
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

### `GET /metrics/aggregate/compare` — Time PostgreSQL vs ClickHouse (Day 4)

Runs the same aggregate on both stores and returns min/median/max latency plus `speedup_median`.

```bash
# Seed identical data into both stores first:
python tests/load/seed_aggregate_compare.py --rows 50000

curl "http://127.0.0.1:8001/metrics/aggregate/compare?machine_id=compare-bench&metric_name=cpu_usage&start_time=2026-06-01T00:00:00Z&end_time=2026-07-01T00:00:00Z&interval=5m&runs=5"
```

| Extra parameter | Default | Description |
|-----------------|---------|-------------|
| `runs` | `3` | How many times to run each store |
| `include_buckets` | `false` | Include both bucket series in the response |

### `POST /logs` — Ingest structured logs (Phase 4 Day 2)

Bulk-indexes log documents into OpenSearch. `event_id` is the document `_id` (re-POST overwrites).

```bash
curl -s -X POST http://127.0.0.1:8001/logs \
  -H 'Content-Type: application/json' \
  -d '{
    "logs": [{
      "event_id": "550e8400-e29b-41d4-a716-446655440000",
      "machine_id": "my-machine",
      "service": "agent",
      "level": "warn",
      "message": "disk usage high on /",
      "timestamp": "2026-07-23T08:00:00+00:00",
      "attrs": {"path": "/", "percent": 92}
    }]
  }'
```

### `GET /logs/{event_id}` — Fetch one log by id

Verification helper for direct document lookup.

```bash
curl http://127.0.0.1:8001/logs/550e8400-e29b-41d4-a716-446655440000
```

### `GET /logs/search` — Full-text + filters (Phase 4 Day 3)

| Parameter | Required | Description |
|-----------|----------|-------------|
| `q` | No | Full-text query on `message` |
| `machine_id` | No | Exact host filter |
| `service` | No | Exact service filter |
| `level` | No | `debug` / `info` / `warn` / `error` |
| `start_time` / `end_time` | No | Time range |
| `limit` / `offset` | No | Pagination (default limit 50) |

```bash
curl "http://127.0.0.1:8001/logs/search?q=disk&level=warn&limit=10"
```

Agent threshold logs (Day 4) — lower thresholds to force a warn:

```bash
cd agent
DISK_WARN_PERCENT=1 python main.py
# then:
curl "http://127.0.0.1:8001/logs/search?service=agent&level=warn"
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

See [docs/phase-1-architecture.md](docs/phase-1-architecture.md) for system diagrams and design decisions.

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

Earlier Phase 1 constraints that motivated later work:

| Limitation | Status |
|------------|--------|
| In-memory queue | Addressed in Phase 2 (Kafka) |
| PostgreSQL-only analytics | Addressed in Phase 3 (ClickHouse aggregates) |
| Single API process | Partially addressed (workers scale; API still one process locally) |
| Retry storm | Softened by durable bus + agent spool |
| No retention policy | Still open (CH monthly partitions make this easier later) |
| No percentiles (p95) | Deferred |
| ClickHouse duplicates under rare redelivery | Accepted; `ReplacingMergeTree` later if needed |

See [docs/bottlenecks-and-roadmap.md](docs/bottlenecks-and-roadmap.md) for scale analysis.

---

## Roadmap

### Completed

| Phase | Focus |
|-------|-------|
| 1 | Agent → API → queue → PostgreSQL; aggregates; idempotency |
| 2 | Kafka ingest bus, workers, DLQ, rate limits, `/pipeline` |
| 3 | ClickHouse dual-write + analytics + PG vs CH compare |
| 4 | OpenSearch logs — ingest, search, agent/API/worker shipping |
| 5 | OpenTelemetry / Jaeger — distributed tracing + dual-write spans |
| 6 | Multi-tenancy — Day 2 storage isolation (in progress) |

### Later phases

| Phase | Focus |
|-------|-------|
| 6 (rest) | Tenant isolation, per-tenant limits, metering, sharding |

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
**Architecture:** [docs/phase-1-architecture.md](docs/phase-1-architecture.md)

---

## Learning goals (Phase 3)

- [x] Run ClickHouse locally and define a MergeTree metrics schema
- [x] Dual-write from Kafka workers (PG idempotent + CH append)
- [x] Route analytical aggregates to ClickHouse
- [x] Measure PostgreSQL vs ClickHouse on the same query
- [x] Document architecture and graduate Phase 3

**Phase 3 graduation:** [docs/phase-3-graduation.md](docs/phase-3-graduation.md)  
**Architecture:** [docs/phase-3-architecture.md](docs/phase-3-architecture.md)

---

## Learning goals (Phase 4)

- [x] Run OpenSearch locally and define a logs index (`text` vs `keyword`)
- [x] Ingest structured logs (`POST /logs`, `event_id` as `_id`)
- [x] Search with full-text + filters (`GET /logs/search`)
- [x] Ship logs from agent, API, and workers (best-effort)
- [x] Document architecture and graduate Phase 4

**Phase 4 graduation:** [docs/phase-4-graduation.md](docs/phase-4-graduation.md)  
**Architecture:** [docs/phase-4-architecture.md](docs/phase-4-architecture.md)

---

## Learning goals (Phase 5)

- [x] Run Jaeger locally and export spans via OTLP
- [x] Auto-instrument FastAPI HTTP requests
- [x] Propagate W3C context across HTTP and Kafka
- [x] Add manual spans for dual-write and logship
- [x] Correlate logs with `attrs.trace_id` / `span_id`
- [x] Document architecture and graduate Phase 5

**Phase 5 graduation:** [docs/phase-5-graduation.md](docs/phase-5-graduation.md)  
**Architecture:** [docs/phase-5-architecture.md](docs/phase-5-architecture.md)

---

## License

Learning project — no license specified.
