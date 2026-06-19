# InsightNode

A simplified observability platform built to learn system design, distributed systems, and telemetry pipelines. Inspired by Datadog — not a clone.

**Current stage: Phase 1 complete**

Phase 1 uses Python, FastAPI, and PostgreSQL only. No Kafka, Redis, ClickHouse, or Kubernetes yet.

---

## What it does

InsightNode collects host metrics (CPU, memory, disk) from a local agent, ingests them through a FastAPI backend, stores them in PostgreSQL, and exposes a query API.

```
Agent (psutil)  →  POST /metrics  →  In-memory queue  →  Worker  →  PostgreSQL
                      (202 fast)         (buffer)         (batch)      (storage)
                                                                           ↑
                                                                    GET /metrics
```

### Features (Phase 1)

| Component | Capability |
|-----------|------------|
| **Agent** | Collects CPU, memory, and disk gauges every 5 seconds |
| **Ingestion API** | Validates payloads with Pydantic, enqueues for async processing |
| **Background worker** | Batch-writes to PostgreSQL (up to 50 payloads per commit) |
| **Query API** | Filter by `machine_id`, `metric_name`, time range |
| **Health endpoint** | Queue depth, worker status |
| **Agent resilience** | Exponential backoff retries + on-disk spool when API is down |
| **Worker resilience** | Re-queues failed batches (up to 3 attempts) |

---

## Project structure

```
InsightNode/
├── agent/
│   ├── main.py          # Telemetry agent — collect, send, replay spool
│   ├── spool.py         # NDJSON disk buffer for failed payloads
│   └── data/            # Runtime spool file (gitignored)
├── backend/
│   ├── main.py          # FastAPI app — ingest, query, health
│   ├── worker.py        # Background consumer — queue → PostgreSQL
│   ├── database.py      # SQLAlchemy engine and session setup
│   └── models.py        # MetricRecord ORM model
├── sql/
│   └── schema.sql       # PostgreSQL table and indexes
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
# Using psql (adjust user/password as needed)
createdb insightnode

# Or via psql:
# CREATE USER insightnode WITH PASSWORD 'insightnode';
# CREATE DATABASE insightnode OWNER insightnode;

psql -U insightnode -d insightnode -f sql/schema.sql
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

### Start the API

```bash
uvicorn backend.main:app --reload
```

API docs: http://127.0.0.1:8000/docs

### Start the agent

In a second terminal:

```bash
cd agent
python main.py
```

Expected steady-state output:

```
[SENT] 202 → {'status': 'accepted', 'machine_id': '...', 'metric_count': 3, 'queued': 0}
```

One line every ~5 seconds.

---

## API reference

### `POST /metrics` — Ingest metrics

Accepts a JSON payload and returns `202 Accepted` immediately. Persistence happens asynchronously via the background worker.

```json
{
  "machine_id": "my-machine",
  "timestamp": "2026-06-19T06:56:00.843013+00:00",
  "metrics": [
    { "name": "cpu_usage", "value": 45.2, "unit": "percent" },
    { "name": "memory_usage", "value": 62.1, "unit": "percent" },
    { "name": "disk_usage", "value": 78.0, "unit": "percent" }
  ]
}
```

Returns `503` if the ingest queue is full (backpressure).

### `GET /metrics` — Query stored metrics

| Parameter | Type | Description |
|-----------|------|-------------|
| `machine_id` | string | Filter by host |
| `metric_name` | string | Filter by metric (e.g. `cpu_usage`) |
| `start_time` | ISO 8601 | Inclusive lower bound |
| `end_time` | ISO 8601 | Inclusive upper bound |
| `limit` | int | Max rows (default 100, max 1000) |

Example:

```bash
curl "http://127.0.0.1:8000/metrics?machine_id=my-machine&metric_name=cpu_usage&limit=10"
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
| `created_at` | TIMESTAMPTZ | Server insert time |

Indexes:

- `idx_metrics_timestamp` — time-range scans
- `idx_metrics_machine_metric_time` — filtered queries by host + metric + time

---

## Architecture

### Ingestion flow

1. Agent samples OS metrics via `psutil` (5-second CPU window).
2. Agent POSTs JSON to `/metrics`.
3. FastAPI validates the payload and enqueues it (non-blocking).
4. API responds `202` immediately.
5. Background worker dequeues payloads in batches and commits to PostgreSQL.

### Failure handling

**Agent side**

- Retries failed POSTs up to 5 times with exponential backoff (1s, 2s, 4s, 8s).
- After all retries fail, appends the payload to `agent/data/spool.ndjson`.
- On each loop, replays buffered payloads before collecting new metrics.

**API side**

- If PostgreSQL is temporarily unavailable, the worker re-queues failed payloads (up to 3 attempts).
- If the in-memory queue is full, the API returns `503` so agents back off or spool.

---

## Known limitations (by design)

These are intentional Phase 1 constraints. They motivate Phase 2.

| Limitation | Impact |
|------------|--------|
| In-memory queue | Metrics in the queue are lost if the API process crashes |
| PostgreSQL for time-series | Slow at very high cardinality and volume |
| No deduplication | Spool replay can insert duplicate rows |
| No aggregations | Query returns raw points only (no avg/min/max buckets) |
| Single process | No horizontal scaling |
| Retry storm | Agent spends a long time retrying when API is down for extended periods |

---

## Roadmap

### Phase 2 (next)

- Redis or Kafka for durable, distributed buffering
- Replace in-memory queue
- Understand backpressure and decoupling at scale

### Later phases

| Phase | Focus |
|-------|-------|
| 3 | ClickHouse — time-series storage and analytical queries |
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
- [x] Identify scalability bottlenecks before introducing advanced tooling

---

## License

Learning project — no license specified.
