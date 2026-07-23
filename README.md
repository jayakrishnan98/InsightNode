# InsightNode

A simplified observability platform built to learn system design, distributed systems, and telemetry pipelines. Inspired by Datadog — not a clone.

**Current stage: Phase 7 complete — visualization & product UI**  
**Next:** Open-ended (retention, percentiles, multi-cell routing, …)

Phase 7 adds Grafana dashboards, Prometheus platform metrics, Grafana→webhook alerting, host/summary APIs, and a thin Next.js UI — on top of the Phase 1–6 ingest pipeline.

---

## Problem statement

Host metrics and logs are easy to *collect* and hard to *operate* on: write pressure, expensive time-range queries, and the temptation to rebuild every chart in a custom app. InsightNode shows a realistic evolution — PostgreSQL first, then Kafka + ClickHouse + OpenSearch — and finishes with **Grafana for ops charts** and a **thin UI for product workflows**.

---

## What it does

```
Agent (psutil + spool + logship + OTEL)
    │  POST /metrics  (+ traceparent)
    │  POST /logs
    ▼
FastAPI ──produce──► Kafka ──workers──► PostgreSQL (idempotent)
  │                                   └─► ClickHouse (analytics)
  ├── GET /metrics, /metrics/aggregate
  ├── GET /hosts, /hosts/{id}, /system/summary
  ├── GET /logs/search
  ├── GET /alert-events ; POST /alert-events/webhook
  ├── GET /prometheus   (process metrics — not ingest)
  ├── GET /health, /pipeline, /dlq, /tenants, /usage
  └── OTLP ──► Jaeger

ClickHouse + Prometheus ──► Grafana (ops dashboards + alerts)
FastAPI (server-side key) ──► Next.js UI :3001 (product workflows)
```

### Features

| Component | Capability |
|-----------|------------|
| **Agent** | CPU, memory, disk gauges; spool; threshold warn logs |
| **Ingest** | FastAPI → Kafka → dual-write PG + ClickHouse |
| **Logs** | OpenSearch ingest + search |
| **Traces** | OpenTelemetry → Jaeger |
| **Tenancy** | API keys, isolation, rate limits, quotas, logical shards |
| **Grafana** | Provisioned Infrastructure + Platform dashboards |
| **Prometheus** | API/worker scrape via `/prometheus` |
| **Alerting** | Grafana rules → webhook → `alert_events` |
| **Custom UI** | Overview, hosts, alerts, logs, agent setup |

---

## Architecture evolution

```text
Version 1:
  Agent → API → (in-memory queue) → PostgreSQL

Observed limitations:
  - queue loss on API crash
  - write pressure and expensive time-range aggregates
  - retention concerns

Version 2:
  Agent → API → Kafka → Workers → PostgreSQL + ClickHouse
  Logs → API → OpenSearch
  Traces → OTLP → Jaeger
  Tenancy → API keys, isolation, limits, quotas

Version 3 (Phase 7):
  Visualization → Grafana (ClickHouse + Prometheus)
  Alerting → Grafana webhook → PostgreSQL alert_events
  Product UI → Next.js (hosts, overview, logs, setup)
```

Only stages that were implemented or genuinely evaluated are listed above.

Detailed diagrams: [docs/phase-7-architecture.md](docs/phase-7-architecture.md).

---

## Local setup

### Prerequisites

- Python 3.11+, PostgreSQL 14+, Node.js 20+ (for UI), Docker

### 1. Python deps + database

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

createdb insightnode   # or create user/db as in earlier docs
psql -U insightnode -d insightnode -f sql/schema.sql
# optional: psql … -f sql/migrations/002_alert_events.sql
# (API also ensures alert_events on boot)
```

### 2. Infrastructure

```bash
cp .env.example .env   # optional overrides
docker compose up -d
```

| Service | Port |
|---------|------|
| Kafka (Redpanda) | 9092 |
| ClickHouse | 8123 / 9000 |
| OpenSearch | 9200 |
| Jaeger UI | 16686 |
| Prometheus | 9090 |
| Grafana | 3000 |

### 3. API + worker + agent

```bash
uvicorn backend.main:app --reload --port 8001
python -m backend.worker
cd agent && python main.py
```

### 4. Custom UI

```bash
cd frontend
cp .env.example .env.local
npm install
npm run dev    # http://localhost:3001
```

### Access

| Surface | URL |
|---------|-----|
| Custom UI | http://localhost:3001 |
| Grafana | http://localhost:3000 (default `admin` / `admin`) |
| Infrastructure dashboard | http://localhost:3000/d/insightnode-infrastructure |
| Platform dashboard | http://localhost:3000/d/insightnode-platform |
| API docs | http://127.0.0.1:8001/docs |
| Prometheus | http://localhost:9090 |
| Jaeger | http://localhost:16686 |

Lab secrets (`dev-local-key`, compose passwords) are for **local learning only**.

---

## Load-test results (lab-scale)

Use the fleet simulator — **do not** extrapolate to production without new measurements.

```bash
source venv/bin/activate
python tests/load/fleet_simulator.py \
  --machines 10 \
  --interval 1 \
  --duration 30 \
  --concurrency 20 \
  --api-key dev-local-key
```

| Scenario | Target (portfolio) |
|----------|--------------------|
| Machines | 10–50 simulated hosts |
| Duration | 30–60 seconds |
| Metrics | Unique `event_id` each POST (real inserts) |

**Recorded run** (2026-07-23, local Mac, API + worker, default tenant rate limit):

```bash
python tests/load/fleet_simulator.py \
  --machines 2 --interval 5 --duration 30 --concurrency 4 \
  --api-key dev-local-key --machine-prefix labfleet
```

| Metric | Value |
|--------|-------|
| Wall time | 30.6 s |
| Metric POSTs | 20 |
| Accepted (202) | 20 (100%) |
| Approx points | 60 |
| POSTs / sec | 0.65 |
| Points / sec | ~2.0 |
| p50 latency | ~1114 ms |
| p95 latency | ~1189 ms |
| 429 / 503 | 0 |

Kept under the default **30 ingest requests / 60s** tenant rate limit so the run measures the happy path, not throttling. For higher RPS demos, raise `RATE_LIMIT_MAX` (and quotas) on the API process and re-measure — still lab-scale only.

Also useful: `python tests/load/unique_post.py --n 1000 --c 50` for burst ingest.

---

## Project structure (high level)

```
InsightNode/
├── agent/                 # Host telemetry agent
├── backend/               # FastAPI, worker, storage clients, alerts, hosts
├── frontend/              # Next.js thin UI (:3001)
├── infrastructure/
│   ├── grafana/           # Provisioning + dashboards
│   └── prometheus/        # Scrape config
├── docs/                  # Phase architecture + graduation
├── sql/                   # PostgreSQL + ClickHouse schemas
├── opensearch/            # Logs index mapping
├── tests/load/            # unique_post + fleet_simulator
└── docker-compose.yml
```

---

## API reference (selected)

Full OpenAPI: http://127.0.0.1:8001/docs

| Method | Path | Notes |
|--------|------|-------|
| `POST` | `/metrics` | Ingest → Kafka (202 / 429 / 503) |
| `GET` | `/metrics/aggregate` | ClickHouse buckets |
| `POST` | `/logs` | OpenSearch ingest |
| `GET` | `/logs/search` | Full-text + filters |
| `GET` | `/hosts` | Fleet from ClickHouse |
| `GET` | `/hosts/{id}` | Detail + warn logs |
| `GET` | `/system/summary` | Overview cards |
| `GET` | `/alert-events` | Alert history |
| `POST` | `/alert-events/webhook` | Grafana Bearer webhook |
| `GET` | `/prometheus` | Process metrics scrape |
| `GET` | `/health` | Dependency + lag snapshot |

Send `X-API-Key` for tenant-scoped routes (lab default `dev-local-key`).

---

## Known limitations

| Limitation | Notes |
|------------|-------|
| Soft tenancy default | `TENANCY_STRICT=0` falls back to `local` |
| OpenSearch security disabled | Local only |
| CH duplicates on rare redelivery | Accepted; ReplacingMergeTree later |
| No retention / p95 | Still open |
| API/worker not containerized | Host processes; Prometheus uses `host.docker.internal` |
| Lab credentials in compose | Change before any shared deploy |

---

## Future improvements

- Retention jobs on ClickHouse partitions
- Percentile / histogram metrics
- Multi-cell routing using `shard_id`
- Optional custom alert-rule evaluator + UI CRUD
- Optional OpenSearch Dashboards for deep log UX
- Dockerize API/worker for cloud VM Compose

Cloud notes: [docs/cloud-readiness.md](docs/cloud-readiness.md).

---

## Roadmap

### Completed

| Phase | Focus |
|-------|-------|
| 1 | Agent → API → queue → PostgreSQL |
| 2 | Kafka, workers, DLQ, rate limits |
| 3 | ClickHouse dual-write + aggregates |
| 4 | OpenSearch logs |
| 5 | OTEL / Jaeger |
| 6 | Multi-tenancy |
| 7 | Grafana, Prometheus, alerts, thin UI |

### Docs by phase

| Phase | Architecture | Graduation |
|-------|--------------|------------|
| 1–6 | `docs/phase-N-architecture.md` | `docs/phase-N-graduation.md` |
| 7 | [phase-7-architecture.md](docs/phase-7-architecture.md) | [phase-7-graduation.md](docs/phase-7-graduation.md) |

Interview prep: [docs/interview-questions.md](docs/interview-questions.md)  
Screenshots: [docs/screenshots/README.md](docs/screenshots/README.md)

---

## Learning goals (Phase 7)

- [x] Provision Grafana datasources/dashboards from git
- [x] Expose platform metrics without colliding with ingest `/metrics`
- [x] Demonstrate alerting with recovery and dedupe
- [x] Ship a thin UI that deep-links to Grafana instead of cloning it
- [x] Document architecture evolution and interview talking points

**Phase 7 graduation:** [docs/phase-7-graduation.md](docs/phase-7-graduation.md)  
**Architecture:** [docs/phase-7-architecture.md](docs/phase-7-architecture.md)

---

## License

Learning project — no license specified.
