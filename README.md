# InsightNode

A simplified observability platform built to learn system design, distributed systems, and time-series data — inspired by Datadog, not a clone of it.

**Phase 1 (Week 1) complete:** Agent → FastAPI → PostgreSQL

## What it does today

| Capability | Status |
|------------|--------|
| Collect host metrics (CPU, memory, disk) | ✅ |
| Push metrics to ingestion API | ✅ |
| Validate payloads with Pydantic | ✅ |
| Store metrics in PostgreSQL | ✅ |
| Query metrics by machine, name, time range | ✅ |
| Dashboards, alerting, logs, traces | 🔜 Phase 2+ |

## Architecture (Phase 1)

```
┌─────────────┐     POST /metrics      ┌─────────────┐     INSERT      ┌──────────────┐
│ Telemetry   │ ─────────────────────► │   FastAPI   │ ─────────────► │  PostgreSQL  │
│   Agent     │ ◄───────────────────── │   Backend   │                │   metrics    │
└─────────────┘      201 Created       └─────────────┘                └──────────────┘
                                              │
                                              │ GET /metrics
                                              ▼
                                        ┌─────────────┐
                                        │   Client    │
                                        │ (curl/docs) │
                                        └─────────────┘
```

## Project structure

```
InsightNode/
├── agent/              # Telemetry agent (runs on each monitored machine)
│   └── main.py
├── backend/            # FastAPI ingestion + query API
│   ├── main.py
│   ├── database.py
│   └── models.py
├── sql/
│   └── schema.sql      # PostgreSQL schema
├── docs/               # Architecture documentation (Week 1)
└── requirements.txt
```

## Quick start

### Prerequisites

- Python 3.13+
- PostgreSQL 16+

### Setup

```bash
# Clone / enter project
cd InsightNode

# Virtual environment
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Database (first time only)
psql postgres -c "CREATE USER insightnode WITH PASSWORD 'insightnode';"
psql postgres -c "CREATE DATABASE insightnode OWNER insightnode;"
psql postgresql://insightnode:insightnode@localhost:5432/insightnode -f sql/schema.sql
```

### Run

**Terminal 1 — API:**

```bash
source venv/bin/activate
uvicorn backend.main:app --reload --port 8000
```

**Terminal 2 — Agent:**

```bash
source venv/bin/activate
cd agent
python main.py
```

**Terminal 3 — Query:**

```bash
curl "http://127.0.0.1:8000/metrics?metric_name=cpu_usage&limit=10"
```

Interactive API docs: http://127.0.0.1:8000/docs

## API endpoints

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/health` | Health check |
| POST | `/metrics` | Ingest metric payload |
| GET | `/metrics` | Query stored metrics |

## Documentation

| Document | Contents |
|----------|----------|
| [Architecture](docs/architecture.md) | System overview, components, design decisions |
| [Request flows](docs/request-flows.md) | Ingest and query sequence diagrams |
| [Database schema](docs/database-schema.md) | Table design, indexes, append-only model |
| [Bottlenecks & roadmap](docs/bottlenecks-and-roadmap.md) | Why Phase 1 doesn't scale, Phase 2+ plan |

## Learning path

| Week / Phase | Focus |
|--------------|-------|
| **Week 1** ✅ | Agent, FastAPI, PostgreSQL — sync pipeline |
| **Phase 2** | Redis, Kafka — queues, batching, async writes |
| **Phase 3** | ClickHouse — time-series storage at scale |
| **Phase 4** | OpenSearch — log collection and search |
| **Phase 5** | OpenTelemetry + Jaeger — distributed tracing |
| **Phase 6** | Multi-tenancy, rate limiting, sharding |

## License

Learning project — use freely.
