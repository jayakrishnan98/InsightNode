# Phase 5 Graduation Checklist

**Completed:** Phase 5 Day 5  
**Next:** Phase 6 — production SaaS concerns (sharding, multi-tenancy, metering)

---

## Collector & SDK (Day 1)

- [x] Jaeger all-in-one in Docker (UI `:16686`, OTLP `:4317` / `:4318`)
- [x] `TracerProvider` + OTLP HTTP exporter (`backend/tracing.py`)
- [x] Bootstrap span on process start; `/health` reports `jaeger_ok`
- [x] `OTEL_ENABLED=0` disables tracing cleanly

## HTTP auto-instrumentation (Day 2)

- [x] `FastAPIInstrumentor` at import time (middleware before ASGI start)
- [x] One server span per request (e.g. `POST /metrics`)
- [x] Excluded noisy URLs (`health`, `docs`, OpenAPI)

## Context propagation (Day 3)

- [x] Agent injects W3C `traceparent` on `POST /metrics` (`agent/tracing.py`)
- [x] API injects same context into Kafka message headers on produce
- [x] Worker extracts headers → `kafka.consume` continues the same `trace_id`
- [x] Distinct `service.name`: `insightnode-agent` / `insightnode-api` / `insightnode-worker`

## Manual spans + correlation (Day 4)

- [x] Worker waterfall: `dual_write` → `db.postgres.insert` + `db.clickhouse.insert`
- [x] Logship spans (`logship.index` / `logship.ship`)
- [x] Span attrs include `insightnode.event_id` / row counts where useful
- [x] Shipped logs carry `attrs.trace_id` + `attrs.span_id` for Jaeger ↔ OpenSearch

## Understanding (explain without notes)

- [x] Why metrics, logs, and traces answer different questions (three pillars)
- [x] Difference between a **span** and a **trace** (`trace_id` shared, `span_id` unique)
- [x] Why `inject` / `extract` are required across process boundaries (HTTP, Kafka)
- [x] Why auto-instrumentation covers HTTP but dual-write needs **manual** spans
- [x] Why FastAPI must be instrumented before the app starts (Starlette middleware rule)
- [x] Why logship attaches `trace_id` to log attrs (correlation, not duplication of the span)

## Docs

- [x] docs/phase-5-architecture.md
- [x] docs/phase-5-graduation.md (this file)
- [x] README reflects Phase 5 complete

---

## Phase 5 complete

You can now follow **one ingest request** across agent → API → Kafka → worker in Jaeger, see where dual-write time goes (PostgreSQL vs ClickHouse), and correlate ops logs in OpenSearch via `attrs.trace_id`.

Phase 6 starts when you need **multi-tenant / production SaaS** concerns (sharding, stronger tenancy isolation, usage metering) on top of this observability stack.
