# Phase 4 Graduation Checklist

**Completed:** Phase 4 Day 5  
**Next:** Phase 5 — OpenTelemetry + traces (Jaeger concepts)

---

## Infra & index

- [x] OpenSearch in Docker (single-node, security disabled for local learning)
- [x] `insightnode-logs` index mapping (`text` + `keyword` + `date`)
- [x] App-level `ensure_index()` on API boot
- [x] `/health` reports `opensearch_ok`

## Ingest & search

- [x] `POST /logs` bulk index (`event_id` = document `_id`)
- [x] `GET /logs/{event_id}` direct lookup
- [x] `GET /logs/search` full-text (`q`) + filters (`machine_id`, `service`, `level`, time)

## Log shipping (Day 4)

- [x] Agent ships lifecycle / threshold logs via `agent/logship.py` → `POST /logs`
- [x] API ships rate-limit / lag events via `backend/logship.py`
- [x] Worker ships retry / DLQ events via `backend/logship.py`
- [x] Shipping is best-effort (never breaks metrics / spool)

## Understanding (explain without notes)

- [x] Why logs need a search engine and metrics need a TSDB / columnar store
- [x] Difference between `keyword` (exact) and `text` (analyzed) fields
- [x] Why `bool` queries separate `must` (scored) from `filter` (not scored)
- [x] Why `/logs/search` must be registered before `/logs/{event_id}`
- [x] Why log shipping is best-effort and metrics remain the critical path

## Docs

- [x] docs/phase-4-architecture.md
- [x] docs/phase-4-graduation.md (this file)
- [x] README reflects Phase 4 complete

---

## Phase 4 complete

You can now **ingest, search, and ship structured logs** alongside the metrics pipeline: OpenSearch for text events, PostgreSQL + ClickHouse for gauges.

Phase 5 starts when you need **distributed traces** (request paths across services), not just metrics and logs.
