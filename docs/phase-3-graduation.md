# Phase 3 Graduation Checklist

**Completed:** Phase 3 Day 5  
**Next:** Phase 4 Day 1 — OpenSearch (logs index + health)

---

## Storage & schema

- [x] ClickHouse in Docker (`docker compose` + auth)
- [x] `insightnode.metrics` MergeTree table (`PARTITION BY` month, `ORDER BY` query shape)
- [x] App-level `ensure_schema()` (not only Docker initdb)
- [x] `/health` reports `clickhouse_ok`

## Dual-write (Day 2)

- [x] Worker writes PostgreSQL then ClickHouse
- [x] Kafka offset commit only after both succeed
- [x] PG keeps `event_id` idempotency; CH may duplicate under rare redelivery

## Read path (Day 3–4)

- [x] `GET /metrics` → PostgreSQL (raw points)
- [x] `GET /metrics/aggregate` → ClickHouse (`toStartOfInterval`)
- [x] `GET /metrics/aggregate/compare` → timed PG vs CH
- [x] Seed script for fair local benchmarks (`tests/load/seed_aggregate_compare.py`)

## Understanding (explain without notes)

- [x] Why columnar storage helps `AVG/MIN/MAX` over many rows
- [x] Why `ORDER BY (machine_id, metric_name, timestamp)` matches dashboards
- [x] Why dual-write commits PG before CH before Kafka offset
- [x] Why CH can look slower at small N (HTTP / cold start) but win at scale
- [x] Why PG still owns idempotency while CH is the analytics copy

## Docs

- [x] docs/phase-3-architecture.md
- [x] docs/phase-3-graduation.md (this file)
- [x] README reflects Phase 3 complete

---

## Phase 3 complete

You can now run a **dual-store metrics pipeline**: durable Kafka ingest, idempotent row storage in PostgreSQL, and columnar analytics in ClickHouse — with a lab endpoint to measure the difference.

Phase 4 starts when you need searchable, centralized **logs** (not just gauges).
