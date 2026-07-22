# Phase 2 Graduation Checklist

**Completed:** Phase 2 Day 6  
**Next:** Phase 3 Day 1 — ClickHouse (schema + health); Day 2 dual-write

---

## Pipeline

- [x] Durable ingest buffer (Redis → Kafka)
- [x] ACK / offset commit only after PostgreSQL success
- [x] Standalone worker process (`python -m backend.worker`)
- [x] Multiple consumers in one group
- [x] Dead-letter path for poison messages
- [x] Soft backpressure (503 on high lag)
- [x] Edge rate limit (429 per machine_id)
- [x] Idempotent Kafka producer
- [x] Per-partition lag visibility (`GET /pipeline`)

## Understanding (explain without notes)

- [x] Why in-memory `queue.Queue` was not enough
- [x] List BRPOP vs Stream XACK vs Kafka offset commit
- [x] Why `machine_id` is the Kafka key
- [x] What lag means and when to scale workers
- [x] How `event_id` + DLQ work together under at-least-once delivery

## Docs

- [x] docs/phase-2-architecture.md
- [x] docs/phase-2-graduation.md (this file)

---

## Phase 2 complete

You can now run a **decoupled, durable, multi-worker ingest pipeline** with ops visibility.
Phase 3 starts when PostgreSQL analytical queries become the bottleneck.
