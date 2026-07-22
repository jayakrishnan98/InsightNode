# Database Schema (Phase 1)

InsightNode stores metrics in a single PostgreSQL table: `metrics`.

---

## Table definition

```sql
CREATE TABLE IF NOT EXISTS metrics (
    id          BIGSERIAL PRIMARY KEY,
    machine_id  VARCHAR(255) NOT NULL,
    metric_name VARCHAR(255) NOT NULL,
    value       DOUBLE PRECISION NOT NULL,
    unit        VARCHAR(50) NOT NULL,
    timestamp   TIMESTAMPTZ NOT NULL,
    event_id    UUID,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

Source of truth: [`sql/schema.sql`](../sql/schema.sql)

---

## Column glossary

| Column | Type | Description |
|--------|------|-------------|
| `id` | BIGSERIAL | Surrogate primary key; internal row identity |
| `machine_id` | VARCHAR(255) | Source host (typically `socket.gethostname()`) |
| `metric_name` | VARCHAR(255) | e.g. `cpu_usage`, `memory_usage`, `disk_usage` |
| `value` | DOUBLE PRECISION | Observed gauge value |
| `unit` | VARCHAR(50) | e.g. `percent` |
| `timestamp` | TIMESTAMPTZ | **Agent observation time** (when sample was taken) |
| `event_id` | UUID | **Idempotency key** — one per agent collection cycle |
| `created_at` | TIMESTAMPTZ | **Server insert time** (when worker persisted the row) |

### `timestamp` vs `created_at`

- **`timestamp`** — what the agent measured; used for charts and alerts
- **`created_at`** — when the row landed in PostgreSQL; useful for debugging queue lag

A row with `created_at - timestamp > 30s` may indicate backlog or outage recovery.

---

## Indexes

| Index | Columns | Purpose |
|-------|---------|---------|
| `metrics_pkey` | `id` | Primary key |
| `idx_metrics_timestamp` | `timestamp` | Broad time-range scans |
| `idx_metrics_machine_metric_time` | `(machine_id, metric_name, timestamp)` | Main query pattern for GET /metrics and aggregate WHERE clause |
| `idx_metrics_dedup` | `(machine_id, event_id, metric_name)` WHERE `event_id IS NOT NULL` | Idempotent inserts — prevents duplicate rows |

---

## Row shape

One agent payload with 3 metrics becomes **3 rows** sharing the same `event_id` and `timestamp`:

```
event_id: 63988cd7-98ce-4daf-8981-8aa7cc55f82e
timestamp: 2026-07-10T05:30:00+00:00

→ cpu_usage     7.6 percent
→ memory_usage 84.8 percent
→ disk_usage   40.0 percent
```

---

## Append-only model

Observability data is **insert-only**:

- New samples → `INSERT`
- Corrections → new row (or future compaction job)
- Updates to old rows → avoided

This matches how Prometheus blocks, Datadog intake, and ClickHouse partitions work at scale.

---

## Migrations

| File | Change |
|------|--------|
| `sql/schema.sql` | Initial table + indexes (fresh installs) |
| `sql/migrations/001_add_event_id.sql` | Adds `event_id` column + `idx_metrics_dedup` (existing DBs) |

Apply migration:

```bash
psql -U insightnode -d insightnode -f sql/migrations/001_add_event_id.sql
```

---

## Current scale snapshot (2026-07-10 capstone)

| Metric | Value |
|--------|-------|
| Total rows | 13,788 |
| Rows with `event_id` | 2,025 |
| Unique events | 675 |
| Table size | ~4 MB |

Pre-Day-11 rows have `event_id = NULL` and are not deduplicated.

---

## Phase 3 — ClickHouse (analytics copy)

PostgreSQL remains the idempotent row store for raw points. ClickHouse holds a dual-written copy for aggregates.

Source: [`sql/clickhouse/schema.sql`](../sql/clickhouse/schema.sql)  
Details: [`phase-3-architecture.md`](phase-3-architecture.md)
