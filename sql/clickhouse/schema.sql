-- Phase 3 Day 1: ClickHouse metrics table (columnar time-series store).
-- Phase 6 Day 2: tenant_id for multi-tenant isolation.
-- Applied automatically by docker-entrypoint-initdb.d on first container boot,
-- and idempotently by backend.clickhouse_client.ensure_schema().

CREATE DATABASE IF NOT EXISTS insightnode;

CREATE TABLE IF NOT EXISTS insightnode.metrics
(
    tenant_id   String DEFAULT 'local',
    machine_id  String,
    metric_name String,
    value       Float64,
    unit        String,
    timestamp   DateTime64(3, 'UTC'),
    event_id    Nullable(UUID),
    created_at  DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(timestamp)
ORDER BY (tenant_id, machine_id, metric_name, timestamp)
SETTINGS index_granularity = 8192;

-- Notes:
--   - MergeTree = append-oriented columnar storage (good for aggregates).
--   - PARTITION BY month keeps time-range scans cheap to drop old data later.
--   - ORDER BY leads with tenant_id so SaaS filters prune efficiently (Phase 6).
--   - No unique constraint here — PostgreSQL still owns idempotency (event_id).
--   - Existing volumes: ensure_schema() ADD COLUMN tenant_id (ORDER BY stays
--     as created until the table is rebuilt).
