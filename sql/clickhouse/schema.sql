-- Phase 3 Day 1: ClickHouse metrics table (columnar time-series store).
-- Applied automatically by docker-entrypoint-initdb.d on first container boot,
-- and idempotently by backend.clickhouse_client.ensure_schema().

CREATE DATABASE IF NOT EXISTS insightnode;

CREATE TABLE IF NOT EXISTS insightnode.metrics
(
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
ORDER BY (machine_id, metric_name, timestamp)
SETTINGS index_granularity = 8192;

-- Notes (Day 1):
--   - MergeTree = append-oriented columnar storage (good for aggregates).
--   - PARTITION BY month keeps time-range scans cheap to drop old data later.
--   - ORDER BY matches our query pattern: host + metric + time.
--   - No unique constraint here — PostgreSQL still owns idempotency (event_id).
--     Day 2 dual-write will insert into both stores; CH may see duplicates under
--     at-least-once delivery until we add ReplacingMergeTree / dedup later.
