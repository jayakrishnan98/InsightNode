
CREATE TABLE IF NOT EXISTS metrics (
    id          BIGSERIAL PRIMARY KEY,
    machine_id  VARCHAR(255) NOT NULL,
    metric_name VARCHAR(255) NOT NULL,
    value       DOUBLE PRECISION NOT NULL,
    unit        VARCHAR(50) NOT NULL,
    timestamp   TIMESTAMPTZ NOT NULL,
    event_id    UUID,  -- Partial unique index: only dedupe rows that have an event_id.
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Time-range scans
CREATE INDEX IF NOT EXISTS idx_metrics_timestamp
    ON metrics (timestamp);
-- Main query pattern: machine + metric + time range (Day 5)
CREATE INDEX IF NOT EXISTS idx_metrics_machine_metric_time
    ON metrics (machine_id, metric_name, timestamp);

CREATE UNIQUE INDEX IF NOT EXISTS idx_metrics_dedup
    ON metrics (machine_id, event_id, metric_name)
    WHERE event_id IS NOT NULL;
-- Day 10: GET /metrics/aggregate uses query-time GROUP BY on this table.
-- idx_metrics_machine_metric_time supports the WHERE clause.
-- At very large scale, consider a separate rollups table or ClickHouse (Phase 3).
-- Phase 6 Day 1: tenant registry (also ensured by backend.tenancy on API boot)
CREATE TABLE IF NOT EXISTS tenants (
    tenant_id   VARCHAR(64) PRIMARY KEY,
    name        VARCHAR(255) NOT NULL,
    api_key     VARCHAR(128) NOT NULL UNIQUE,
    active      BOOLEAN NOT NULL DEFAULT TRUE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    notes       TEXT
);
