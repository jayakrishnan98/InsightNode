
CREATE TABLE IF NOT EXISTS metrics (
    id          BIGSERIAL PRIMARY KEY,
    tenant_id   VARCHAR(64) NOT NULL DEFAULT 'local',  -- Phase 6 Day 2
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
-- Main query pattern: tenant + machine + metric + time (Phase 6 Day 2)
CREATE INDEX IF NOT EXISTS idx_metrics_tenant_machine_metric_time
    ON metrics (tenant_id, machine_id, metric_name, timestamp);
-- Legacy index (pre-tenant); kept for old query patterns / gradual migration
CREATE INDEX IF NOT EXISTS idx_metrics_machine_metric_time
    ON metrics (machine_id, metric_name, timestamp);

-- Idempotency is per-tenant (two orgs may reuse the same agent event_id shape)
CREATE UNIQUE INDEX IF NOT EXISTS idx_metrics_dedup
    ON metrics (tenant_id, machine_id, event_id, metric_name)
    WHERE event_id IS NOT NULL;

-- Day 10: GET /metrics/aggregate uses query-time GROUP BY on this table.
-- At very large scale, consider a separate rollups table or ClickHouse (Phase 3).

-- Phase 6 Day 1: tenant registry (also ensured by backend.tenancy on API boot)
CREATE TABLE IF NOT EXISTS tenants (
    tenant_id            VARCHAR(64) PRIMARY KEY,
    name                 VARCHAR(255) NOT NULL,
    api_key              VARCHAR(128) NOT NULL UNIQUE,
    active               BOOLEAN NOT NULL DEFAULT TRUE,
    rate_limit_max       INTEGER,  -- Phase 6 Day 3; NULL = use RATE_LIMIT_MAX env
    quota_metric_events  INTEGER,  -- Phase 6 Day 4; NULL = use QUOTA_* env
    quota_log_events     INTEGER,
    quota_metric_points  INTEGER,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    notes                TEXT
);

-- Phase 6 Day 4: monthly usage counters (UTC calendar month)
CREATE TABLE IF NOT EXISTS tenant_usage (
    id              SERIAL PRIMARY KEY,
    tenant_id       VARCHAR(64) NOT NULL,
    period_start    DATE NOT NULL,
    metric_events   BIGINT NOT NULL DEFAULT 0,
    log_events      BIGINT NOT NULL DEFAULT 0,
    metric_points   BIGINT NOT NULL DEFAULT 0,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_tenant_usage_period UNIQUE (tenant_id, period_start)
);

-- Phase 7: alert events from Grafana Unified Alerting webhooks
CREATE TABLE IF NOT EXISTS alert_events (
    id              BIGSERIAL PRIMARY KEY,
    tenant_id       VARCHAR(64) NOT NULL DEFAULT 'local',
    fingerprint     VARCHAR(128) NOT NULL,
    rule_name       VARCHAR(255) NOT NULL,
    status          VARCHAR(32) NOT NULL,
    severity        VARCHAR(32),
    machine_id      VARCHAR(255),
    metric_name     VARCHAR(255),
    summary         TEXT,
    starts_at       TIMESTAMPTZ NOT NULL,
    ends_at         TIMESTAMPTZ,
    raw_payload     JSONB,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_alert_events_fingerprint_starts
        UNIQUE (fingerprint, starts_at)
);

CREATE INDEX IF NOT EXISTS idx_alert_events_tenant_status
    ON alert_events (tenant_id, status, starts_at DESC);

CREATE INDEX IF NOT EXISTS idx_alert_events_fingerprint
    ON alert_events (fingerprint);
