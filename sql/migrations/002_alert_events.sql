-- Phase 7: alert events from Grafana Unified Alerting webhooks.
-- Applied via psql or idempotently by backend.alerts.ensure_alert_events_schema().

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
