ALTER TABLE metrics
    ADD COLUMN IF NOT EXISTS event_id UUID;

-- Partial unique index: only dedupe rows that have an event_id.
-- Old rows (NULL event_id) are untouched.
CREATE UNIQUE INDEX IF NOT EXISTS idx_metrics_dedup
    ON metrics (machine_id, event_id, metric_name)
    WHERE event_id IS NOT NULL;