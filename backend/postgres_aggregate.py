"""
PostgreSQL time-bucket aggregation (Phase 1–2 path).

Kept for Phase 3 Day 4 side-by-side comparison with ClickHouse.
Production GET /metrics/aggregate uses ClickHouse (Day 3).
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from sqlalchemy import func, select, text
from sqlalchemy.orm import Session

from backend.models import MetricRecord

# Allowlisted intervals — same keys as ClickHouse CH_INTERVAL_MAP.
PG_INTERVAL_MAP: dict[str, str] = {
    "1m": "1 minute",
    "5m": "5 minutes",
    "15m": "15 minutes",
    "1h": "1 hour",
    "3h": "3 hours",
    "6h": "6 hours",
    "12h": "12 hours",
    "24h": "24 hours",
    "1d": "1 day",
}


def query_aggregate(
    db: Session,
    *,
    tenant_id: str,
    machine_id: str,
    metric_name: str,
    start_time: datetime,
    end_time: datetime,
    interval: str,
) -> list[dict[str, Any]]:
    """
    Aggregate metrics with PostgreSQL date_bin (legacy analytics path).

    Logic:
        - Filter by tenant_id (Phase 6 Day 2).
        - Map interval → allowlisted PG interval literal.
        - date_bin(width, timestamp, origin) for bucket starts.
        - AVG / MIN / MAX / COUNT grouped by machine, metric, bucket.

    Reason:
        Day 4 runs this next to ClickHouse to feel row-store vs columnar cost
        on the same filter shape.
    """
    pg_interval = PG_INTERVAL_MAP.get(interval)
    if pg_interval is None:
        raise ValueError(f"Unsupported interval: {interval!r}")

    bucket = func.date_bin(
        text(f"'{pg_interval}'"),
        MetricRecord.timestamp,
        datetime(2000, 1, 1, tzinfo=timezone.utc),
    ).label("bucket_start")

    stmt = (
        select(
            MetricRecord.machine_id,
            MetricRecord.metric_name,
            bucket,
            func.avg(MetricRecord.value).label("avg"),
            func.min(MetricRecord.value).label("min"),
            func.max(MetricRecord.value).label("max"),
            func.count().label("sample_count"),
        )
        .where(MetricRecord.tenant_id == tenant_id)
        .where(MetricRecord.machine_id == machine_id)
        .where(MetricRecord.metric_name == metric_name)
        .where(MetricRecord.timestamp >= start_time)
        .where(MetricRecord.timestamp < end_time)
        .group_by(
            MetricRecord.machine_id,
            MetricRecord.metric_name,
            bucket,
        )
        .order_by(bucket.asc())
    )

    rows = db.execute(stmt).all()
    return [
        {
            "machine_id": row.machine_id,
            "metric_name": row.metric_name,
            "bucket_start": row.bucket_start,
            "avg": float(row.avg),
            "min": float(row.min),
            "max": float(row.max),
            "sample_count": int(row.sample_count),
        }
        for row in rows
    ]
