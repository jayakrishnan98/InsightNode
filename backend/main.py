"""
InsightNode API — ingestion and query endpoints for host metrics.

Architecture (Phase 3 Day 1):
    Agent --POST /metrics--> Kafka topic --standalone worker(s)--> PostgreSQL
                              (202 + rate limit)  (consumer group + manual commit)
                                                   │
                                                   └─ poison → Kafka DLQ topic

    ClickHouse is up (schema + health ping). Dual-write lands in Day 2;
    aggregate query routing lands in Day 3.
"""

from datetime import datetime, timezone
from typing import Optional, Literal

import logging
import os

from fastapi import Depends, FastAPI, Query, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import select, func, text
from sqlalchemy.orm import Session
from contextlib import asynccontextmanager

from backend.database import get_db
from backend.models import MetricRecord
from backend.clickhouse_client import (
    close_client as close_clickhouse,
    ensure_schema as ensure_clickhouse_schema,
    ping as clickhouse_ping,
)
from backend.kafka_client import (
    MAX_DELIVERIES,
    QUEUE_MAX_LENGTH,
    QueueFullError,
    approximate_lag,
    dlq_size_estimate,
    enqueue_payload,
    ensure_topics,
    get_producer,
    partition_lag_details,
    peek_dlq,
    ping as kafka_ping,
)
from backend.rate_limit import (
    RATE_LIMIT_MAX,
    RATE_LIMIT_WINDOW_SECONDS,
    ingest_rate_limiter,
)

logger = logging.getLogger(__name__)
kafka_producer = None  # set in lifespan

# Optional: set EMBEDDED_WORKER=1 to run a worker thread inside the API (dev only).
EMBEDDED_WORKER = os.getenv("EMBEDDED_WORKER", "0") == "1"

INTERVAL_MAP: dict[str, str] = {
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


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    API lifespan: Kafka + ClickHouse bootstrap; optionally start embedded worker.

    Logic:
        - ensure_topics() so produce never hits UNKNOWN_TOPIC on first boot.
        - ensure_clickhouse_schema() so insightnode.metrics exists.
        - Create a process-wide KafkaProducer.
        - If EMBEDDED_WORKER=1, spawn in-process worker thread.

    Reason:
        Producers should be long-lived (connection reuse). Topics and CH schema
        must exist before agents start posting / Day 2 dual-write begins.
    """
    global kafka_producer

    ensure_topics()
    kafka_producer = get_producer()
    logger.info("Kafka producer ready")

    ensure_clickhouse_schema()
    logger.info("ClickHouse schema ready")

    stop_event = None
    worker_thread = None

    if EMBEDDED_WORKER:
        import threading

        from backend.worker import run_ingest_worker

        stop_event = threading.Event()
        worker_thread = threading.Thread(
            target=run_ingest_worker,
            args=(stop_event,),
            name="ingest-worker-embedded",
            daemon=True,
        )
        worker_thread.start()
        logger.info("Embedded Kafka worker started (EMBEDDED_WORKER=1)")
    else:
        logger.info(
            "API started without embedded worker — run: python -m backend.worker"
        )

    yield

    if stop_event is not None and worker_thread is not None:
        stop_event.set()
        worker_thread.join(timeout=5.0)
        logger.info("Embedded ingest worker stopped")

    if kafka_producer is not None:
        kafka_producer.flush()
        kafka_producer.close()
        kafka_producer = None

    close_clickhouse()


app = FastAPI(title="InsightNode", version="0.4.0", lifespan=lifespan)

class Metric(BaseModel):
    """Single metric reading inside an ingestion payload (name, value, unit)."""
    name: str = Field(..., min_length=1, examples=["cpu_usage"])
    value: float = Field(..., examples=[45.2])
    unit: str = Field(..., min_length=1, examples=["percent"])


class MetricsPayload(BaseModel):
    """Request body for POST /metrics — one timestamp, many metrics for one machine."""
    event_id: str = Field(
        ...,
        min_length=36,
        max_length=36,
        examples=["550e8400-e29b-41d4-a716-446655440000"],
    )
    machine_id: str = Field(
        ..., min_length=1, examples=["Jayakrishnans-MacBook-Air.local"]
    )
    timestamp: datetime = Field(..., examples=["2026-06-14T08:59:35.550356+00:00"])
    metrics: list[Metric] = Field(..., min_length=1)


class MetricPoint(BaseModel):
    """One stored metric row returned by GET /metrics (flattened from DB)."""
    machine_id: str
    metric_name: str
    value: float
    unit: str
    timestamp: datetime
    event_id: str | None = None

class MetricBucket(BaseModel):
    """One aggregated time bucket — summary stats for a machine + metric."""
    machine_id: str
    metric_name: str
    bucket_start: datetime
    avg: float
    min: float
    max: float
    sample_count: int

class MetricsAggregateResponse(BaseModel):
    """Wrapper for aggregation results."""
    interval: str
    count: int
    buckets: list[MetricBucket]

class MetricsQueryResponse(BaseModel):
    """Wrapper for query results: total count plus list of metric points."""
    count: int
    metrics: list[MetricPoint]


@app.get("/health")
def health_check():
    """
    Liveness probe and ingest pipeline visibility.

    Logic:
        - Ping Kafka and ClickHouse; report approximate consumer lag and DLQ size.

    Reason:
        High lag = workers behind. Growing dlq_size = poison payloads parked.
        clickhouse_ok proves Day 1 columnar store is reachable before dual-write.
    """
    return {
        "status": "ok",
        "kafka_ok": kafka_ping(),
        "clickhouse_ok": clickhouse_ping(),
        "queue_backend": "kafka",
        "queue_size": approximate_lag(),
        "queue_maxsize": QUEUE_MAX_LENGTH,
        "dlq_size": dlq_size_estimate(),
        "max_deliveries": MAX_DELIVERIES,
        "rate_limit_max": RATE_LIMIT_MAX,
        "rate_limit_window_seconds": RATE_LIMIT_WINDOW_SECONDS,
        "worker_mode": "embedded" if EMBEDDED_WORKER else "external",
    }


@app.get("/pipeline")
def pipeline_status():
    """
    Per-partition Kafka lag and pipeline summary (Phase 2 Day 6).

    Logic:
        - partition_lag_details() for each ingest partition.
        - Include DLQ size and rate-limit config.

    Reason:
        Total lag hides hot partitions. When one machine_id hashes heavily to
        one partition, only that partition's lag spikes — this endpoint shows it.
    """
    lag = partition_lag_details()
    return {
        "kafka_ok": kafka_ping(),
        "ingest": lag,
        "dlq_size": dlq_size_estimate(),
        "backpressure_max_lag": QUEUE_MAX_LENGTH,
        "max_deliveries": MAX_DELIVERIES,
        "rate_limit": {
            "max": RATE_LIMIT_MAX,
            "window_seconds": RATE_LIMIT_WINDOW_SECONDS,
        },
        "worker_mode": "embedded" if EMBEDDED_WORKER else "external",
    }


@app.get("/dlq")
def list_dlq(limit: int = Query(20, ge=1, le=100)):
    """
    Peek recent dead-letter messages from the Kafka DLQ topic.

    Logic:
        - Seek near end of DLQ partitions and poll up to `limit` messages.

    Reason:
        Operators need to inspect poison payloads that exceeded MAX_DELIVERIES.
    """
    entries = peek_dlq(limit=limit)
    return {"count": len(entries), "entries": entries}


@app.get("/metrics", response_model=MetricsQueryResponse)
def query_metrics(
    machine_id: Optional[str] = Query(
        None, examples=["Jayakrishnans-MacBook-Air.local"]
    ),
    metric_name: Optional[str] = Query(None, examples=["cpu_usage"]),
    start_time: Optional[datetime] = Query(
        None, examples=["2026-06-14T19:00:00+00:00"]
    ),
    end_time: Optional[datetime] = Query(None, examples=["2026-06-14T20:00:00+00:00"]),
    limit: int = Query(100, ge=1, le=1000),
    db: Session = Depends(get_db),
):
    """
    Query stored metrics from PostgreSQL with optional filters.

    Logic:
        - Build a SELECT on MetricRecord.
        - Apply optional filters: machine_id, metric_name, start_time, end_time.
        - Order by timestamp ascending, cap rows with limit (default 100, max 1000).
        - Map ORM rows to MetricPoint response objects.

    Reason:
        Dashboards and alerts need time-range scans, not full table dumps.
        Filters match the composite index (machine_id, metric_name, timestamp).
        limit prevents accidentally loading millions of raw points in one request.
    """
    stmt = select(MetricRecord)

    if machine_id:
        stmt = stmt.where(MetricRecord.machine_id == machine_id)
    if metric_name:
        stmt = stmt.where(MetricRecord.metric_name == metric_name)
    if start_time:
        stmt = stmt.where(MetricRecord.timestamp >= start_time)
    if end_time:
        stmt = stmt.where(MetricRecord.timestamp <= end_time)

    stmt = stmt.order_by(MetricRecord.timestamp.asc()).limit(limit)

    rows = db.scalars(stmt).all()

    metrics = [
        MetricPoint(
            machine_id=row.machine_id,
            metric_name=row.metric_name,
            value=row.value,
            unit=row.unit,
            timestamp=row.timestamp,
            event_id=str(row.event_id) if row.event_id else None,
        )
        for row in rows
    ]

    return MetricsQueryResponse(count=len(metrics), metrics=metrics)


@app.post("/metrics", status_code=202)
def ingest_metrics(payload: MetricsPayload):
    """
    Accept metrics from agents — produce to Kafka for async persistence.

    Logic:
        - Validate body via Pydantic.
        - Per-machine_id rate limit (Day 6) → 429 if exceeded.
        - Produce JSON to insightnode.ingest (key=machine_id).
        - If lag too high → 503 backpressure.
        - Return 202 Accepted with approximate lag.

    Reason:
        Rate limit protects the pipeline from a runaway agent.
        Lag-based 503 protects when workers fall behind.
        202 = accepted for processing, not yet stored.
    """
    if kafka_producer is None:
        raise HTTPException(status_code=503, detail="Kafka producer not ready")

    if not ingest_rate_limiter.allow(payload.machine_id):
        logger.warning("Rate limit exceeded machine=%s", payload.machine_id)
        raise HTTPException(
            status_code=429,
            detail=(
                f"Rate limit exceeded: max {RATE_LIMIT_MAX} requests "
                f"per {RATE_LIMIT_WINDOW_SECONDS:.0f}s for this machine_id"
            ),
        )

    item = payload.model_dump(mode="json")

    try:
        enqueue_payload(kafka_producer, item)
    except QueueFullError:
        logger.warning("Kafka ingest lag too high — rejecting payload")
        raise HTTPException(
            status_code=503,
            detail="Ingest queue full, try again later",
        )

    depth = approximate_lag()
    logger.info(
        "Enqueued metrics machine=%s metric_count=%s lag=%s",
        payload.machine_id,
        len(payload.metrics),
        depth,
    )

    return {
        "status": "accepted",
        "machine_id": payload.machine_id,
        "metric_count": len(payload.metrics),
        "queued": depth,
    }


@app.get("/metrics/aggregate", response_model=MetricsAggregateResponse)
def query_metrics_aggregate(
    machine_id: str = Query(..., examples=["Jayakrishnans-MacBook-Air.local"]),
    metric_name: str = Query(..., examples=["cpu_usage"]),
    start_time: datetime = Query(..., examples=["2026-06-19T10:00:00+00:00"]),
    end_time: datetime = Query(..., examples=["2026-06-19T12:00:00+00:00"]),
    interval: Literal["1m", "5m", "15m", "1h", "3h", "6h", "12h", "24h", "1d"] = Query("5m"),
    db: Session = Depends(get_db),
):
    """
    Aggregate raw metrics into time buckets (avg, min, max per bucket).

    Logic:
        - Validate interval against INTERVAL_MAP.
        - Filter rows by machine_id, metric_name, and time range.
        - Group by date_bin(interval, timestamp) bucket.
        - Compute AVG, MIN, MAX, COUNT per bucket.
        - Order buckets chronologically.

    Reason:
        Dashboards and alerts need summarized series, not thousands of raw points.
        Query-time aggregation is simple and correct for Phase 1 scale.
    """
    if start_time >= end_time:
        raise HTTPException(status_code=400, detail="start_time must be before end_time")

    pg_interval = INTERVAL_MAP[interval]

    # date_bin needs a literal interval string — use text() for the width
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

    buckets = [
        MetricBucket(
        machine_id=row.machine_id,
        metric_name=row.metric_name,
        bucket_start=row.bucket_start,
        avg=round(float(row.avg), 2),
        min=round(float(row.min), 2),
        max=round(float(row.max), 2),
        sample_count=row.sample_count,
        )
        for row in rows
    ]

    return MetricsAggregateResponse(
        interval=interval,
        count=len(buckets),
        buckets=buckets,
    )