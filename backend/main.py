"""
InsightNode API — metrics, logs, and OpenTelemetry traces (Phase 5 complete).

Architecture (Phase 5):
    Agent → FastAPI (HTTP spans) → Kafka (W3C headers) → worker (dual-write spans).
    Spans export via OTLP → Jaeger; ops logs may carry attrs.trace_id.
"""

from datetime import datetime
from typing import Any, Optional, Literal
import logging
import os
import statistics
import time

from fastapi import Depends, FastAPI, Query, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.orm import Session
from contextlib import asynccontextmanager

from backend.database import get_db
from backend.models import MetricRecord
from backend.clickhouse_client import (
    close_client as close_clickhouse,
    ensure_schema as ensure_clickhouse_schema,
    ping as clickhouse_ping,
    query_aggregate as clickhouse_query_aggregate,
)
from backend.opensearch_client import (
    close_client as close_opensearch,
    ensure_index as ensure_opensearch_index,
    get_log as opensearch_get_log,
    index_logs as opensearch_index_logs,
    ping as opensearch_ping,
    search_logs as opensearch_search_logs,
)
from backend.postgres_aggregate import query_aggregate as postgres_query_aggregate
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
from backend import logship as backend_logship
from backend.tracing import (
    instrument_fastapi,
    ping as jaeger_ping,
    setup_tracing,
    shutdown_tracing,
)

logger = logging.getLogger(__name__)
kafka_producer = None  # set in lifespan

# Optional: set EMBEDDED_WORKER=1 to run a worker thread inside the API (dev only).
EMBEDDED_WORKER = os.getenv("EMBEDDED_WORKER", "0") == "1"


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    API lifespan: Kafka + ClickHouse + OpenSearch + tracing bootstrap.

    Logic:
        - ensure_topics / ClickHouse schema / OpenSearch index.
        - setup_tracing() in lifespan (idempotent if already done at import).
        - FastAPI instrumentation is applied at import time (Day 2).
        - Optional embedded Kafka worker.
    """
    global kafka_producer

    ensure_topics()
    kafka_producer = get_producer()
    logger.info("Kafka producer ready")

    ensure_clickhouse_schema()
    logger.info("ClickHouse schema ready")

    ensure_opensearch_index()
    logger.info("OpenSearch logs index ready")

    if setup_tracing():
        logger.info("OpenTelemetry tracing ready")
    else:
        logger.info("OpenTelemetry tracing skipped")

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
    close_opensearch()
    shutdown_tracing()


app = FastAPI(title="InsightNode", version="0.9.0", lifespan=lifespan)

# Phase 5 Day 2: instrument BEFORE the ASGI server starts (cannot add middleware later).
if setup_tracing():
    instrument_fastapi(app)

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


class StoreTiming(BaseModel):
    """Timing stats for one store over N runs."""
    store: str
    runs: int
    ms_min: float
    ms_median: float
    ms_max: float
    bucket_count: int
    sample_total: int


class AggregateCompareResponse(BaseModel):
    """Side-by-side PostgreSQL vs ClickHouse aggregate timing (Phase 3 Day 4)."""
    interval: str
    runs: int
    postgres: StoreTiming
    clickhouse: StoreTiming
    speedup_median: float | None  # postgres_median / clickhouse_median
    buckets_match: bool
    sample_totals_match: bool
    notes: list[str]
    postgres_buckets: list[MetricBucket] | None = None
    clickhouse_buckets: list[MetricBucket] | None = None


class LogEvent(BaseModel):
    """One structured log line for OpenSearch (Phase 4 Day 2)."""
    event_id: str = Field(
        ...,
        min_length=36,
        max_length=36,
        examples=["550e8400-e29b-41d4-a716-446655440000"],
    )
    machine_id: str = Field(..., min_length=1, examples=["Jayakrishnans-MacBook-Air.local"])
    service: str = Field(default="unknown", min_length=1, examples=["agent"])
    level: Literal["debug", "info", "warn", "error"] = Field(
        default="info", examples=["info"]
    )
    message: str = Field(..., min_length=1, examples=["CPU sample collected"])
    timestamp: datetime = Field(..., examples=["2026-07-23T08:00:00+00:00"])
    attrs: dict[str, Any] = Field(default_factory=dict)


class LogsPayload(BaseModel):
    """Batch of log events for POST /logs."""
    logs: list[LogEvent] = Field(..., min_length=1)


class LogsIngestResponse(BaseModel):
    """Result of indexing logs into OpenSearch."""
    status: str
    indexed: int
    ids: list[str]


class LogSearchHit(BaseModel):
    """One hit from GET /logs/search."""
    event_id: str
    machine_id: str
    service: str
    level: str
    message: str
    timestamp: datetime | str
    attrs: dict[str, Any] = Field(default_factory=dict)
    score: float | None = None


class LogsSearchResponse(BaseModel):
    """Wrapper for OpenSearch log search results."""
    total: int
    count: int
    logs: list[LogSearchHit]


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
        "opensearch_ok": opensearch_ping(),
        "jaeger_ok": jaeger_ping(),
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


@app.post("/logs", response_model=LogsIngestResponse, status_code=202)
def ingest_logs(payload: LogsPayload):
    """
    Accept structured logs and index them into OpenSearch (Phase 4 Day 2).

    Logic:
        - Validate batch via Pydantic.
        - Bulk-index into insightnode-logs using event_id as document _id.
        - Return 202 with indexed count + ids.
        - refresh=True so immediate GET /logs/{event_id} works in labs.

    Reason:
        Logs are text events, not gauges — OpenSearch is the right store.
        Direct index (no Kafka yet) keeps Day 2 focused on the document model.
        Day 4 can add agent shipping; a Kafka log topic can come later if needed.
    """
    docs = [log.model_dump(mode="json") for log in payload.logs]
    try:
        result = opensearch_index_logs(docs, refresh=True)
    except Exception:
        logger.exception("OpenSearch log ingest failed")
        raise HTTPException(status_code=503, detail="OpenSearch log ingest failed")

    if result["errors"]:
        raise HTTPException(status_code=502, detail="OpenSearch bulk index had errors")

    return LogsIngestResponse(
        status="accepted",
        indexed=result["indexed"],
        ids=result["ids"],
    )


@app.get("/logs/search", response_model=LogsSearchResponse)
def search_logs(
    q: Optional[str] = Query(
        None,
        description="Full-text query against message (AND operator)",
        examples=["disk usage"],
    ),
    machine_id: Optional[str] = Query(None, examples=["my-machine"]),
    service: Optional[str] = Query(None, examples=["agent"]),
    level: Optional[Literal["debug", "info", "warn", "error"]] = Query(None),
    start_time: Optional[datetime] = Query(None, examples=["2026-07-23T00:00:00+00:00"]),
    end_time: Optional[datetime] = Query(None, examples=["2026-07-24T00:00:00+00:00"]),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0, le=10_000),
):
    """
    Search logs in OpenSearch (Phase 4 Day 3).

    Logic:
        - Optional full-text `q` on message (scored).
        - Optional exact filters: machine_id, service, level.
        - Optional time range on timestamp.
        - Newest first; paginate with limit/offset.

    Reason:
        Finding "warn disk on host X" is the core log use case — not SQL LIKE.
        Must register this route before /logs/{event_id} so "search" is not an id.
    """
    if start_time is not None and end_time is not None and start_time >= end_time:
        raise HTTPException(status_code=400, detail="start_time must be before end_time")

    try:
        result = opensearch_search_logs(
            q=q,
            machine_id=machine_id,
            service=service,
            level=level,
            start_time=start_time,
            end_time=end_time,
            limit=limit,
            offset=offset,
        )
    except Exception:
        logger.exception("OpenSearch log search failed")
        raise HTTPException(status_code=503, detail="OpenSearch log search failed")

    return LogsSearchResponse(
        total=result["total"],
        count=result["count"],
        logs=[LogSearchHit(**hit) for hit in result["logs"]],
    )


@app.get("/logs/{event_id}")
def get_log_by_event_id(event_id: str):
    """
    Fetch one log document by event_id (OpenSearch _id).

    Day 2 verification helper — full-text search arrives in Day 3.
    """
    doc = opensearch_get_log(event_id)
    if doc is None:
        raise HTTPException(status_code=404, detail="Log not found")
    return doc


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
        backend_logship.warn(
            "api",
            "Ingest rate limit exceeded",
            machine_id=payload.machine_id,
            rate_limit_max=RATE_LIMIT_MAX,
            window_seconds=RATE_LIMIT_WINDOW_SECONDS,
        )
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
        backend_logship.warn(
            "api",
            "Kafka ingest lag too high — rejecting payload",
            machine_id=payload.machine_id,
            event_id=payload.event_id,
            queue_maxsize=QUEUE_MAX_LENGTH,
        )
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
):
    """
    Aggregate raw metrics into time buckets via ClickHouse (Phase 3 Day 3).

    Logic:
        - Validate start < end.
        - Delegate to clickhouse_query_aggregate (toStartOfInterval + avg/min/max).
        - Return the same MetricsAggregateResponse shape as Phase 1/2.

    Reason:
        Dashboards need summarized series. Columnar storage is the right read path
        for aggregates; raw GET /metrics stays on PostgreSQL.
    """
    if start_time >= end_time:
        raise HTTPException(status_code=400, detail="start_time must be before end_time")

    try:
        rows = clickhouse_query_aggregate(
            machine_id=machine_id,
            metric_name=metric_name,
            start_time=start_time,
            end_time=end_time,
            interval=interval,
        )
    except Exception:
        logger.exception("ClickHouse aggregate query failed")
        raise HTTPException(status_code=503, detail="ClickHouse aggregate query failed")

    buckets = [
        MetricBucket(
            machine_id=row["machine_id"],
            metric_name=row["metric_name"],
            bucket_start=row["bucket_start"],
            avg=round(float(row["avg"]), 2),
            min=round(float(row["min"]), 2),
            max=round(float(row["max"]), 2),
            sample_count=row["sample_count"],
        )
        for row in rows
    ]

    return MetricsAggregateResponse(
        interval=interval,
        count=len(buckets),
        buckets=buckets,
    )


def _rows_to_buckets(rows: list[dict]) -> list[MetricBucket]:
    return [
        MetricBucket(
            machine_id=row["machine_id"],
            metric_name=row["metric_name"],
            bucket_start=row["bucket_start"],
            avg=round(float(row["avg"]), 2),
            min=round(float(row["min"]), 2),
            max=round(float(row["max"]), 2),
            sample_count=row["sample_count"],
        )
        for row in rows
    ]


def _time_runs(fn, runs: int) -> tuple[list[float], list[dict]]:
    """Run fn() `runs` times; return (elapsed_ms list, last result rows)."""
    timings_ms: list[float] = []
    last: list[dict] = []
    for _ in range(runs):
        t0 = time.perf_counter()
        last = fn()
        timings_ms.append((time.perf_counter() - t0) * 1000.0)
    return timings_ms, last


@app.get("/metrics/aggregate/compare", response_model=AggregateCompareResponse)
def compare_metrics_aggregate(
    machine_id: str = Query(..., examples=["compare-bench"]),
    metric_name: str = Query(..., examples=["cpu_usage"]),
    start_time: datetime = Query(..., examples=["2026-07-01T00:00:00+00:00"]),
    end_time: datetime = Query(..., examples=["2026-07-23T00:00:00+00:00"]),
    interval: Literal["1m", "5m", "15m", "1h", "3h", "6h", "12h", "24h", "1d"] = Query("5m"),
    runs: int = Query(3, ge=1, le=20),
    include_buckets: bool = Query(False),
    db: Session = Depends(get_db),
):
    """
    Run the same aggregate on PostgreSQL and ClickHouse; report timings (Day 4).

    Logic:
        - Execute each store `runs` times; record min / median / max ms.
        - Compare bucket counts and sum(sample_count).
        - Optionally return both bucket series for inspection.

    Reason:
        Feeling why columnar wins for analytics requires measuring the same
        query on both engines against enough rows (see tests/load/seed_aggregate_compare.py).
    """
    if start_time >= end_time:
        raise HTTPException(status_code=400, detail="start_time must be before end_time")

    try:
        pg_ms, pg_rows = _time_runs(
            lambda: postgres_query_aggregate(
                db,
                machine_id=machine_id,
                metric_name=metric_name,
                start_time=start_time,
                end_time=end_time,
                interval=interval,
            ),
            runs,
        )
        ch_ms, ch_rows = _time_runs(
            lambda: clickhouse_query_aggregate(
                machine_id=machine_id,
                metric_name=metric_name,
                start_time=start_time,
                end_time=end_time,
                interval=interval,
            ),
            runs,
        )
    except Exception:
        logger.exception("Aggregate compare failed")
        raise HTTPException(status_code=503, detail="Aggregate compare failed")

    pg_buckets = _rows_to_buckets(pg_rows)
    ch_buckets = _rows_to_buckets(ch_rows)
    pg_samples = sum(b.sample_count for b in pg_buckets)
    ch_samples = sum(b.sample_count for b in ch_buckets)

    pg_median = statistics.median(pg_ms)
    ch_median = statistics.median(ch_ms)
    speedup = round(pg_median / ch_median, 2) if ch_median > 0 else None

    notes: list[str] = [
        "Production /metrics/aggregate still uses ClickHouse only.",
        "Warm both stores with a few compare calls; first run includes cold-start noise.",
        "Seed larger data with: python tests/load/seed_aggregate_compare.py",
    ]
    if pg_samples != ch_samples:
        notes.append(
            "sample_totals differ — often ClickHouse duplicates from at-least-once dual-write."
        )

    return AggregateCompareResponse(
        interval=interval,
        runs=runs,
        postgres=StoreTiming(
            store="postgresql",
            runs=runs,
            ms_min=round(min(pg_ms), 3),
            ms_median=round(pg_median, 3),
            ms_max=round(max(pg_ms), 3),
            bucket_count=len(pg_buckets),
            sample_total=pg_samples,
        ),
        clickhouse=StoreTiming(
            store="clickhouse",
            runs=runs,
            ms_min=round(min(ch_ms), 3),
            ms_median=round(ch_median, 3),
            ms_max=round(max(ch_ms), 3),
            bucket_count=len(ch_buckets),
            sample_total=ch_samples,
        ),
        speedup_median=speedup,
        buckets_match=len(pg_buckets) == len(ch_buckets),
        sample_totals_match=pg_samples == ch_samples,
        notes=notes,
        postgres_buckets=pg_buckets if include_buckets else None,
        clickhouse_buckets=ch_buckets if include_buckets else None,
    )
