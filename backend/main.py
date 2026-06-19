"""
InsightNode API — ingestion and query endpoints for host metrics.

Architecture:
    Agent --POST /metrics--> in-memory queue --worker thread--> PostgreSQL
                              (fast 202)                    (batched writes)

The HTTP handler does not write to the database directly. That decouples accept
latency from PostgreSQL commit time and absorbs short write spikes in the queue.
"""

from datetime import datetime
from typing import Optional

import logging
import threading
from contextlib import asynccontextmanager
from queue import Full, Queue

from fastapi import Depends, FastAPI, Query
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.orm import Session

from backend.database import get_db
from backend.models import MetricRecord

logger = logging.getLogger(__name__)
ingest_queue: Queue = Queue(maxsize=10000)
stop_event = threading.Event()
worker_thread: threading.Thread | None = None

@asynccontextmanager
async def lifespan(app: FastAPI):
  """
  Start and stop the background ingest worker with the FastAPI application.

  Logic:
      - On startup: clear stop_event, spawn a daemon thread running run_ingest_worker.
      - yield — app serves traffic while the worker drains the queue.
      - On shutdown: set stop_event, join the worker (up to 5s) so it can finish.

  Reason:
      The worker must share the same process and ingest_queue as the API handlers.
      FastAPI lifespan is the supported hook for this; without it, POST /metrics
      would enqueue data that nothing consumes.
  """
  global worker_thread

  from backend.worker import run_ingest_worker

  stop_event.clear()
  worker_thread = threading.Thread(
    target=run_ingest_worker,
    args=(ingest_queue, stop_event),
    name="ingest-worker",
    daemon=True,
  )
  worker_thread.start()
  logger.info("Ingest worker started")

  yield

  stop_event.set()
  if worker_thread is not None:
    worker_thread.join(timeout=5.0)
    logger.info("Ingest worker stopped")


app = FastAPI(title="InsightNode", version="0.1.0", lifespan=lifespan)

class Metric(BaseModel):
    """Single metric reading inside an ingestion payload (name, value, unit)."""
    name: str = Field(..., min_length=1, examples=["cpu_usage"])
    value: float = Field(..., examples=[45.2])
    unit: str = Field(..., min_length=1, examples=["percent"])


class MetricsPayload(BaseModel):
    """Request body for POST /metrics — one timestamp, many metrics for one machine."""
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


class MetricsQueryResponse(BaseModel):
    """Wrapper for query results: total count plus list of metric points."""
    count: int
    metrics: list[MetricPoint]


@app.get("/health")
def health_check():
  """
  Liveness probe and ingest pipeline visibility.

  Logic:
      - Return static status "ok".
      - Expose current queue depth, max capacity, and whether the worker thread is alive.

  Reason:
      Operators (and future you) need to see backpressure before agents time out.
      A growing queue_size with worker_alive=false signals a stuck or crashed worker.
  """
  return {
    "status": "ok",
    "queue_size": ingest_queue.qsize(),
    "queue_maxsize": ingest_queue.maxsize,
    "worker_alive": worker_thread.is_alive() if worker_thread else False,
  }


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
        )
        for row in rows
    ]

    return MetricsQueryResponse(count=len(metrics), metrics=metrics)


@app.post("/metrics", status_code=202)
def ingest_metrics(payload: MetricsPayload):
  """
  Accept metrics from agents — enqueue for async persistence, respond immediately.

  Logic:
      - Validate body via Pydantic (MetricsPayload).
      - Serialize to a JSON-safe dict; stamp _retry_count=0 for worker retry tracking.
      - Non-blocking put on ingest_queue; if full, return 503 (backpressure).
      - Return 202 Accepted with queue depth (data not yet in PostgreSQL).

  Reason:
      202 means "accepted for processing," not "stored." Agents get fast ACKs even
      when PostgreSQL is slow. block=False avoids hanging HTTP threads when the
      queue is full — the agent should retry or spool instead.
  """
  # model_dump(mode="json") → JSON-safe dict (datetime → ISO string)
  item = payload.model_dump(mode="json")
  item["_retry_count"] = 0

  try:
    ingest_queue.put(item, block=False)
  except Full:
    logger.warning("Ingest queue full — rejecting payload")
    from fastapi import HTTPException
    raise HTTPException(status_code=503, detail="Ingest queue full, try again later")

  logger.info(
    "Enqueued metrics machine=%s metric_count=%s queue_size=%s",
    payload.machine_id,
    len(payload.metrics),
    ingest_queue.qsize(),
  )

  return {
    "status": "accepted",
    "machine_id": payload.machine_id,
    "metric_count": len(payload.metrics),
    "queued": ingest_queue.qsize(),
  }
