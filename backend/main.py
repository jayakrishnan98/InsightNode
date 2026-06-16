from datetime import datetime
from typing import Optional

from fastapi import Depends, FastAPI, Query
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.orm import Session

from backend.database import get_db
from backend.models import MetricRecord

app = FastAPI(title="InsightNode", version="0.1.0")


class Metric(BaseModel):
    name: str = Field(..., min_length=1, examples=["cpu_usage"])
    value: float = Field(..., examples=[45.2])
    unit: str = Field(..., min_length=1, examples=["percent"])


class MetricsPayload(BaseModel):
    machine_id: str = Field(
        ..., min_length=1, examples=["Jayakrishnans-MacBook-Air.local"]
    )
    timestamp: datetime = Field(..., examples=["2026-06-14T08:59:35.550356+00:00"])
    metrics: list[Metric] = Field(..., min_length=1)


class MetricPoint(BaseModel):
    machine_id: str
    metric_name: str
    value: float
    unit: str
    timestamp: datetime


class MetricsQueryResponse(BaseModel):
    count: int
    metrics: list[MetricPoint]


@app.get("/health")
def health_check():
    return {"status": "ok"}


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


@app.post("/metrics", status_code=201)
def ingest_metrics(payload: MetricsPayload, db: Session = Depends(get_db)):
    rows = [
        MetricRecord(
            machine_id=payload.machine_id,
            metric_name=metric.name,
            value=metric.value,
            unit=metric.unit,
            timestamp=payload.timestamp,
        )
        for metric in payload.metrics
    ]

    db.add_all(rows)
    db.commit()

    print(f"[INGEST] machine={payload.machine_id} time={payload.timestamp.isoformat()}")
    for metric in payload.metrics:
        print(f"  - {metric.name}: {metric.value} {metric.unit}")

    return {
        "status": "accepted",
        "machine_id": payload.machine_id,
        "metric_count": len(payload.metrics),
    }
