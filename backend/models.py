"""
SQLAlchemy ORM models — maps Python classes to PostgreSQL tables.
"""

from datetime import datetime

from sqlalchemy import BigInteger, DateTime, Float, String, func
from sqlalchemy.orm import Mapped, mapped_column

from backend.database import Base


class MetricRecord(Base):
    """
    One stored metric sample — one row per (machine, metric name, timestamp, value).

    Logic (schema):
        - id: surrogate primary key (BIGSERIAL) for efficient row identity.
        - machine_id + metric_name + timestamp: natural query dimensions.
        - value + unit: the observed gauge reading.
        - created_at: server-side insert time (when the API persisted the row).

    Reason:
        Observability data is append-only: we insert new samples, rarely update.
        Separating timestamp (agent observation time) from created_at (ingest time)
        helps debug clock skew and queue lag. Indexes on timestamp and
        (machine_id, metric_name, timestamp) support the GET /metrics query pattern.
    """
    __tablename__ = "metrics"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    machine_id: Mapped[str] = mapped_column(String(255), nullable=False)
    metric_name: Mapped[str] = mapped_column(String(255), nullable=False)
    value: Mapped[float] = mapped_column(Float, nullable=False)
    unit: Mapped[str] = mapped_column(String(50), nullable=False)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )