"""
SQLAlchemy ORM models — maps Python classes to PostgreSQL tables.
"""

from datetime import datetime

from sqlalchemy import BigInteger, DateTime, Float, String, Uuid, func
from uuid import UUID
from sqlalchemy.orm import Mapped, mapped_column

from backend.database import Base


class MetricRecord(Base):
    """
    One stored metric sample — one row per (tenant, machine, metric, timestamp).

    Logic (schema):
        - id: surrogate primary key (BIGSERIAL) for efficient row identity.
        - tenant_id: owning customer (Phase 6 Day 2 isolation).
        - machine_id + metric_name + timestamp: natural query dimensions.
        - value + unit: the observed gauge reading.
        - event_id: client-generated idempotency key (one per agent collection).
        - created_at: server-side insert time (when the API persisted the row).

    Reason:
        Observability data is append-only: we insert new samples, rarely update.
        Separating timestamp (agent observation time) from created_at (ingest time)
        helps debug clock skew and queue lag. Dedup is per-tenant so two orgs
        cannot collide on the same (machine, event_id, metric) triple.
    """
    __tablename__ = "metrics"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    tenant_id: Mapped[str] = mapped_column(String(64), nullable=False, default="local")
    machine_id: Mapped[str] = mapped_column(String(255), nullable=False)
    metric_name: Mapped[str] = mapped_column(String(255), nullable=False)
    value: Mapped[float] = mapped_column(Float, nullable=False)
    unit: Mapped[str] = mapped_column(String(50), nullable=False)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    event_id: Mapped[UUID | None] = mapped_column(Uuid, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
