"""
Usage metering + simple monthly quotas (Phase 6 Day 4).

Rate limits (Day 3) throttle bursts. Quotas (Day 4) cap cumulative monthly usage —
the billable ceiling SaaS products sell.

Counters live in PostgreSQL so they survive API restarts (unlike in-memory rate limits).
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import date, datetime, timezone

from fastapi import HTTPException
from sqlalchemy import BigInteger, Date, DateTime, Integer, String, UniqueConstraint, func, select, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Mapped, mapped_column

from backend.database import Base, SessionLocal, engine

logger = logging.getLogger(__name__)

# Generous lab defaults — tighten via env or tenants.quota_* columns.
DEFAULT_QUOTA_METRIC_EVENTS = int(os.getenv("QUOTA_METRIC_EVENTS_MONTHLY", "100000"))
DEFAULT_QUOTA_LOG_EVENTS = int(os.getenv("QUOTA_LOG_EVENTS_MONTHLY", "100000"))
DEFAULT_QUOTA_METRIC_POINTS = int(os.getenv("QUOTA_METRIC_POINTS_MONTHLY", "500000"))


class TenantUsage(Base):
    """
    One usage row per tenant per calendar month (UTC).

    Logic:
        - period_start = first day of the month (UTC).
        - Counters are incremented on successful ingest accept.
        - Quotas compared before increment (reject → no charge).

    Reason:
        Monthly periods mirror typical SaaS billing. Storing counters in PG keeps
        metering durable across restarts — unlike Day 3's in-memory rate window.
    """

    __tablename__ = "tenant_usage"
    __table_args__ = (
        UniqueConstraint("tenant_id", "period_start", name="uq_tenant_usage_period"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    tenant_id: Mapped[str] = mapped_column(String(64), nullable=False)
    period_start: Mapped[date] = mapped_column(Date, nullable=False)
    metric_events: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    log_events: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    metric_points: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )


@dataclass(frozen=True)
class QuotaPlan:
    """Effective monthly ceilings for a tenant."""

    metric_events: int
    log_events: int
    metric_points: int


def period_start_utc(when: datetime | None = None) -> date:
    """First day of the current UTC calendar month."""
    now = when or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    else:
        now = now.astimezone(timezone.utc)
    return date(now.year, now.month, 1)


def ensure_usage_schema() -> None:
    """Create tenant_usage + optional quota columns on tenants (idempotent)."""
    TenantUsage.__table__.create(bind=engine, checkfirst=True)
    with engine.begin() as conn:
        for col in (
            "quota_metric_events INTEGER",
            "quota_log_events INTEGER",
            "quota_metric_points INTEGER",
        ):
            conn.execute(text(f"ALTER TABLE tenants ADD COLUMN IF NOT EXISTS {col}"))
    logger.info("Tenant usage / quota schema ready")


def effective_quotas(
    *,
    quota_metric_events: int | None,
    quota_log_events: int | None,
    quota_metric_points: int | None,
) -> QuotaPlan:
    return QuotaPlan(
        metric_events=(
            int(quota_metric_events)
            if quota_metric_events is not None
            else DEFAULT_QUOTA_METRIC_EVENTS
        ),
        log_events=(
            int(quota_log_events)
            if quota_log_events is not None
            else DEFAULT_QUOTA_LOG_EVENTS
        ),
        metric_points=(
            int(quota_metric_points)
            if quota_metric_points is not None
            else DEFAULT_QUOTA_METRIC_POINTS
        ),
    )


def get_usage(tenant_id: str, *, period: date | None = None) -> dict:
    """Return usage + quotas for a tenant's period (zeros if no row yet)."""
    from backend.tenancy import Tenant  # avoid circular import at module load

    period = period or period_start_utc()
    db = SessionLocal()
    try:
        tenant = db.get(Tenant, tenant_id)
        quotas = effective_quotas(
            quota_metric_events=getattr(tenant, "quota_metric_events", None) if tenant else None,
            quota_log_events=getattr(tenant, "quota_log_events", None) if tenant else None,
            quota_metric_points=getattr(tenant, "quota_metric_points", None) if tenant else None,
        )
        row = db.scalar(
            select(TenantUsage).where(
                TenantUsage.tenant_id == tenant_id,
                TenantUsage.period_start == period,
            )
        )
        usage = {
            "metric_events": int(row.metric_events) if row else 0,
            "log_events": int(row.log_events) if row else 0,
            "metric_points": int(row.metric_points) if row else 0,
        }
        return {
            "tenant_id": tenant_id,
            "period_start": period.isoformat(),
            "usage": usage,
            "quotas": {
                "metric_events": quotas.metric_events,
                "log_events": quotas.log_events,
                "metric_points": quotas.metric_points,
            },
            "remaining": {
                "metric_events": max(0, quotas.metric_events - usage["metric_events"]),
                "log_events": max(0, quotas.log_events - usage["log_events"]),
                "metric_points": max(0, quotas.metric_points - usage["metric_points"]),
            },
        }
    finally:
        db.close()


def check_quota(
    *,
    tenant_id: str,
    quotas: QuotaPlan,
    metric_events: int = 0,
    log_events: int = 0,
    metric_points: int = 0,
) -> None:
    """
    Raise HTTP 402 if this delta would exceed the monthly plan (no increment).
    """
    if metric_events < 0 or log_events < 0 or metric_points < 0:
        raise ValueError("usage deltas must be non-negative")

    period = period_start_utc()
    db = SessionLocal()
    try:
        row = db.scalar(
            select(TenantUsage).where(
                TenantUsage.tenant_id == tenant_id,
                TenantUsage.period_start == period,
            )
        )
        current_me = int(row.metric_events) if row else 0
        current_le = int(row.log_events) if row else 0
        current_mp = int(row.metric_points) if row else 0

        violations: list[str] = []
        if current_me + metric_events > quotas.metric_events:
            violations.append(
                f"metric_events {current_me + metric_events}/{quotas.metric_events}"
            )
        if current_le + log_events > quotas.log_events:
            violations.append(
                f"log_events {current_le + log_events}/{quotas.log_events}"
            )
        if current_mp + metric_points > quotas.metric_points:
            violations.append(
                f"metric_points {current_mp + metric_points}/{quotas.metric_points}"
            )

        if violations:
            logger.warning(
                "Quota exceeded tenant=%s period=%s %s",
                tenant_id,
                period,
                "; ".join(violations),
            )
            raise HTTPException(
                status_code=402,
                detail=(
                    f"Monthly quota exceeded for tenant {tenant_id}: "
                    + "; ".join(violations)
                ),
            )
    finally:
        db.close()


def record_usage(
    *,
    tenant_id: str,
    metric_events: int = 0,
    log_events: int = 0,
    metric_points: int = 0,
) -> dict:
    """
    Increment monthly counters after a successful accept (Phase 6 Day 4).

    Logic:
        - Upsert (tenant_id, period) then lock and add deltas.
        - Call only after Kafka/OpenSearch accept so failed requests are not billed.

    Reason:
        Separating check vs record avoids charging 503/502 failures; a tiny race
        under concurrency is acceptable for local learning (billing systems use
        stronger ledger semantics).
    """
    if metric_events == 0 and log_events == 0 and metric_points == 0:
        return {}

    period = period_start_utc()
    db = SessionLocal()
    try:
        db.execute(
            insert(TenantUsage)
            .values(
                tenant_id=tenant_id,
                period_start=period,
                metric_events=0,
                log_events=0,
                metric_points=0,
            )
            .on_conflict_do_nothing(index_elements=["tenant_id", "period_start"])
        )
        db.flush()

        row = db.scalar(
            select(TenantUsage)
            .where(
                TenantUsage.tenant_id == tenant_id,
                TenantUsage.period_start == period,
            )
            .with_for_update()
        )
        if row is None:
            raise RuntimeError("tenant_usage row missing after upsert")

        row.metric_events += metric_events
        row.log_events += log_events
        row.metric_points += metric_points
        db.commit()

        return {
            "period_start": period.isoformat(),
            "metric_events": int(row.metric_events),
            "log_events": int(row.log_events),
            "metric_points": int(row.metric_points),
        }
    except Exception:
        db.rollback()
        logger.exception("Usage metering failed tenant=%s", tenant_id)
        raise
    finally:
        db.close()
