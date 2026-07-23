"""
Alert events from Grafana Unified Alerting webhooks (Phase 7).

Grafana owns rule evaluation; InsightNode stores firing/resolved events for
the custom UI and interview demos. Duplicate firings are ignored via
UNIQUE (fingerprint, starts_at).
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import (
    BigInteger,
    DateTime,
    String,
    Text,
    UniqueConstraint,
    func,
    select,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, insert
from sqlalchemy.orm import Mapped, Session, mapped_column

from backend.database import Base, engine

logger = logging.getLogger(__name__)

GRAFANA_WEBHOOK_SECRET = os.getenv("GRAFANA_WEBHOOK_SECRET", "dev-webhook-secret")
DEFAULT_ALERT_TENANT_ID = os.getenv("DEFAULT_TENANT_ID", "local")


class AlertEvent(Base):
    """One Grafana alert instance (firing or resolved)."""

    __tablename__ = "alert_events"
    __table_args__ = (
        UniqueConstraint(
            "fingerprint",
            "starts_at",
            name="uq_alert_events_fingerprint_starts",
        ),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    tenant_id: Mapped[str] = mapped_column(String(64), nullable=False, default="local")
    fingerprint: Mapped[str] = mapped_column(String(128), nullable=False)
    rule_name: Mapped[str] = mapped_column(String(255), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    severity: Mapped[str | None] = mapped_column(String(32), nullable=True)
    machine_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    metric_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    starts_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    ends_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    raw_payload: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


def ensure_alert_events_schema() -> None:
    """Idempotently create alert_events (fresh labs + existing DBs)."""
    AlertEvent.__table__.create(bind=engine, checkfirst=True)
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE INDEX IF NOT EXISTS idx_alert_events_tenant_status
                ON alert_events (tenant_id, status, starts_at DESC)
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE INDEX IF NOT EXISTS idx_alert_events_fingerprint
                ON alert_events (fingerprint)
                """
            )
        )
    logger.info("PostgreSQL alert_events schema ready")


def _parse_dt(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value
    text_val = str(value).strip()
    if not text_val or text_val.startswith("0001-01-01"):
        return None
    if text_val.endswith("Z"):
        text_val = text_val[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text_val)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _alert_fields(alert: dict[str, Any]) -> dict[str, Any]:
    labels = alert.get("labels") or {}
    annotations = alert.get("annotations") or {}
    rule_name = (
        labels.get("alertname")
        or alert.get("labels", {}).get("rulename")
        or "unknown"
    )
    summary = (
        annotations.get("summary")
        or annotations.get("description")
        or rule_name
    )
    starts_at = _parse_dt(alert.get("startsAt")) or datetime.now(timezone.utc)
    ends_at = _parse_dt(alert.get("endsAt"))
    status = str(alert.get("status") or "firing").lower()
    if status not in ("firing", "resolved"):
        status = "firing"

    return {
        "tenant_id": DEFAULT_ALERT_TENANT_ID,
        "fingerprint": str(alert.get("fingerprint") or f"{rule_name}:{starts_at.isoformat()}"),
        "rule_name": str(rule_name)[:255],
        "status": status,
        "severity": (str(labels["severity"])[:32] if labels.get("severity") else None),
        "machine_id": (
            str(labels["machine_id"])[:255] if labels.get("machine_id") else None
        ),
        "metric_name": (
            str(labels["metric_name"])[:255] if labels.get("metric_name") else None
        ),
        "summary": str(summary) if summary else None,
        "starts_at": starts_at,
        "ends_at": ends_at,
        "raw_payload": alert,
    }


def apply_grafana_alert(db: Session, alert: dict[str, Any]) -> str:
    """
    Persist one Grafana alert instance.

    Returns: 'inserted' | 'duplicate' | 'resolved' | 'resolve_inserted'
    """
    fields = _alert_fields(alert)

    if fields["status"] == "resolved":
        existing = db.execute(
            select(AlertEvent).where(
                AlertEvent.fingerprint == fields["fingerprint"],
                AlertEvent.starts_at == fields["starts_at"],
            )
        ).scalar_one_or_none()

        if existing is not None:
            existing.status = "resolved"
            existing.ends_at = fields["ends_at"] or datetime.now(timezone.utc)
            existing.summary = fields["summary"] or existing.summary
            existing.raw_payload = fields["raw_payload"]
            db.commit()
            return "resolved"

        # Resolve without a prior firing row (missed webhook) — still record it.
        db.add(AlertEvent(**fields))
        db.commit()
        return "resolve_inserted"

    stmt = (
        insert(AlertEvent)
        .values(**fields)
        .on_conflict_do_nothing(index_elements=["fingerprint", "starts_at"])
    )
    result = db.execute(stmt)
    db.commit()
    if result.rowcount and result.rowcount > 0:
        return "inserted"
    return "duplicate"


def process_grafana_webhook(db: Session, payload: dict[str, Any]) -> dict[str, int]:
    """
    Handle a Grafana Unified Alerting webhook body.

    Logic:
        - Iterate payload['alerts'] (or treat the body as a single alert).
        - Firing → insert (dedupe on fingerprint+starts_at).
        - Resolved → update matching row or insert resolved.
    """
    alerts = payload.get("alerts")
    if not isinstance(alerts, list):
        alerts = [payload]

    counts = {
        "received": 0,
        "inserted": 0,
        "duplicate": 0,
        "resolved": 0,
        "resolve_inserted": 0,
    }
    for alert in alerts:
        if not isinstance(alert, dict):
            continue
        counts["received"] += 1
        action = apply_grafana_alert(db, alert)
        counts[action] = counts.get(action, 0) + 1

    logger.info("Grafana webhook processed %s", counts)
    return counts


def list_alert_events(
    db: Session,
    *,
    tenant_id: str,
    status: str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> list[AlertEvent]:
    """Newest-first alert events for a tenant."""
    stmt = select(AlertEvent).where(AlertEvent.tenant_id == tenant_id)
    if status:
        stmt = stmt.where(AlertEvent.status == status)
    stmt = (
        stmt.order_by(AlertEvent.starts_at.desc(), AlertEvent.id.desc())
        .limit(limit)
        .offset(offset)
    )
    return list(db.execute(stmt).scalars().all())


def count_open_alerts(db: Session, *, tenant_id: str) -> int:
    """Number of currently firing alerts for a tenant."""
    stmt = (
        select(func.count())
        .select_from(AlertEvent)
        .where(
            AlertEvent.tenant_id == tenant_id,
            AlertEvent.status == "firing",
        )
    )
    return int(db.execute(stmt).scalar_one())


def verify_webhook_secret(authorization: str | None) -> bool:
    """True if Authorization Bearer matches GRAFANA_WEBHOOK_SECRET."""
    if not authorization:
        return False
    parts = authorization.split(None, 1)
    if len(parts) != 2:
        return False
    scheme, token = parts[0], parts[1]
    if scheme.lower() != "bearer":
        return False
    return token == GRAFANA_WEBHOOK_SECRET
