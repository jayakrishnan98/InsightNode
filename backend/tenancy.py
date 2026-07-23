"""
Multi-tenancy foundation (Phase 6 Day 1).

SaaS observability platforms isolate customers by tenant. Day 1 introduces:
  - a `tenants` registry in PostgreSQL
  - API-key authentication (`X-API-Key`) that resolves to a tenant
  - stamping `tenant_id` onto ingest payloads (storage isolation is Day 2)

Learning mode seeds a default local tenant so agents keep working out of the box.
"""

from __future__ import annotations

import logging
import os
import secrets
from dataclasses import dataclass
from datetime import datetime

from fastapi import Header, HTTPException
from sqlalchemy import Boolean, DateTime, String, Text, func, select
from sqlalchemy.orm import Mapped, mapped_column

from backend.database import Base, SessionLocal, engine

logger = logging.getLogger(__name__)

# Soft learning defaults — override in real deployments.
DEFAULT_TENANT_ID = os.getenv("DEFAULT_TENANT_ID", "local")
DEFAULT_TENANT_NAME = os.getenv("DEFAULT_TENANT_NAME", "Local Dev")
DEFAULT_API_KEY = os.getenv("DEFAULT_API_KEY", "dev-local-key")
# When 0, missing/invalid keys fall back to the default tenant (lab convenience).
TENANCY_STRICT = os.getenv("TENANCY_STRICT", "0") == "1"


class Tenant(Base):
    """
    One customer / org in InsightNode.

    Logic:
        - tenant_id: stable public identifier (used in payloads + later sharding).
        - api_key: shared secret agents send as X-API-Key.
        - active: soft-disable without deleting historical data.

    Reason:
        Day 1 teaches identity before quotas/metering. Plain api_key is fine for
        local learning; production would store hashes and rotate keys.
    """

    __tablename__ = "tenants"

    tenant_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    api_key: Mapped[str] = mapped_column(String(128), unique=True, nullable=False)
    active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)


@dataclass(frozen=True)
class TenantContext:
    """Resolved tenant attached to an authenticated request."""

    tenant_id: str
    name: str


def ensure_tenants_schema_and_seed() -> None:
    """
    Ensure `tenants` exists and seed the default local tenant.

    Logic:
        - CREATE TABLE via SQLAlchemy metadata for `tenants` only.
        - Upsert default tenant by tenant_id (api_key from env).

    Reason:
        Same pattern as ClickHouse/OpenSearch ensure_* — app boot is self-healing
        for local labs without manual SQL.
    """
    Tenant.__table__.create(bind=engine, checkfirst=True)

    db = SessionLocal()
    try:
        existing = db.get(Tenant, DEFAULT_TENANT_ID)
        if existing is None:
            db.add(
                Tenant(
                    tenant_id=DEFAULT_TENANT_ID,
                    name=DEFAULT_TENANT_NAME,
                    api_key=DEFAULT_API_KEY,
                    active=True,
                    notes="Seeded by Phase 6 Day 1 for local learning",
                )
            )
            db.commit()
            logger.info(
                "Seeded default tenant id=%s api_key=%s",
                DEFAULT_TENANT_ID,
                DEFAULT_API_KEY,
            )
        else:
            # Keep api_key in sync with env so rotating DEFAULT_API_KEY works locally.
            if existing.api_key != DEFAULT_API_KEY:
                existing.api_key = DEFAULT_API_KEY
                db.commit()
                logger.info("Updated default tenant api_key from env")
            logger.info("Default tenant ready id=%s", DEFAULT_TENANT_ID)
    finally:
        db.close()


def resolve_tenant_by_api_key(db, api_key: str | None) -> TenantContext:
    """
    Map X-API-Key → TenantContext.

    Logic:
        - Look up active tenant by api_key.
        - If missing and TENANCY_STRICT=0 → fall back to default tenant.
        - If missing and TENANCY_STRICT=1 → 401.

    Reason:
        Strict mode teaches real SaaS auth; soft mode keeps older curl labs working.
    """
    if api_key:
        row = db.scalar(
            select(Tenant).where(Tenant.api_key == api_key, Tenant.active.is_(True))
        )
        if row is not None:
            return TenantContext(tenant_id=row.tenant_id, name=row.name)

    if TENANCY_STRICT:
        raise HTTPException(
            status_code=401,
            detail=(
                "Invalid or missing X-API-Key "
                "(set TENANCY_STRICT=0 for lab fallback)"
            ),
        )

    row = db.get(Tenant, DEFAULT_TENANT_ID)
    if row is None or not row.active:
        raise HTTPException(status_code=503, detail="Default tenant not configured")
    if api_key and api_key != row.api_key:
        logger.warning("Unknown API key — falling back to default tenant (soft mode)")
    return TenantContext(tenant_id=row.tenant_id, name=row.name)


def require_tenant(
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> TenantContext:
    """
    FastAPI dependency: resolve the calling tenant from X-API-Key.

    Opens a short-lived session (auth only — not the request DB session).
    """
    db = SessionLocal()
    try:
        return resolve_tenant_by_api_key(db, x_api_key)
    finally:
        db.close()


def list_tenants() -> list[dict]:
    """Return active tenants (api keys masked)."""
    db = SessionLocal()
    try:
        rows = db.scalars(
            select(Tenant).where(Tenant.active.is_(True)).order_by(Tenant.tenant_id)
        ).all()
        return [
            {
                "tenant_id": t.tenant_id,
                "name": t.name,
                "api_key_hint": _mask_key(t.api_key),
                "created_at": t.created_at.isoformat() if t.created_at else None,
            }
            for t in rows
        ]
    finally:
        db.close()


def _mask_key(key: str) -> str:
    if len(key) <= 8:
        return "***"
    return f"{key[:4]}…{key[-4:]}"


def new_api_key() -> str:
    """Generate a random API key (for future admin create-tenant)."""
    return secrets.token_urlsafe(24)
