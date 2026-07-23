"""
Host inventory derived from ClickHouse metrics (Phase 7).

No separate hosts registry — machine_id + last_seen come from insightnode.metrics.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any

from backend.clickhouse_client import METRICS_TABLE, get_client

logger = logging.getLogger(__name__)

GRAFANA_PUBLIC_URL = os.getenv("GRAFANA_PUBLIC_URL", "http://localhost:3000")
DEFAULT_ACTIVE_WITHIN_SECONDS = 120
GAUGE_NAMES = ("cpu_usage", "memory_usage", "disk_usage")


def _as_utc(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def _host_status(last_seen: datetime, active_within_seconds: int, *, now: datetime) -> str:
    age = (now - last_seen).total_seconds()
    return "online" if age <= active_within_seconds else "offline"


def _row_to_host(
    row: dict[str, Any],
    *,
    active_within_seconds: int,
    now: datetime,
) -> dict[str, Any]:
    last_seen = _as_utc(row["last_seen"])
    assert last_seen is not None
    latest: dict[str, float] = {}
    for name in GAUGE_NAMES:
        value = row.get(name)
        if value is not None:
            latest[name] = float(value)
    return {
        "machine_id": str(row["machine_id"]),
        "last_seen": last_seen,
        "status": _host_status(last_seen, active_within_seconds, now=now),
        "latest": latest,
    }


def list_hosts(
    *,
    tenant_id: str,
    active_within_seconds: int = DEFAULT_ACTIVE_WITHIN_SECONDS,
) -> list[dict[str, Any]]:
    """
    All machines seen for a tenant, with latest gauges and online/offline status.

    Logic:
        - GROUP BY machine_id; max(timestamp) = last_seen.
        - argMaxIf(value, timestamp, metric) for each gauge name.
    """
    sql = f"""
        SELECT
            machine_id,
            max(timestamp) AS last_seen,
            argMaxIf(value, timestamp, metric_name = 'cpu_usage') AS cpu_usage,
            argMaxIf(value, timestamp, metric_name = 'memory_usage') AS memory_usage,
            argMaxIf(value, timestamp, metric_name = 'disk_usage') AS disk_usage
        FROM {METRICS_TABLE}
        WHERE tenant_id = {{tenant_id:String}}
        GROUP BY machine_id
        ORDER BY last_seen DESC
    """
    result = get_client().query(sql, parameters={"tenant_id": tenant_id})
    now = datetime.now(timezone.utc)
    hosts = [
        _row_to_host(row, active_within_seconds=active_within_seconds, now=now)
        for row in result.named_results()
    ]
    logger.info(
        "Listed hosts tenant=%s count=%s active_within=%ss",
        tenant_id,
        len(hosts),
        active_within_seconds,
    )
    return hosts


def get_host(
    *,
    tenant_id: str,
    machine_id: str,
    active_within_seconds: int = DEFAULT_ACTIVE_WITHIN_SECONDS,
) -> dict[str, Any] | None:
    """One host snapshot, or None if the machine has no metrics for this tenant."""
    sql = f"""
        SELECT
            machine_id,
            max(timestamp) AS last_seen,
            argMaxIf(value, timestamp, metric_name = 'cpu_usage') AS cpu_usage,
            argMaxIf(value, timestamp, metric_name = 'memory_usage') AS memory_usage,
            argMaxIf(value, timestamp, metric_name = 'disk_usage') AS disk_usage
        FROM {METRICS_TABLE}
        WHERE tenant_id = {{tenant_id:String}}
          AND machine_id = {{machine_id:String}}
        GROUP BY machine_id
    """
    result = get_client().query(
        sql,
        parameters={"tenant_id": tenant_id, "machine_id": machine_id},
    )
    rows = list(result.named_results())
    if not rows:
        return None
    now = datetime.now(timezone.utc)
    return _row_to_host(rows[0], active_within_seconds=active_within_seconds, now=now)


def fleet_counts(
    *,
    tenant_id: str,
    active_within_seconds: int = DEFAULT_ACTIVE_WITHIN_SECONDS,
) -> dict[str, Any]:
    """
    active_hosts / offline_hosts / latest_metric_at for system summary.

    Offline = seen at least once but not within the active window.
    """
    sql = f"""
        SELECT
            countIf(last_seen > now64(3) - INTERVAL {{active_within:UInt32}} SECOND)
                AS active_hosts,
            countIf(last_seen <= now64(3) - INTERVAL {{active_within:UInt32}} SECOND)
                AS offline_hosts,
            max(last_seen) AS latest_metric_at
        FROM (
            SELECT
                machine_id,
                max(timestamp) AS last_seen
            FROM {METRICS_TABLE}
            WHERE tenant_id = {{tenant_id:String}}
            GROUP BY machine_id
        )
    """
    result = get_client().query(
        sql,
        parameters={
            "tenant_id": tenant_id,
            "active_within": int(active_within_seconds),
        },
    )
    rows = list(result.named_results())
    if not rows:
        return {
            "active_hosts": 0,
            "offline_hosts": 0,
            "latest_metric_at": None,
        }
    row = rows[0]
    return {
        "active_hosts": int(row["active_hosts"] or 0),
        "offline_hosts": int(row["offline_hosts"] or 0),
        "latest_metric_at": _as_utc(row["latest_metric_at"]),
    }


def grafana_dashboard_url(machine_id: str | None = None) -> str:
    """Deep-link into the provisioned Infrastructure dashboard."""
    base = GRAFANA_PUBLIC_URL.rstrip("/")
    url = f"{base}/d/insightnode-infrastructure/infrastructure-monitoring"
    if machine_id:
        from urllib.parse import quote

        url += f"?var-machine_id={quote(machine_id, safe='')}"
    return url
