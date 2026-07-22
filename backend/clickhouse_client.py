"""
ClickHouse client for Phase 3.

Day 1: connect, ensure schema, health ping.
Day 2: insert_metrics() for worker dual-write.
Day 3: aggregate query routing (not yet).
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import UUID

import clickhouse_connect
from clickhouse_connect.driver.client import Client

logger = logging.getLogger(__name__)

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "insightnode")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "insightnode")
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "insightnode")

METRICS_TABLE = f"{CLICKHOUSE_DATABASE}.metrics"
SCHEMA_PATH = Path(__file__).resolve().parent.parent / "sql" / "clickhouse" / "schema.sql"

COLUMN_NAMES = [
    "machine_id",
    "metric_name",
    "value",
    "unit",
    "timestamp",
    "event_id",
]

_client: Client | None = None


def get_client() -> Client:
    """
    Return a process-wide ClickHouse HTTP client.

    Logic:
        - Lazy-create one client on first use (reuse TCP/HTTP connections).
        - Connect without a default DB; DDL/DML use fully qualified names.

    Reason:
        clickhouse-connect is designed for long-lived clients. Creating one per
        request wastes sockets and handshake time.
    """
    global _client
    if _client is None:
        _client = clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            username=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD,
            # No database= here: ensure_schema() creates insightnode first.
            # All DDL/DML uses fully qualified insightnode.metrics.
        )
        logger.info(
            "ClickHouse client connected host=%s port=%s",
            CLICKHOUSE_HOST,
            CLICKHOUSE_PORT,
        )
    return _client


def ping() -> bool:
    """True if ClickHouse answers a trivial SELECT."""
    try:
        result = get_client().query("SELECT 1")
        return result.result_rows == [(1,)]
    except Exception:
        logger.exception("ClickHouse ping failed")
        return False


def _statements_from_schema_file() -> list[str]:
    """
    Parse sql/clickhouse/schema.sql into executable statements.

    Skips full-line -- comments; splits on semicolons.
    """
    raw = SCHEMA_PATH.read_text(encoding="utf-8")
    without_line_comments = "\n".join(
        line for line in raw.splitlines() if not line.strip().startswith("--")
    )
    return [
        stmt.strip()
        for stmt in without_line_comments.split(";")
        if stmt.strip()
    ]


def ensure_schema() -> None:
    """
    Idempotently create insightnode DB + metrics MergeTree table.

    Logic:
        - Run each statement from sql/clickhouse/schema.sql (IF NOT EXISTS).
        - Safe to call on every API / worker boot.

    Reason:
        Docker init scripts only run on an empty data volume. App-level ensure
        keeps local restarts and fresh clones reliable.
    """
    client = get_client()
    for stmt in _statements_from_schema_file():
        client.command(stmt)
    logger.info(
        "ClickHouse schema ready database=%s table=metrics",
        CLICKHOUSE_DATABASE,
    )


def _as_utc_datetime(value: Any) -> datetime:
    """Normalize ISO strings / datetimes to timezone-aware UTC for DateTime64."""
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str):
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    else:
        raise TypeError(f"Unsupported timestamp type: {type(value)!r}")

    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _as_uuid_or_none(value: Any) -> UUID | None:
    if value is None or value == "":
        return None
    if isinstance(value, UUID):
        return value
    return UUID(str(value))


def insert_metrics(rows: list[dict[str, Any]]) -> None:
    """
    Batch-insert metric rows into ClickHouse (Phase 3 Day 2).

    Logic:
        - Map the same row dicts the worker uses for PostgreSQL.
        - Normalize timestamp / event_id types for DateTime64 + Nullable(UUID).
        - client.insert() one batch (created_at uses table DEFAULT).

    Reason:
        Dual-write keeps PG as the idempotent row store and CH as the analytics
        copy. ClickHouse has no unique constraint yet — at-least-once redelivery
        after a successful CH insert can create duplicate rows (accepted for Day 2).
    """
    if not rows:
        return

    data = [
        [
            row["machine_id"],
            row["metric_name"],
            float(row["value"]),
            row["unit"],
            _as_utc_datetime(row["timestamp"]),
            _as_uuid_or_none(row.get("event_id")),
        ]
        for row in rows
    ]

    get_client().insert(
        METRICS_TABLE,
        data,
        column_names=COLUMN_NAMES,
    )
    logger.info("ClickHouse insert: %s row(s) into %s", len(data), METRICS_TABLE)


def close_client() -> None:
    """Close the shared client (API / worker shutdown)."""
    global _client
    if _client is not None:
        _client.close()
        _client = None
        logger.info("ClickHouse client closed")
