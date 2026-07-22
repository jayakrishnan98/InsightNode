"""
ClickHouse client for Phase 3 Day 1.

Day 1 only: connect, ensure schema, health ping.
Dual-write from the Kafka worker lands in Day 2.
Aggregate query routing lands in Day 3.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

import clickhouse_connect
from clickhouse_connect.driver.client import Client

logger = logging.getLogger(__name__)

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "insightnode")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "insightnode")
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "insightnode")

SCHEMA_PATH = Path(__file__).resolve().parent.parent / "sql" / "clickhouse" / "schema.sql"

_client: Client | None = None


def get_client() -> Client:
    """
    Return a process-wide ClickHouse HTTP client.

    Logic:
        - Lazy-create one client on first use (reuse TCP/HTTP connections).
        - Connect to the default DB first; ensure_schema() creates insightnode.

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


def close_client() -> None:
    """Close the shared client (API shutdown)."""
    global _client
    if _client is not None:
        _client.close()
        _client = None
        logger.info("ClickHouse client closed")
