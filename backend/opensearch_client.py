"""
OpenSearch client for Phase 4.

Day 1: connect, ensure logs index, health ping.
Day 2: index_logs() / get_log() for POST /logs ingest.
Day 3: search (not yet).
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from opensearchpy import OpenSearch, helpers

logger = logging.getLogger(__name__)

OPENSEARCH_HOST = os.getenv("OPENSEARCH_HOST", "localhost")
OPENSEARCH_PORT = int(os.getenv("OPENSEARCH_PORT", "9200"))
OPENSEARCH_USER = os.getenv("OPENSEARCH_USER", "")
OPENSEARCH_PASSWORD = os.getenv("OPENSEARCH_PASSWORD", "")
LOGS_INDEX = os.getenv("OPENSEARCH_LOGS_INDEX", "insightnode-logs")

INDEX_BODY_PATH = (
    Path(__file__).resolve().parent.parent / "opensearch" / "logs_index.json"
)

_client: OpenSearch | None = None


def get_client() -> OpenSearch:
    """
    Return a process-wide OpenSearch client.

    Logic:
        - Lazy-create one HTTP client (connection pool).
        - Local Day 1 uses http:// with security plugin disabled (no auth).

    Reason:
        Reusing one client avoids handshake cost on every request.
    """
    global _client
    if _client is None:
        http_auth = None
        if OPENSEARCH_USER:
            http_auth = (OPENSEARCH_USER, OPENSEARCH_PASSWORD)

        _client = OpenSearch(
            hosts=[{"host": OPENSEARCH_HOST, "port": OPENSEARCH_PORT}],
            http_compress=True,
            use_ssl=False,
            verify_certs=False,
            ssl_show_warn=False,
            http_auth=http_auth,
        )
        logger.info(
            "OpenSearch client connected host=%s port=%s index=%s",
            OPENSEARCH_HOST,
            OPENSEARCH_PORT,
            LOGS_INDEX,
        )
    return _client


def ping() -> bool:
    """True if the cluster answers ping()."""
    try:
        return bool(get_client().ping())
    except Exception:
        logger.exception("OpenSearch ping failed")
        return False


def ensure_index() -> None:
    """
    Idempotently create the insightnode-logs index from opensearch/logs_index.json.

    Logic:
        - If index exists → no-op.
        - Else create with mappings (keyword filters + text message).

    Reason:
        Docker does not auto-apply our mapping file. App-level ensure keeps
        local restarts reliable — same pattern as ClickHouse ensure_schema().
    """
    client = get_client()
    if client.indices.exists(index=LOGS_INDEX):
        logger.info("OpenSearch index already exists: %s", LOGS_INDEX)
        return

    body = json.loads(INDEX_BODY_PATH.read_text(encoding="utf-8"))
    client.indices.create(index=LOGS_INDEX, body=body)
    logger.info("Created OpenSearch index: %s", LOGS_INDEX)


def _as_utc_iso(value: Any) -> str:
    """Normalize datetime / ISO string to UTC ISO-8601 for the date field."""
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str):
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    else:
        raise TypeError(f"Unsupported timestamp type: {type(value)!r}")

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.isoformat()


def _normalize_doc(doc: dict[str, Any]) -> dict[str, Any]:
    """Map an API log event into an OpenSearch _source document."""
    event_id = str(doc["event_id"])
    return {
        "event_id": event_id,
        "machine_id": doc["machine_id"],
        "service": doc.get("service") or "unknown",
        "level": doc.get("level") or "info",
        "message": doc["message"],
        "timestamp": _as_utc_iso(doc["timestamp"]),
        "attrs": doc.get("attrs") or {},
    }


def index_logs(docs: list[dict[str, Any]], *, refresh: bool = False) -> dict[str, Any]:
    """
    Bulk-index log documents into insightnode-logs (Phase 4 Day 2).

    Logic:
        - Use event_id as document _id (idempotent re-ingest overwrites).
        - helpers.bulk for efficient multi-doc indexing.
        - refresh=False by default (near-real-time; index refresh_interval=1s).

    Reason:
        Logs arrive in bursts; bulk beats one HTTP round-trip per line.
        Stable _id mirrors metrics event_id idempotency without a SQL unique index.
    """
    if not docs:
        return {"indexed": 0, "errors": False, "ids": []}

    actions = []
    ids: list[str] = []
    for raw in docs:
        source = _normalize_doc(raw)
        event_id = source["event_id"]
        ids.append(event_id)
        actions.append(
            {
                "_op_type": "index",
                "_index": LOGS_INDEX,
                "_id": event_id,
                "_source": source,
            }
        )

    success, errors = helpers.bulk(
        get_client(),
        actions,
        refresh=refresh,
        raise_on_error=False,
    )
    had_errors = bool(errors)
    if had_errors:
        logger.error("OpenSearch bulk index partial failure: %s", errors[:3])
    else:
        logger.info("OpenSearch indexed %s log(s) into %s", success, LOGS_INDEX)

    return {
        "indexed": success,
        "errors": had_errors,
        "ids": ids,
    }


def get_log(event_id: str) -> dict[str, Any] | None:
    """
    Fetch one log by event_id (document _id).

    Returns None if missing — useful to verify Day 2 ingest before Day 3 search.
    """
    client = get_client()
    try:
        result = client.get(index=LOGS_INDEX, id=event_id)
    except Exception as exc:
        # NotFoundError and transport errors — treat missing as None
        if getattr(exc, "status_code", None) == 404:
            return None
        # opensearch-py NotFoundError
        if exc.__class__.__name__ == "NotFoundError":
            return None
        raise

    source = result.get("_source")
    if not isinstance(source, dict):
        return None
    return source


def close_client() -> None:
    """Close the shared client (API shutdown)."""
    global _client
    if _client is not None:
        _client.close()
        _client = None
        logger.info("OpenSearch client closed")
