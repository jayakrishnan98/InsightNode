"""
OpenSearch client for Phase 4.

Day 1: connect, ensure logs index, health ping.
Day 2+: ingest + search APIs (not yet).
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path

from opensearchpy import OpenSearch

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


def close_client() -> None:
    """Close the shared client (API shutdown)."""
    global _client
    if _client is not None:
        _client.close()
        _client = None
        logger.info("OpenSearch client closed")
