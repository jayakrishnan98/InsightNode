"""
Ship structured ops logs from API / worker into OpenSearch (Phase 4 Day 4).

Uses the in-process OpenSearch client (no HTTP loopback to POST /logs).
Best-effort: never raise into the request/worker path.
"""

from __future__ import annotations

import logging
import os
import socket
import uuid
from datetime import datetime, timezone
from typing import Any

from backend.opensearch_client import index_logs

logger = logging.getLogger(__name__)

MACHINE_ID = os.getenv("HOSTNAME") or socket.gethostname()


def ship(
    *,
    service: str,
    level: str,
    message: str,
    attrs: dict[str, Any] | None = None,
) -> None:
    """
    Index one ops log document into insightnode-logs.

    Logic:
        - Build the same document shape as POST /logs.
        - index_logs(refresh=False); swallow all exceptions.

    Reason:
        Rate-limit / DLQ / backlog events are exactly what you search for later.
        Failures shipping logs must not break metric ingest.
    """
    doc = {
        "event_id": str(uuid.uuid4()),
        "machine_id": MACHINE_ID,
        "service": service,
        "level": level,
        "message": message,
        "timestamp": datetime.now(timezone.utc),
        "attrs": attrs or {},
    }
    try:
        result = index_logs([doc], refresh=False)
        if result.get("errors"):
            logger.warning("logship bulk reported errors for service=%s", service)
    except Exception:
        logger.exception("logship failed service=%s", service)


def warn(service: str, message: str, **attrs: Any) -> None:
    ship(service=service, level="warn", message=message, attrs=attrs)


def error(service: str, message: str, **attrs: Any) -> None:
    ship(service=service, level="error", message=message, attrs=attrs)


def info(service: str, message: str, **attrs: Any) -> None:
    ship(service=service, level="info", message=message, attrs=attrs)
