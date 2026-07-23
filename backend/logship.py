"""
Ship structured ops logs from API / worker into OpenSearch (Phase 4 Day 4).

Uses the in-process OpenSearch client (no HTTP loopback to POST /logs).
Best-effort: never raise into the request/worker path.

Phase 5 Day 4: wraps index in a manual span and attaches trace_id/span_id attrs
so OpenSearch logs can be correlated with Jaeger traces.
"""

from __future__ import annotations

import logging
import os
import socket
import uuid
from datetime import datetime, timezone
from typing import Any

from backend.opensearch_client import index_logs
from backend.tracing import current_trace_context, manual_span, record_span_error

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
        - Merge current W3C trace_id/span_id into attrs (Day 4 correlation).
        - Build the same document shape as POST /logs.
        - Under logship.index span → index_logs(refresh=False); swallow errors.

    Reason:
        Rate-limit / DLQ / backlog events are exactly what you search for later.
        Failures shipping logs must not break metric ingest.
    """
    merged = dict(attrs or {})
    merged.update(current_trace_context())

    doc = {
        "event_id": str(uuid.uuid4()),
        "machine_id": MACHINE_ID,
        "service": service,
        "level": level,
        "message": message,
        "timestamp": datetime.now(timezone.utc),
        "attrs": merged,
    }

    span_attrs: dict[str, Any] = {
        "insightnode.service": service,
        "insightnode.log_level": level,
        "db.system": "opensearch",
    }
    if merged.get("trace_id"):
        span_attrs["insightnode.trace_id"] = merged["trace_id"]
    event_id = merged.get("event_id") or merged.get("metrics_event_id")
    if event_id:
        span_attrs["insightnode.event_id"] = str(event_id)

    with manual_span("logship.index", attributes=span_attrs) as span:
        try:
            result = index_logs([doc], refresh=False)
            if result.get("errors"):
                span.set_attribute("insightnode.logship_bulk_errors", True)
                logger.warning("logship bulk reported errors for service=%s", service)
        except Exception as exc:
            record_span_error(span, exc)
            logger.exception("logship failed service=%s", service)
            # Do not re-raise — logship is best-effort.


def warn(service: str, message: str, **attrs: Any) -> None:
    ship(service=service, level="warn", message=message, attrs=attrs)


def error(service: str, message: str, **attrs: Any) -> None:
    ship(service=service, level="error", message=message, attrs=attrs)


def info(service: str, message: str, **attrs: Any) -> None:
    ship(service=service, level="info", message=message, attrs=attrs)
