"""
Ship structured logs from the agent to InsightNode POST /logs (Phase 4 Day 4).

Best-effort: log shipping must never block or break metrics collection.

Phase 5 Day 4: optional logship.ship span + trace_id/span_id in attrs.
"""

from __future__ import annotations

import os
import socket
import uuid
from datetime import datetime, timezone
from typing import Any

import httpx

import tracing as agent_tracing

LOGS_URL = os.getenv("INSIGHTNODE_LOGS_URL", "http://127.0.0.1:8001/logs")
MACHINE_ID = socket.gethostname()
SERVICE = "agent"


def _event(
    *,
    level: str,
    message: str,
    attrs: dict[str, Any] | None = None,
) -> dict[str, Any]:
    merged = dict(attrs or {})
    merged.update(agent_tracing.current_trace_context())
    return {
        "event_id": str(uuid.uuid4()),
        "machine_id": MACHINE_ID,
        "service": SERVICE,
        "level": level,
        "message": message,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "attrs": merged,
    }


def ship(
    level: str,
    message: str,
    *,
    attrs: dict[str, Any] | None = None,
) -> bool:
    """
    POST one structured log event to the API.

    Logic:
        - Build a LogEvent-shaped dict (same schema as POST /logs).
        - Attach current trace_id/span_id when agent tracing is active.
        - Single HTTP attempt under logship.ship; swallow failures.

    Reason:
        Observability of the agent itself should not create a death spiral:
        if OpenSearch/API is down, metrics spool still works; logs are best-effort.
    """
    payload = {"logs": [_event(level=level, message=message, attrs=attrs)]}
    try:
        with agent_tracing.logship_span(level, message) as span:
            try:
                response = httpx.post(LOGS_URL, json=payload, timeout=5.0)
                response.raise_for_status()
                span.set_attribute("http.status_code", response.status_code)
            except Exception as exc:
                agent_tracing.record_error(span, exc)
                raise
        return True
    except httpx.HTTPError as exc:
        print(f"[LOGSHIP] failed: {exc}")
        return False


def info(message: str, **attrs: Any) -> bool:
    return ship("info", message, attrs=attrs)


def warn(message: str, **attrs: Any) -> bool:
    return ship("warn", message, attrs=attrs)


def error(message: str, **attrs: Any) -> bool:
    return ship("error", message, attrs=attrs)
