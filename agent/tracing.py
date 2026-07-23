"""
OpenTelemetry for the metrics agent (Phase 5 Day 3).

Starts a CLIENT span per push and injects W3C `traceparent` into HTTP headers
so FastAPI continues the same trace_id.
"""

from __future__ import annotations

import logging
import os
from contextlib import contextmanager
from typing import Iterator

from opentelemetry import propagate, trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace import SpanKind, Status, StatusCode

logger = logging.getLogger(__name__)

OTEL_ENABLED = os.getenv("OTEL_ENABLED", "1") == "1"
OTEL_SERVICE_NAME = os.getenv("OTEL_SERVICE_NAME", "insightnode-agent")
OTEL_EXPORTER_OTLP_ENDPOINT = os.getenv(
    "OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4318"
)

_provider: TracerProvider | None = None


def setup_tracing(service_name: str | None = None) -> bool:
    """Install TracerProvider → Jaeger OTLP (same collector as API/worker)."""
    global _provider

    if not OTEL_ENABLED:
        logger.info("Agent OpenTelemetry disabled (OTEL_ENABLED=0)")
        return False
    if _provider is not None:
        return True

    name = service_name or OTEL_SERVICE_NAME
    resource = Resource.create(
        {
            "service.name": name,
            "service.namespace": "insightnode",
        }
    )
    provider = TracerProvider(resource=resource)
    endpoint = OTEL_EXPORTER_OTLP_ENDPOINT.rstrip("/")
    if not endpoint.endswith("/v1/traces"):
        endpoint = f"{endpoint}/v1/traces"

    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint)))
    trace.set_tracer_provider(provider)
    _provider = provider
    logger.info("Agent tracing ready service=%s otlp=%s", name, endpoint)
    return True


def shutdown_tracing() -> None:
    """Flush spans on agent exit."""
    global _provider
    if _provider is None:
        return
    try:
        _provider.force_flush(timeout_millis=5000)
        _provider.shutdown()
    except Exception:
        logger.exception("Agent tracing shutdown failed")
    finally:
        _provider = None


def inject_headers() -> dict[str, str]:
    """W3C carrier for the current span (`traceparent`, optional `tracestate`)."""
    carrier: dict[str, str] = {}
    propagate.inject(carrier)
    return carrier


@contextmanager
def push_metrics_span(event_id: str | None = None) -> Iterator[trace.Span]:
    """CLIENT span around POST /metrics — inject headers while this span is current."""
    tracer = trace.get_tracer("insightnode.agent")
    with tracer.start_as_current_span(
        "agent.push_metrics",
        kind=SpanKind.CLIENT,
    ) as span:
        span.set_attribute("http.method", "POST")
        span.set_attribute("insightnode.phase", "5")
        span.set_attribute("insightnode.day", "3")
        if event_id:
            span.set_attribute("insightnode.event_id", event_id)
        yield span


def record_error(span: trace.Span, exc: BaseException) -> None:
    span.record_exception(exc)
    span.set_status(Status(StatusCode.ERROR, str(exc)))
