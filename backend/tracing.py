"""
OpenTelemetry tracing for Phase 5.

Day 1: configure TracerProvider + OTLP HTTP exporter → Jaeger; health ping.
Day 2+: FastAPI / agent / worker instrumentation (not yet).
"""

from __future__ import annotations

import logging
import os

import httpx
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

logger = logging.getLogger(__name__)

OTEL_ENABLED = os.getenv("OTEL_ENABLED", "1") == "1"
OTEL_SERVICE_NAME = os.getenv("OTEL_SERVICE_NAME", "insightnode-api")
# Jaeger OTLP HTTP collector (protobuf). Exporter appends /v1/traces.
OTEL_EXPORTER_OTLP_ENDPOINT = os.getenv(
    "OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4318"
)
JAEGER_UI_URL = os.getenv("JAEGER_UI_URL", "http://localhost:16686")
JAEGER_ADMIN_URL = os.getenv("JAEGER_ADMIN_URL", "http://localhost:14269")

_provider: TracerProvider | None = None


def setup_tracing(service_name: str | None = None) -> bool:
    """
    Install a process-wide TracerProvider that exports spans to Jaeger via OTLP.

    Logic:
        - No-op when OTEL_ENABLED=0.
        - Resource.service.name identifies this process in the Jaeger UI.
        - BatchSpanProcessor + OTLP HTTP exporter → :4318.
        - Emit a tiny startup.bootstrap span so Day 1 is visible in the UI.

    Reason:
        Day 1 proves the pipeline (SDK → OTLP → Jaeger) before instrumenting routes.
    """
    global _provider

    if not OTEL_ENABLED:
        logger.info("OpenTelemetry disabled (OTEL_ENABLED=0)")
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

    exporter = OTLPSpanExporter(endpoint=endpoint)
    provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(provider)
    _provider = provider

    tracer = trace.get_tracer("insightnode.tracing")
    with tracer.start_as_current_span("startup.bootstrap") as span:
        span.set_attribute("insightnode.phase", "5")
        span.set_attribute("insightnode.day", "1")

    logger.info(
        "OpenTelemetry tracing ready service=%s otlp=%s",
        name,
        endpoint,
    )
    return True


def get_tracer(name: str = "insightnode") -> trace.Tracer:
    """Return a tracer (no-op tracer if provider was never set)."""
    return trace.get_tracer(name)


def ping() -> bool:
    """True if Jaeger admin or UI answers (collector is up)."""
    for url in (JAEGER_ADMIN_URL, JAEGER_UI_URL):
        try:
            response = httpx.get(url, timeout=2.0)
            if response.status_code < 500:
                return True
        except Exception:
            continue
    logger.warning("Jaeger ping failed admin=%s ui=%s", JAEGER_ADMIN_URL, JAEGER_UI_URL)
    return False


def shutdown_tracing() -> None:
    """Flush and shut down the TracerProvider (API lifespan)."""
    global _provider
    if _provider is None:
        return
    try:
        _provider.force_flush(timeout_millis=5000)
        _provider.shutdown()
    except Exception:
        logger.exception("OpenTelemetry shutdown failed")
    finally:
        _provider = None
        logger.info("OpenTelemetry tracing shut down")
