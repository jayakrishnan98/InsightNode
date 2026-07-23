"""
Prometheus metrics for InsightNode API and workers (Phase 7 platform observability).

Expose via GET /prometheus on the API and a small HTTP server on the worker
(WORKER_METRICS_PORT, default 8002). Path is /prometheus because /metrics is
the telemetry ingest API.
"""

from __future__ import annotations

import logging
import os
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Callable

from prometheus_client import (
    CONTENT_TYPE_LATEST,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)

logger = logging.getLogger(__name__)

HTTP_REQUESTS = Counter(
    "insightnode_http_requests_total",
    "HTTP requests handled by the InsightNode API",
    ["method", "path", "status"],
)

HTTP_DURATION = Histogram(
    "insightnode_http_request_duration_seconds",
    "HTTP request latency in seconds",
    ["method", "path"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
)

INGEST_ACCEPTED = Counter(
    "insightnode_ingest_accepted_total",
    "Metric ingest payloads accepted (202) and enqueued to Kafka",
)

INGEST_REJECTED = Counter(
    "insightnode_ingest_rejected_total",
    "Metric ingest payloads rejected before enqueue",
    ["reason"],
)

KAFKA_LAG = Gauge(
    "insightnode_kafka_lag",
    "Approximate Kafka consumer lag for the ingest topic",
)

KAFKA_DLQ_SIZE = Gauge(
    "insightnode_kafka_dlq_size",
    "Approximate DLQ topic message count",
)

WORKER_DUAL_WRITE_SUCCESS = Counter(
    "insightnode_worker_dual_write_success_total",
    "Successful dual-write batches (PostgreSQL + ClickHouse)",
)

WORKER_DUAL_WRITE_FAILURES = Counter(
    "insightnode_worker_dual_write_failures_total",
    "Dual-write failures by store",
    ["store"],
)

WORKER_BATCHES = Counter(
    "insightnode_worker_batches_total",
    "Worker batches that completed dual-write successfully",
)


def refresh_queue_gauges(
    lag_fn: Callable[[], int],
    dlq_fn: Callable[[], int],
) -> None:
    """Update Kafka lag / DLQ gauges (call before scraping)."""
    try:
        KAFKA_LAG.set(lag_fn())
    except Exception:
        logger.debug("Failed to refresh kafka lag gauge", exc_info=True)
    try:
        KAFKA_DLQ_SIZE.set(dlq_fn())
    except Exception:
        logger.debug("Failed to refresh dlq size gauge", exc_info=True)


def prometheus_response_body(
    *,
    lag_fn: Callable[[], int] | None = None,
    dlq_fn: Callable[[], int] | None = None,
) -> tuple[bytes, str]:
    """Return (body, content_type) for a Prometheus scrape."""
    if lag_fn is not None and dlq_fn is not None:
        refresh_queue_gauges(lag_fn, dlq_fn)
    return generate_latest(), CONTENT_TYPE_LATEST


def start_worker_metrics_server(port: int | None = None) -> HTTPServer | None:
    """
    Serve GET /prometheus on a background thread (standalone worker).

    Returns the HTTPServer, or None if port is 0 / disabled.
    """
    if port is None:
        port = int(os.getenv("WORKER_METRICS_PORT", "8002"))
    if port <= 0:
        logger.info("Worker Prometheus server disabled (WORKER_METRICS_PORT=%s)", port)
        return None

    class _Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            path = self.path.split("?", 1)[0]
            if path != "/prometheus":
                self.send_response(404)
                self.end_headers()
                return
            body, content_type = prometheus_response_body()
            self.send_response(200)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: object) -> None:  # noqa: A003
            # Keep worker logs focused on ingest, not scrape chatter.
            logger.debug("prometheus scrape: " + format, *args)

    server = HTTPServer(("0.0.0.0", port), _Handler)
    thread = threading.Thread(
        target=server.serve_forever,
        name="prometheus-metrics",
        daemon=True,
    )
    thread.start()
    logger.info("Worker Prometheus metrics listening on 0.0.0.0:%s/prometheus", port)
    return server
