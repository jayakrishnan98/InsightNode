"""
Telemetry agent — collects host metrics and pushes them to the InsightNode API.

Runs on each machine you want to monitor. If the API is temporarily down, payloads
are buffered to disk (see spool.py) and replayed when connectivity returns.

Phase 4 Day 4: also ships structured logs to POST /logs (see logship.py) for
retries, spool events, and threshold warnings — searchable in OpenSearch.
Phase 5 Day 3: injects W3C trace context on POST /metrics (see tracing.py).
"""

import os
import time
import socket
import psutil
import httpx
from datetime import datetime, timezone
import uuid

import spool
import logship
import tracing as agent_tracing

MACHINE_ID = socket.gethostname()
COLLECTION_INTERVAL_SECONDS = int(os.getenv("COLLECTION_INTERVAL_SECONDS", "5"))
API_URL = os.getenv("INSIGHTNODE_METRICS_URL", "http://127.0.0.1:8001/metrics")
MAX_RETRIES = 5
RETRY_BASE_DELAY_SECONDS = 1.0

# Thresholds for structured warn logs (percent gauges).
CPU_WARN_PERCENT = float(os.getenv("CPU_WARN_PERCENT", "90"))
MEMORY_WARN_PERCENT = float(os.getenv("MEMORY_WARN_PERCENT", "85"))
DISK_WARN_PERCENT = float(os.getenv("DISK_WARN_PERCENT", "80"))


def collect_metrics() -> dict:
    """
    Sample CPU, memory, and disk usage from the local operating system.

    Logic:
        - Block for COLLECTION_INTERVAL_SECONDS while measuring CPU (psutil needs
          a sampling window to compute a meaningful percentage).
        - Read memory and disk gauges in the same pass.
        - Generate event_id (UUID) once per collection cycle.
        - Build one payload dict: event_id, machine_id, UTC timestamp, metrics.

    Reason:
        Observability starts at the source. Each metric is a gauge (current value),
        not a counter. UTC + ISO 8601 keeps timestamps unambiguous across machines
        and time zones. event_id is stable across retries and spool replay so the
        API can deduplicate at-least-once deliveries.
    """
    cpu = psutil.cpu_percent(interval=COLLECTION_INTERVAL_SECONDS)
    memory = psutil.virtual_memory()
    disk = psutil.disk_usage("/")

    return {
        "event_id": str(uuid.uuid4()),
        "machine_id": MACHINE_ID,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "metrics": [
            {
                "name": "cpu_usage",
                "value": round(cpu, 1),
                "unit": "percent",
            },
            {
                "name": "memory_usage",
                "value": round(memory.percent, 1),
                "unit": "percent",
            },
            {
                "name": "disk_usage",
                "value": round(disk.percent, 1),
                "unit": "percent",
            },
        ],
    }


def _metric_value(payload: dict, name: str) -> float | None:
    for m in payload.get("metrics", []):
        if m.get("name") == name:
            return float(m["value"])
    return None


def emit_threshold_logs(payload: dict) -> None:
    """
    Ship warn logs when gauges cross configured thresholds (Phase 4 Day 4).

    Logic:
        - Read cpu/memory/disk from the same payload we just collected.
        - For each threshold breach, POST a structured warn to /logs.

    Reason:
        Metrics answer "what is the value?"; logs answer "what happened?" —
        threshold crossings are searchable events in OpenSearch.
    """
    cpu = _metric_value(payload, "cpu_usage")
    memory = _metric_value(payload, "memory_usage")
    disk = _metric_value(payload, "disk_usage")

    if cpu is not None and cpu >= CPU_WARN_PERCENT:
        logship.warn(
            f"CPU usage high: {cpu}%",
            metric="cpu_usage",
            value=cpu,
            threshold=CPU_WARN_PERCENT,
            event_id=payload.get("event_id"),
        )
    if memory is not None and memory >= MEMORY_WARN_PERCENT:
        logship.warn(
            f"Memory usage high: {memory}%",
            metric="memory_usage",
            value=memory,
            threshold=MEMORY_WARN_PERCENT,
            event_id=payload.get("event_id"),
        )
    if disk is not None and disk >= DISK_WARN_PERCENT:
        logship.warn(
            f"Disk usage high: {disk}%",
            metric="disk_usage",
            value=disk,
            threshold=DISK_WARN_PERCENT,
            event_id=payload.get("event_id"),
            path="/",
        )


def send_metrics_with_retry(payload: dict) -> bool:
    """
    POST a metrics payload to the API, retrying on transient network/API errors.

    Logic:
        - Attempt up to MAX_RETRIES HTTP POSTs to API_URL.
        - On success (2xx), return True immediately.
        - On failure, wait with exponential backoff (1s, 2s, 4s, …) before retrying.
        - After all attempts fail, return False so the caller can buffer to disk.

    Reason:
        Short outages (API restart, brief network blip) should not lose data.
        Exponential backoff avoids hammering a recovering server. A bounded retry
        count prevents the agent from blocking forever on a hard failure.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            with agent_tracing.push_metrics_span(
                event_id=str(payload.get("event_id") or "") or None
            ) as span:
                try:
                    headers = agent_tracing.inject_headers()
                    response = httpx.post(
                        API_URL,
                        json=payload,
                        headers=headers,
                        timeout=10.0,
                    )
                    response.raise_for_status()
                    span.set_attribute("http.status_code", response.status_code)
                except Exception as exc:
                    agent_tracing.record_error(span, exc)
                    raise
            print(f"[SENT] {response.status_code} → {response.json()}")
            return True
        except httpx.HTTPError as e:
            if attempt == MAX_RETRIES:
                print(f"[ERROR] Failed after {MAX_RETRIES} attempts: {e}")
                logship.error(
                    f"Metrics send failed after {MAX_RETRIES} attempts",
                    attempts=MAX_RETRIES,
                    error=str(e),
                    metrics_event_id=payload.get("event_id"),
                )
                return False

            delay = RETRY_BASE_DELAY_SECONDS * (2 ** (attempt - 1))
            print(f"[RETRY] attempt {attempt}/{MAX_RETRIES}, waiting {delay:.1f}s — {e}")
            logship.warn(
                f"Metrics send retry {attempt}/{MAX_RETRIES}",
                attempt=attempt,
                max_retries=MAX_RETRIES,
                delay_seconds=delay,
                error=str(e),
                metrics_event_id=payload.get("event_id"),
            )
            time.sleep(delay)

    return False


def replay_spool() -> None:
    """
    Send any metrics that were previously saved to the on-disk spool file.

    Logic:
        - Read all buffered payloads from spool.ndjson.
        - Try to send each one via send_metrics_with_retry (FIFO order).
        - Collect payloads that still fail and rewrite the spool with only those.
        - Log how many were drained vs still pending.

    Reason:
        Disk spool survives agent restarts and long API outages. Replaying before
        each new collection prioritizes clearing backlog over ingesting fresh data.
        Tradeoff: at-least-once delivery — a crash mid-replay may re-send payloads,
        but Day 11 idempotency (event_id) prevents duplicate rows in PostgreSQL.
    """
    buffered = spool.read_all()
    if not buffered:
        return

    print(f"[SPOOL] Replaying {len(buffered)} buffered payload(s)...")
    logship.info("Replaying metric spool", buffered=len(buffered))
    still_failed: list[dict] = []

    for payload in buffered:
        if send_metrics_with_retry(payload):
            print(f"[SPOOL] Replayed payload @ {payload['timestamp']}")
        else:
            still_failed.append(payload)

    spool.rewrite(still_failed)

    if still_failed:
        print(f"[SPOOL] {len(still_failed)} payload(s) still buffered on disk")
        logship.warn(
            "Metric spool still has pending payloads",
            pending=len(still_failed),
        )
    else:
        print("[SPOOL] Spool drained successfully")
        logship.info("Metric spool drained successfully")


def main() -> None:
    """
    Main agent loop: replay backlog, collect fresh metrics, send or spool.

    Logic:
        1. replay_spool() — drain any metrics buffered from earlier failures.
        2. collect_metrics() — sample the OS (blocks ~COLLECTION_INTERVAL_SECONDS).
        3. emit_threshold_logs() — ship warn logs if gauges cross thresholds.
        4. send_metrics_with_retry() — push to API.
        5. On failure, spool.append() — persist payload to disk for later replay.
        6. Repeat forever.

    Reason:
        Separates "try to clear old data" from "collect new data" so recovery is
        automatic when the API comes back. Structured logs make agent health
        searchable in OpenSearch alongside metrics charts.
    """
    agent_tracing.setup_tracing()
    logship.info("Agent starting", metrics_url=API_URL)
    try:
        while True:
            replay_spool()

            payload = collect_metrics()
            emit_threshold_logs(payload)

            if send_metrics_with_retry(payload):
                pass  # success
            else:
                spool.append(payload)
                print(f"[SPOOL] Buffered payload on disk (total={spool.size()})")
                logship.warn(
                    "Buffered metrics payload to disk spool",
                    spool_size=spool.size(),
                    metrics_event_id=payload.get("event_id"),
                )
    finally:
        agent_tracing.shutdown_tracing()


if __name__ == "__main__":
    main()
