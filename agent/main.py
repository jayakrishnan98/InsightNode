"""
Telemetry agent — collects host metrics and pushes them to the InsightNode API.

Runs on each machine you want to monitor. If the API is temporarily down, payloads
are buffered to disk (see spool.py) and replayed when connectivity returns.
"""

import time
import socket
import psutil
import httpx
from datetime import datetime, timezone

import spool

MACHINE_ID = socket.gethostname()
COLLECTION_INTERVAL_SECONDS = 5
API_URL = "http://127.0.0.1:8000/metrics"
MAX_RETRIES = 5
RETRY_BASE_DELAY_SECONDS = 1.0


def collect_metrics() -> dict:
    """
    Sample CPU, memory, and disk usage from the local operating system.

    Logic:
        - Block for COLLECTION_INTERVAL_SECONDS while measuring CPU (psutil needs
          a sampling window to compute a meaningful percentage).
        - Read memory and disk gauges in the same pass.
        - Build one payload dict: machine_id, UTC timestamp, and a list of metrics.

    Reason:
        Observability starts at the source. Each metric is a gauge (current value),
        not a counter. UTC + ISO 8601 keeps timestamps unambiguous across machines
        and time zones.
    """
    cpu = psutil.cpu_percent(interval=COLLECTION_INTERVAL_SECONDS)
    memory = psutil.virtual_memory()
    disk = psutil.disk_usage("/")

    return {
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
            response = httpx.post(API_URL, json=payload, timeout=10.0)
            response.raise_for_status()
            print(f"[SENT] {response.status_code} → {response.json()}")
            return True
        except httpx.HTTPError as e:
            if attempt == MAX_RETRIES:
                print(f"[ERROR] Failed after {MAX_RETRIES} attempts: {e}")
                return False

            delay = RETRY_BASE_DELAY_SECONDS * (2 ** (attempt - 1))
            print(f"[RETRY] attempt {attempt}/{MAX_RETRIES}, waiting {delay:.1f}s — {e}")
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
        Tradeoff: at-least-once delivery — a crash mid-replay can cause duplicates.
    """
    buffered = spool.read_all()
    if not buffered:
        return

    print(f"[SPOOL] Replaying {len(buffered)} buffered payload(s)...")
    still_failed: list[dict] = []

    for payload in buffered:
        if send_metrics_with_retry(payload):
            print(f"[SPOOL] Replayed payload @ {payload['timestamp']}")
        else:
            still_failed.append(payload)

    spool.rewrite(still_failed)

    if still_failed:
        print(f"[SPOOL] {len(still_failed)} payload(s) still buffered on disk")
    else:
        print("[SPOOL] Spool drained successfully")
        
def main() -> None:
    """
    Main agent loop: replay backlog, collect fresh metrics, send or spool.

    Logic:
        1. replay_spool() — drain any metrics buffered from earlier failures.
        2. collect_metrics() — sample the OS (blocks ~COLLECTION_INTERVAL_SECONDS).
        3. send_metrics_with_retry() — push to API.
        4. On failure, spool.append() — persist payload to disk for later replay.
        5. Repeat forever.

    Reason:
        Separates "try to clear old data" from "collect new data" so recovery is
        automatic when the API comes back. The loop interval is driven by CPU
        sampling (interval=) rather than an extra sleep.
    """
    while True:
        replay_spool()

        payload = collect_metrics()

        if send_metrics_with_retry(payload):
            pass  # success
        else:
            spool.append(payload)
            print(f"[SPOOL] Buffered payload on disk (total={spool.size()})")


if __name__ == "__main__":
    main()