import socket
import time

import httpx
import psutil
from datetime import datetime, timezone

MACHINE_ID = socket.gethostname()
COLLECTION_INTERVAL_SECONDS = 5
API_URL = "http://127.0.0.1:8000/metrics"

# Retry configuration
MAX_RETRIES = 5
BASE_RETRY_DELAY_SECONDS = 1
REQUEST_TIMEOUT_SECONDS = 10.0


def collect_metrics() -> dict:
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


def send_metrics(payload: dict) -> None:
    response = httpx.post(API_URL, json=payload, timeout=REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()
    print(f"[SENT] {response.status_code} → {response.json()}")


def is_retryable(error: httpx.HTTPError) -> bool:
    """Return True if the request should be retried."""
    if isinstance(error, httpx.HTTPStatusError):
        status = error.response.status_code
        # 5xx = server error, 429 = rate limited — retry may succeed
        # 4xx (except 429) = client error — retrying won't help
        return status >= 500 or status == 429
    # Connection refused, timeouts, DNS failures, etc.
    return True


def send_metrics_with_retry(payload: dict) -> bool:
    """
    Try to send metrics with exponential backoff.
    Returns True if sent successfully, False if all attempts failed.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            send_metrics(payload)
            if attempt > 1:
                print(f"[RECOVERED] Succeeded on attempt {attempt}")
            return True
        except httpx.HTTPError as e:
            is_last_attempt = attempt == MAX_RETRIES
            if not is_retryable(e) or is_last_attempt:
                print(f"[ERROR] Failed after {attempt} attempt(s): {e}")
                return False

            delay = BASE_RETRY_DELAY_SECONDS * (2 ** (attempt - 1))
            print(
                f"[RETRY] Attempt {attempt}/{MAX_RETRIES} failed: {e}. "
                f"Retrying in {delay}s..."
            )
            time.sleep(delay)

    return False


def main() -> None:
    while True:
        payload = collect_metrics()
        send_metrics_with_retry(payload)


if __name__ == "__main__":
    main()
