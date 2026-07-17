#!/usr/bin/env python3
"""
Load-test POST /metrics with a unique event_id per request.

Unlike hey (-D file), every request gets a fresh UUID so PostgreSQL
actually inserts rows instead of ON CONFLICT DO NOTHING.

Usage (from project root, venv active):

    python tests/load/unique_post.py --n 1000 --c 50
    python tests/load/unique_post.py --n 5000 --c 100 --url http://127.0.0.1:8001
"""

from __future__ import annotations

import argparse
import asyncio
import statistics
import time
import uuid
from collections import Counter
from datetime import datetime, timezone

import httpx


def build_payload(machine_id: str) -> dict:
    return {
        "event_id": str(uuid.uuid4()),
        "machine_id": machine_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "metrics": [
            {"name": "cpu_usage", "value": 45.2, "unit": "percent"},
            {"name": "memory_usage", "value": 62.1, "unit": "percent"},
            {"name": "disk_usage", "value": 78.0, "unit": "percent"},
        ],
    }


def percentile(sorted_values: list[float], p: float) -> float:
    if not sorted_values:
        return 0.0
    if len(sorted_values) == 1:
        return sorted_values[0]
    k = (len(sorted_values) - 1) * (p / 100.0)
    f = int(k)
    c = min(f + 1, len(sorted_values) - 1)
    return sorted_values[f] + (sorted_values[c] - sorted_values[f]) * (k - f)


async def run_load(
    url: str,
    total: int,
    concurrency: int,
    machine_id: str,
    timeout: float,
) -> None:
    endpoint = url.rstrip("/") + "/metrics"
    statuses: Counter[str] = Counter()
    latencies: list[float] = []
    errors: Counter[str] = Counter()
    sem = asyncio.Semaphore(concurrency)

    async with httpx.AsyncClient(timeout=timeout) as client:

        async def one_request() -> None:
            payload = build_payload(machine_id)
            async with sem:
                started = time.perf_counter()
                try:
                    response = await client.post(endpoint, json=payload)
                    elapsed = time.perf_counter() - started
                    latencies.append(elapsed)
                    statuses[str(response.status_code)] += 1
                except Exception as exc:
                    elapsed = time.perf_counter() - started
                    latencies.append(elapsed)
                    errors[type(exc).__name__] += 1

        print(f"POST {endpoint}")
        print(f"Requests: {total}  Concurrency: {concurrency}  machine_id: {machine_id}")
        print("Each request uses a unique event_id (real DB inserts).\n")

        wall_start = time.perf_counter()
        await asyncio.gather(*(one_request() for _ in range(total)))
        wall_elapsed = time.perf_counter() - wall_start

    latencies.sort()
    ok = statuses.get("202", 0)
    failed = total - ok

    print("Summary:")
    print(f"  Total:        {wall_elapsed:.4f} secs")
    print(f"  Slowest:      {latencies[-1]:.4f} secs" if latencies else "  Slowest:      n/a")
    print(f"  Fastest:      {latencies[0]:.4f} secs" if latencies else "  Fastest:      n/a")
    print(f"  Average:      {statistics.mean(latencies):.4f} secs" if latencies else "  Average:      n/a")
    print(f"  Requests/sec: {total / wall_elapsed:.4f}" if wall_elapsed else "  Requests/sec: n/a")
    print()
    print("Latency distribution:")
    for p in (10, 25, 50, 75, 90, 95, 99):
        print(f"  {p}% in {percentile(latencies, p):.4f} secs")
    print()
    print("Status code distribution:")
    for code, count in sorted(statuses.items()):
        print(f"  [{code}] {count} responses")
    if errors:
        print()
        print("Error distribution:")
        for name, count in sorted(errors.items()):
            print(f"  [{count}] {name}")
    print()
    print(f"Accepted (202): {ok}   Failed/other: {failed}")
    print()
    print("Verify DB growth (expect ~3 new rows per accepted request):")
    print(
        f'  psql -U insightnode -d insightnode -c '
        f'"SELECT COUNT(*) FROM metrics WHERE machine_id = \'{machine_id}\';"'
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="POST /metrics load test with unique event_id per request",
    )
    parser.add_argument("--n", type=int, default=1000, help="Total requests (default: 1000)")
    parser.add_argument("--c", type=int, default=50, help="Concurrency (default: 50)")
    parser.add_argument(
        "--url",
        default="http://127.0.0.1:8001",
        help="API base URL (default: http://127.0.0.1:8001)",
    )
    parser.add_argument(
        "--machine-id",
        default="load-test-unique",
        help="machine_id for all payloads (default: load-test-unique)",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=30.0,
        help="Per-request timeout in seconds (default: 30)",
    )
    args = parser.parse_args()

    if args.n < 1:
        parser.error("--n must be >= 1")
    if args.c < 1:
        parser.error("--c must be >= 1")

    asyncio.run(
        run_load(
            url=args.url,
            total=args.n,
            concurrency=args.c,
            machine_id=args.machine_id,
            timeout=args.timeout,
        )
    )


if __name__ == "__main__":
    main()
