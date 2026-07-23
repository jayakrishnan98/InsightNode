#!/usr/bin/env python3
"""
Fleet load simulator — multiple machines posting metrics (and optional logs).

Lab-scale only. Do not claim production throughput without measured results.

Usage (from project root, venv active, API + worker running):

    python tests/load/fleet_simulator.py --machines 10 --interval 1 --duration 30
    python tests/load/fleet_simulator.py --machines 20 --duration 60 --logs --concurrency 40
"""

from __future__ import annotations

import argparse
import asyncio
import random
import statistics
import time
import uuid
from collections import Counter
from datetime import datetime, timezone

import httpx


def percentile(sorted_values: list[float], p: float) -> float:
    if not sorted_values:
        return 0.0
    if len(sorted_values) == 1:
        return sorted_values[0]
    k = (len(sorted_values) - 1) * (p / 100.0)
    f = int(k)
    c = min(f + 1, len(sorted_values) - 1)
    return sorted_values[f] + (sorted_values[c] - sorted_values[f]) * (k - f)


def build_metrics_payload(machine_id: str) -> dict:
    return {
        "event_id": str(uuid.uuid4()),
        "machine_id": machine_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "metrics": [
            {
                "name": "cpu_usage",
                "value": round(random.uniform(5.0, 95.0), 1),
                "unit": "percent",
            },
            {
                "name": "memory_usage",
                "value": round(random.uniform(20.0, 90.0), 1),
                "unit": "percent",
            },
            {
                "name": "disk_usage",
                "value": round(random.uniform(30.0, 85.0), 1),
                "unit": "percent",
            },
        ],
    }


def build_log_payload(machine_id: str) -> dict:
    return {
        "logs": [
            {
                "event_id": str(uuid.uuid4()),
                "machine_id": machine_id,
                "service": "fleet-simulator",
                "level": random.choice(["info", "warn", "error"]),
                "message": f"simulated log from {machine_id}",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "attrs": {"source": "fleet_simulator"},
            }
        ]
    }


async def run_fleet(
    *,
    base_url: str,
    api_key: str,
    machines: int,
    interval: float,
    duration: float,
    concurrency: int,
    include_logs: bool,
    fail_rate: float,
    timeout: float,
    machine_prefix: str,
) -> None:
    metrics_url = base_url.rstrip("/") + "/metrics"
    logs_url = base_url.rstrip("/") + "/logs"
    headers = {"X-API-Key": api_key, "Content-Type": "application/json"}
    machine_ids = [f"{machine_prefix}-{i:03d}" for i in range(machines)]

    statuses: Counter[str] = Counter()
    log_statuses: Counter[str] = Counter()
    errors: Counter[str] = Counter()
    latencies: list[float] = []
    metric_posts = 0
    log_posts = 0
    simulated_fails = 0

    sem = asyncio.Semaphore(concurrency)
    stop_at = time.perf_counter() + duration

    print("Fleet simulator (lab-scale)")
    print(f"  API:          {base_url}")
    print(f"  Machines:     {machines} ({machine_prefix}-*)")
    print(f"  Interval:     {interval}s")
    print(f"  Duration:     {duration}s")
    print(f"  Concurrency:  {concurrency}")
    print(f"  Logs:         {include_logs}")
    print(f"  Fail rate:    {fail_rate:.0%} (client-side skip)")
    print()

    async with httpx.AsyncClient(timeout=timeout, headers=headers) as client:

        async def tick(machine_id: str) -> None:
            nonlocal metric_posts, log_posts, simulated_fails

            if fail_rate > 0 and random.random() < fail_rate:
                simulated_fails += 1
                errors["simulated_skip"] += 1
                return

            async with sem:
                started = time.perf_counter()
                try:
                    response = await client.post(
                        metrics_url,
                        json=build_metrics_payload(machine_id),
                    )
                    latencies.append(time.perf_counter() - started)
                    statuses[str(response.status_code)] += 1
                    metric_posts += 1
                except Exception as exc:
                    latencies.append(time.perf_counter() - started)
                    errors[type(exc).__name__] += 1

                if include_logs:
                    try:
                        response = await client.post(
                            logs_url,
                            json=build_log_payload(machine_id),
                        )
                        log_statuses[str(response.status_code)] += 1
                        log_posts += 1
                    except Exception as exc:
                        errors[f"log:{type(exc).__name__}"] += 1

        wall_start = time.perf_counter()
        while time.perf_counter() < stop_at:
            loop_start = time.perf_counter()
            await asyncio.gather(*(tick(mid) for mid in machine_ids))
            elapsed = time.perf_counter() - loop_start
            sleep_for = interval - elapsed
            if sleep_for > 0 and time.perf_counter() + sleep_for < stop_at:
                await asyncio.sleep(sleep_for)

        wall_elapsed = time.perf_counter() - wall_start

    latencies.sort()
    accepted = statuses.get("202", 0)
    points_accepted = accepted * 3  # cpu + memory + disk

    print("Summary:")
    print(f"  Wall time:           {wall_elapsed:.2f}s")
    print(f"  Metric POSTs:        {metric_posts}")
    print(f"  Accepted (202):      {accepted}")
    print(f"  Metric points ~:     {points_accepted} (3 × accepted)")
    print(f"  Events/sec (POST):   {metric_posts / wall_elapsed:.2f}" if wall_elapsed else "  Events/sec: n/a")
    print(
        f"  Points/sec ~:        {points_accepted / wall_elapsed:.2f}"
        if wall_elapsed
        else "  Points/sec: n/a"
    )
    if include_logs:
        print(f"  Log POSTs:           {log_posts}")
        print(f"  Log status counts:   {dict(sorted(log_statuses.items()))}")
    print(f"  Simulated skips:     {simulated_fails}")
    print()

    if latencies:
        print("Metric POST latency:")
        print(f"  min:  {latencies[0]*1000:.2f} ms")
        print(f"  avg:  {statistics.mean(latencies)*1000:.2f} ms")
        print(f"  p50:  {percentile(latencies, 50)*1000:.2f} ms")
        print(f"  p95:  {percentile(latencies, 95)*1000:.2f} ms")
        print(f"  p99:  {percentile(latencies, 99)*1000:.2f} ms")
        print(f"  max:  {latencies[-1]*1000:.2f} ms")
        print()

    print("Metric status codes:")
    for code, count in sorted(statuses.items()):
        print(f"  [{code}] {count}")
    if errors:
        print()
        print("Errors / skips:")
        for name, count in sorted(errors.items()):
            print(f"  [{count}] {name}")

    print()
    print("Follow-ups:")
    print("  curl -H 'X-API-Key: …' http://127.0.0.1:8001/system/summary")
    print("  curl -H 'X-API-Key: …' http://127.0.0.1:8001/pipeline")
    print("  Grafana: Infrastructure + Platform dashboards")


def main() -> None:
    parser = argparse.ArgumentParser(description="Multi-machine InsightNode fleet simulator")
    parser.add_argument("--machines", type=int, default=10, help="Simulated hosts (default 10)")
    parser.add_argument(
        "--interval",
        type=float,
        default=1.0,
        help="Seconds between rounds (default 1)",
    )
    parser.add_argument(
        "--duration",
        type=float,
        default=30.0,
        help="Total run duration in seconds (default 30)",
    )
    parser.add_argument("--concurrency", type=int, default=20, help="Max in-flight POSTs")
    parser.add_argument(
        "--url",
        default="http://127.0.0.1:8001",
        help="API base URL",
    )
    parser.add_argument(
        "--api-key",
        default="dev-local-key",
        help="X-API-Key (default lab key)",
    )
    parser.add_argument("--logs", action="store_true", help="Also POST /logs each tick")
    parser.add_argument(
        "--fail-rate",
        type=float,
        default=0.0,
        help="Fraction of ticks to skip client-side (0–1)",
    )
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument(
        "--machine-prefix",
        default="fleet",
        help="Prefix for machine_id values",
    )
    args = parser.parse_args()

    if args.machines < 1:
        parser.error("--machines must be >= 1")
    if args.interval <= 0:
        parser.error("--interval must be > 0")
    if args.duration <= 0:
        parser.error("--duration must be > 0")
    if not 0 <= args.fail_rate < 1:
        parser.error("--fail-rate must be in [0, 1)")

    asyncio.run(
        run_fleet(
            base_url=args.url,
            api_key=args.api_key,
            machines=args.machines,
            interval=args.interval,
            duration=args.duration,
            concurrency=args.concurrency,
            include_logs=args.logs,
            fail_rate=args.fail_rate,
            timeout=args.timeout,
            machine_prefix=args.machine_prefix,
        )
    )


if __name__ == "__main__":
    main()
