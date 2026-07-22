#!/usr/bin/env python3
"""
Seed identical metric rows into PostgreSQL and ClickHouse for Day 4 compares.

Bypasses Kafka so both stores get the same dataset quickly.

Usage (from project root, venv active):

    python tests/load/seed_aggregate_compare.py
    python tests/load/seed_aggregate_compare.py --rows 100000 --machine-id compare-bench

Then:

    curl "http://127.0.0.1:8001/metrics/aggregate/compare?machine_id=compare-bench&metric_name=cpu_usage&start_time=2020-01-01T00:00:00Z&end_time=2030-01-01T00:00:00Z&interval=5m&runs=5"
"""

from __future__ import annotations

import argparse
import math
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

# Allow `python tests/load/seed_aggregate_compare.py` from project root.
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import text

from backend.clickhouse_client import (
    close_client,
    ensure_schema,
    insert_metrics,
)
from backend.database import SessionLocal
from backend.models import MetricRecord


def build_rows(
    *,
    machine_id: str,
    metric_name: str,
    n: int,
    start: datetime,
    step_seconds: int,
) -> list[dict]:
    rows: list[dict] = []
    for i in range(n):
        ts = start + timedelta(seconds=i * step_seconds)
        # Gentle sine so aggregates are non-trivial but deterministic-ish
        value = 50.0 + 20.0 * math.sin(i / 17.0)
        rows.append(
            {
                "machine_id": machine_id,
                "metric_name": metric_name,
                "value": value,
                "unit": "percent",
                "timestamp": ts,
                "event_id": str(uuid4()),
            }
        )
    return rows


def insert_postgres(rows: list[dict], batch_size: int = 2000) -> int:
    db = SessionLocal()
    inserted = 0
    try:
        for i in range(0, len(rows), batch_size):
            chunk = rows[i : i + batch_size]
            stmt = insert(MetricRecord).values(chunk)
            stmt = stmt.on_conflict_do_nothing(
                index_elements=["machine_id", "event_id", "metric_name"],
                index_where=text("event_id IS NOT NULL"),
            )
            result = db.execute(stmt)
            db.commit()
            inserted += result.rowcount or 0
            print(f"  PG  {min(i + batch_size, len(rows))}/{len(rows)}")
    finally:
        db.close()
    return inserted


def insert_clickhouse(rows: list[dict], batch_size: int = 5000) -> None:
    ensure_schema()
    for i in range(0, len(rows), batch_size):
        chunk = rows[i : i + batch_size]
        insert_metrics(chunk)
        print(f"  CH  {min(i + batch_size, len(rows))}/{len(rows)}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Seed PG + CH for aggregate compare")
    parser.add_argument("--rows", type=int, default=50_000, help="Rows per store")
    parser.add_argument("--machine-id", default="compare-bench")
    parser.add_argument("--metric-name", default="cpu_usage")
    parser.add_argument(
        "--start",
        default="2026-06-01T00:00:00+00:00",
        help="ISO start timestamp for the series",
    )
    parser.add_argument(
        "--step-seconds",
        type=int,
        default=5,
        help="Seconds between samples (default 5 ≈ agent cadence)",
    )
    args = parser.parse_args()

    start = datetime.fromisoformat(args.start.replace("Z", "+00:00"))
    if start.tzinfo is None:
        start = start.replace(tzinfo=timezone.utc)

    print(f"Building {args.rows} rows for machine_id={args.machine_id!r} …")
    rows = build_rows(
        machine_id=args.machine_id,
        metric_name=args.metric_name,
        n=args.rows,
        start=start,
        step_seconds=args.step_seconds,
    )
    end = rows[-1]["timestamp"]

    print("Inserting into PostgreSQL …")
    pg_new = insert_postgres(rows)
    print(f"PostgreSQL new rows: {pg_new}")

    print("Inserting into ClickHouse …")
    insert_clickhouse(rows)
    close_client()

    print(
        "\nDone. Compare with:\n"
        f'  curl "http://127.0.0.1:8001/metrics/aggregate/compare'
        f"?machine_id={args.machine_id}&metric_name={args.metric_name}"
        f"&start_time={start.isoformat().replace('+', '%2B')}"
        f"&end_time={(end + timedelta(seconds=1)).isoformat().replace('+', '%2B')}"
        f'&interval=5m&runs=5"\n'
    )


if __name__ == "__main__":
    main()
