"""
Background worker — drains the Redis Stream and batch-writes to PostgreSQL.

Phase 2 Day 3: runs as a separate process; multiple workers share one group.
Phase 2 Day 4: after MAX_DELIVERIES failures, poison messages go to the DLQ.
"""

from __future__ import annotations

import logging
import os
import signal
import socket
import threading
import time

from redis import Redis
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import text
from sqlalchemy.orm import Session

from backend.database import SessionLocal
from backend.models import MetricRecord
from backend.redis_client import (
    MAX_DELIVERIES,
    ack_messages,
    claim_stale_messages,
    ensure_consumer_group,
    get_delivery_count,
    get_redis,
    move_to_dlq,
    read_batch,
)

logger = logging.getLogger(__name__)

BATCH_SIZE = 50
BATCH_TIMEOUT_SECONDS = 1.0
STALE_IDLE_MS = int(os.getenv("STALE_IDLE_MS", "60000"))


def default_consumer_name() -> str:
    explicit = os.getenv("WORKER_NAME")
    if explicit:
        return explicit
    host = socket.gethostname().split(".")[0]
    return f"worker-{host}-{os.getpid()}"


def _payload_to_rows(payload: dict) -> list[dict]:
    event_id = payload.get("event_id")
    return [
        {
            "machine_id": payload["machine_id"],
            "metric_name": metric["name"],
            "value": metric["value"],
            "unit": metric["unit"],
            "timestamp": payload["timestamp"],
            "event_id": event_id,
        }
        for metric in payload["metrics"]
    ]


def _flush_batch(db: Session, batch: list[dict]) -> None:
    rows: list[dict] = []
    for payload in batch:
        rows.extend(_payload_to_rows(payload))
    if not rows:
        return
    stmt = insert(MetricRecord).values(rows)
    stmt = stmt.on_conflict_do_nothing(
        index_elements=["machine_id", "event_id", "metric_name"],
        index_where=text("event_id IS NOT NULL"),
    )
    result = db.execute(stmt)
    db.commit()
    logger.info(
        "Persisted batch: %s row(s) submitted, %s new (rest were duplicates)",
        len(rows),
        result.rowcount,
    )


def _handle_failed_messages(
    redis_client: Redis,
    batch_entries: list[tuple[str, dict]],
    error: Exception,
) -> None:
    reason = f"{type(error).__name__}: {error}"
    for message_id, payload in batch_entries:
        db = SessionLocal()
        try:
            _flush_batch(db, [payload])
            ack_messages(redis_client, [message_id])
            logger.info("Recovered message %s on per-item retry", message_id)
        except Exception as item_error:
            db.rollback()
            deliveries = get_delivery_count(redis_client, message_id)
            if deliveries >= MAX_DELIVERIES:
                dlq_id = move_to_dlq(
                    redis_client,
                    message_id=message_id,
                    payload=payload,
                    reason=f"{reason} | last={type(item_error).__name__}: {item_error}",
                    delivery_count=deliveries,
                )
                logger.error(
                    "Moved poison message %s to DLQ %s after %s deliveries",
                    message_id,
                    dlq_id,
                    deliveries,
                )
            else:
                logger.warning(
                    "Leaving message %s un-ACK'd (deliveries=%s/%s)",
                    message_id,
                    deliveries,
                    MAX_DELIVERIES,
                )
        finally:
            db.close()


def run_ingest_worker(
    redis_client: Redis,
    stop_event: threading.Event,
    consumer_name: str | None = None,
) -> None:
    name = consumer_name or default_consumer_name()
    logger.info(
        "Ingest worker loop starting consumer_name=%s max_deliveries=%s",
        name,
        MAX_DELIVERIES,
    )

    while True:
        batch_entries = claim_stale_messages(
            redis_client,
            consumer_name=name,
            min_idle_ms=STALE_IDLE_MS,
            count=BATCH_SIZE,
        )
        if not batch_entries:
            batch_entries = read_batch(
                redis_client,
                consumer_name=name,
                count=BATCH_SIZE,
                block_ms=int(BATCH_TIMEOUT_SECONDS * 1000),
            )

        if not batch_entries:
            if stop_event.is_set():
                break
            continue

        message_ids = [mid for mid, _ in batch_entries]
        payloads = [payload for _, payload in batch_entries]

        db = SessionLocal()
        try:
            _flush_batch(db, payloads)
            ack_messages(redis_client, message_ids)
        except Exception as exc:
            logger.exception(
                "Failed to persist batch of %s messages — falling back per-item",
                len(message_ids),
            )
            db.rollback()
            _handle_failed_messages(redis_client, batch_entries, exc)
            time.sleep(1)
        finally:
            db.close()

    logger.info("Ingest worker loop stopped consumer_name=%s", name)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    redis_client = get_redis()
    ensure_consumer_group(redis_client)
    stop_event = threading.Event()
    consumer_name = default_consumer_name()

    def _handle_signal(signum: int, _frame: object) -> None:
        logger.info("Received signal %s — shutting down worker", signum)
        stop_event.set()

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)
    logger.info("Standalone worker starting consumer_name=%s", consumer_name)
    run_ingest_worker(redis_client, stop_event, consumer_name=consumer_name)


if __name__ == "__main__":
    main()
