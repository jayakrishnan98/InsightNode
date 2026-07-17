"""
Background worker — drains the Redis ingest queue and batch-writes to PostgreSQL.

Runs in a separate thread (started from backend.main lifespan). Batching reduces
commit overhead; failed batches are re-queued with a retry cap to handle transient DB errors.
"""

import logging
import threading

from redis import Redis
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import text
from sqlalchemy.orm import Session

from backend.database import SessionLocal
from backend.models import MetricRecord
from backend.redis_client import (
    QueueFullError,
    dequeue_payload,
    enqueue_payload_retry,
    try_dequeue_nowait,
)

logger = logging.getLogger(__name__)

BATCH_SIZE = 50
BATCH_TIMEOUT_SECONDS = 1.0
MAX_BATCH_RETRIES = 3


def _payload_to_rows(payload: dict) -> list[dict]:
    """
    Expand one agent payload into one MetricRecord per metric in the payload.

    Logic:
        - Read machine_id and timestamp once from the payload envelope.
        - For each entry in payload["metrics"], create a MetricRecord row.

    Reason:
        Returns plain dicts (not ORM objects) so we can use PostgreSQL-specific
        INSERT ... ON CONFLICT DO NOTHING for idempotent writes.
    """
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
    """
    Insert all rows from a batch, skipping duplicates (idempotent).

    Logic:
        - Flatten payloads to row dicts.
        - PostgreSQL INSERT with ON CONFLICT DO NOTHING on dedup index.
        - Single commit per batch.

    Reason:
        Spool replay and retries may send the same event_id twice. Without this,
        unique constraint violations would fail the whole batch. DO NOTHING
        silently skips duplicates — safe for at-least-once delivery.
    """
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


def run_ingest_worker(redis_client: Redis, stop_event: threading.Event) -> None:
    """
    Drain Redis ingest list and batch-write to PostgreSQL.

    Logic:
        1. BRPOP first item (timeout = BATCH_TIMEOUT_SECONDS).
        2. RPOP extras non-blocking up to BATCH_SIZE (FIFO drain).
        3. _flush_batch() once.
        4. On DB error: re-LPUSH each payload if _retry_count < MAX_BATCH_RETRIES.
        5. On stop_event + empty: exit.

    Reason:
        Same batching idea as Phase 1; only the buffer moved to Redis.
        Blocking wait for first item, then grab whatever else is ready.
    """
    while True:
        first = dequeue_payload(redis_client, timeout_seconds=BATCH_TIMEOUT_SECONDS)
        if first is None:
            if stop_event.is_set():
                break
            continue

        batch: list[dict] = [first]
        while len(batch) < BATCH_SIZE:
            item = try_dequeue_nowait(redis_client)
            if item is None:
                break
            batch.append(item)

        db = SessionLocal()
        try:
            _flush_batch(db, batch)
        except Exception:
            logger.exception("Failed to persist batch of %s payloads", len(batch))
            db.rollback()

            for payload in batch:
                retries = payload.get("_retry_count", 0)
                if retries < MAX_BATCH_RETRIES:
                    payload["_retry_count"] = retries + 1
                    try:
                        enqueue_payload_retry(redis_client, payload)
                        logger.warning(
                            "Re-queued payload machine=%s retry=%s",
                            payload.get("machine_id"),
                            payload["_retry_count"],
                        )
                    except QueueFullError:
                        logger.error(
                            "Queue full — dropping payload machine=%s after DB failure",
                            payload.get("machine_id"),
                        )
                else:
                    logger.error(
                        "Dropping payload machine=%s after %s failed attempts",
                        payload.get("machine_id"),
                        MAX_BATCH_RETRIES,
                    )
        finally:
            db.close()
            # No task_done() — Redis List has no join semantics
