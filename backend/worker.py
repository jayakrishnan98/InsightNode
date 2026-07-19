"""
Background worker — drains the Redis Stream and batch-writes to PostgreSQL.

Uses a consumer group: messages stay in the Pending Entries List until XACK
after a successful DB commit (Phase 2 Day 2).
"""

import logging
import threading
import time

from redis import Redis
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import text
from sqlalchemy.orm import Session

from backend.database import SessionLocal
from backend.models import MetricRecord
from backend.redis_client import (
    ack_messages,
    claim_stale_messages,
    read_batch,
)

logger = logging.getLogger(__name__)

BATCH_SIZE = 50
BATCH_TIMEOUT_SECONDS = 1.0
CONSUMER_NAME = "worker-1"
STALE_IDLE_MS = 60_000


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


def run_ingest_worker(redis_client: Redis, stop_event: threading.Event) -> None:
    """Drain Redis Stream via consumer group; ACK only after DB success."""
    while True:
        batch_entries = claim_stale_messages(
            redis_client,
            consumer_name=CONSUMER_NAME,
            min_idle_ms=STALE_IDLE_MS,
            count=BATCH_SIZE,
        )
        if not batch_entries:
            batch_entries = read_batch(
                redis_client,
                consumer_name=CONSUMER_NAME,
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
        except Exception:
            logger.exception(
                "Failed to persist batch of %s messages — leaving un-ACK'd",
                len(message_ids),
            )
            db.rollback()
            time.sleep(1)
        finally:
            db.close()
