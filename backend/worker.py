"""
Background worker — drains Kafka ingest topic and batch-writes to PostgreSQL.

Phase 2 Day 5: Kafka consumer group replaces Redis Streams.
  - Manual offset commit after successful DB write (ACK-after-commit).
  - Poison messages after MAX_DELIVERIES → DLQ topic.
"""

from __future__ import annotations

import logging
import os
import signal
import socket
import threading
import time
from collections import defaultdict

from kafka import KafkaConsumer, KafkaProducer, OffsetAndMetadata, TopicPartition
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import text
from sqlalchemy.orm import Session

from backend.database import SessionLocal
from backend.kafka_client import (
    CONSUMER_GROUP,
    MAX_DELIVERIES,
    ensure_topics,
    get_consumer,
    get_producer,
    publish_dlq,
)
from backend.models import MetricRecord

logger = logging.getLogger(__name__)

BATCH_SIZE = 50
POLL_TIMEOUT_MS = 1000


def default_consumer_name() -> str:
    """Unique client id for logging (Kafka group id is shared)."""
    explicit = os.getenv("WORKER_NAME")
    if explicit:
        return explicit
    host = socket.gethostname().split(".")[0]
    return f"worker-{host}-{os.getpid()}"


def _payload_to_rows(payload: dict) -> list[dict]:
    """Expand one agent payload into ORM row dicts (one per metric)."""
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
    """Idempotent batch insert into PostgreSQL."""
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


def _commit_messages(consumer: KafkaConsumer, messages: list) -> None:
    """Commit offsets past each message (offset + 1)."""
    if not messages:
        return
    offsets: dict[TopicPartition, OffsetAndMetadata] = {}
    for msg in messages:
        tp = TopicPartition(msg.topic, msg.partition)
        # Keep the highest offset+1 per partition
        next_offset = msg.offset + 1
        current = offsets.get(tp)
        if current is None or next_offset > current.offset:
            offsets[tp] = OffsetAndMetadata(next_offset, None)
    consumer.commit(offsets)


def _handle_failed_messages(
    consumer: KafkaConsumer,
    producer: KafkaProducer,
    messages: list,
    error: Exception,
    fail_counts: dict[str, int],
) -> None:
    """
    Per-message recovery after a batch failure.

    Logic:
        - Try each message alone.
        - Success → commit that message's offset.
        - Fail: increment in-memory fail_counts[event_id].
          If >= MAX_DELIVERIES → DLQ + commit (stop retrying).
          Else leave uncommitted so poll redelivers.
    """
    reason = f"{type(error).__name__}: {error}"

    for msg in messages:
        payload = msg.value
        event_id = str(payload.get("event_id", f"{msg.partition}:{msg.offset}"))

        db = SessionLocal()
        try:
            _flush_batch(db, [payload])
            _commit_messages(consumer, [msg])
            fail_counts.pop(event_id, None)
            logger.info("Recovered message event_id=%s on per-item retry", event_id)
        except Exception as item_error:
            db.rollback()
            fail_counts[event_id] = fail_counts.get(event_id, 0) + 1
            deliveries = fail_counts[event_id]
            if deliveries >= MAX_DELIVERIES:
                publish_dlq(
                    producer,
                    payload=payload,
                    reason=f"{reason} | last={type(item_error).__name__}: {item_error}",
                    delivery_count=deliveries,
                )
                _commit_messages(consumer, [msg])
                fail_counts.pop(event_id, None)
                logger.error(
                    "Moved poison message event_id=%s to DLQ after %s deliveries",
                    event_id,
                    deliveries,
                )
            else:
                logger.warning(
                    "Leaving message event_id=%s uncommitted (deliveries=%s/%s)",
                    event_id,
                    deliveries,
                    MAX_DELIVERIES,
                )
        finally:
            db.close()


def run_ingest_worker(
    stop_event: threading.Event,
    consumer_name: str | None = None,
) -> None:
    """
    Poll Kafka ingest topic; commit offsets only after DB success.

    Logic:
        1. poll() up to BATCH_SIZE records.
        2. Flush payloads to PostgreSQL.
        3. On success → commit offsets.
        4. On failure → per-message retry / DLQ.
        5. Exit when stop_event set and idle.
    """
    name = consumer_name or default_consumer_name()
    consumer = get_consumer(group_id=CONSUMER_GROUP)
    producer = get_producer()
    fail_counts: dict[str, int] = defaultdict(int)

    logger.info(
        "Kafka ingest worker starting name=%s group=%s max_deliveries=%s",
        name,
        CONSUMER_GROUP,
        MAX_DELIVERIES,
    )

    while True:
        records = consumer.poll(
            timeout_ms=POLL_TIMEOUT_MS,
            max_records=BATCH_SIZE,
        )

        messages: list = []
        for _tp, msgs in records.items():
            messages.extend(msgs)

        if not messages:
            if stop_event.is_set():
                break
            continue

        payloads = [m.value for m in messages]
        db = SessionLocal()
        try:
            _flush_batch(db, payloads)
            _commit_messages(consumer, messages)
            for m in messages:
                eid = str(m.value.get("event_id", ""))
                fail_counts.pop(eid, None)
        except Exception as exc:
            logger.exception(
                "Failed to persist batch of %s messages — falling back per-item",
                len(messages),
            )
            db.rollback()
            _handle_failed_messages(consumer, producer, messages, exc, fail_counts)
            time.sleep(1)
        finally:
            db.close()

    consumer.close()
    producer.close()
    logger.info("Kafka ingest worker stopped name=%s", name)


def main() -> None:
    """Standalone worker entrypoint: python -m backend.worker"""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    ensure_topics()

    stop_event = threading.Event()
    consumer_name = default_consumer_name()

    def _handle_signal(signum: int, _frame: object) -> None:
        logger.info("Received signal %s — shutting down worker", signum)
        stop_event.set()

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    logger.info(
        "Standalone Kafka worker starting name=%s bootstrap=%s",
        consumer_name,
        os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"),
    )
    run_ingest_worker(stop_event, consumer_name=consumer_name)


if __name__ == "__main__":
    main()
