"""
Background worker — drains Kafka ingest topic and dual-writes storage.

Phase 2: Kafka consumer group, ACK-after-commit, DLQ for poison messages.
Phase 3 Day 2: each successful batch also inserts into ClickHouse.
Phase 4 Day 4: ships structured ops logs for DLQ / delivery failures.
Phase 5 Day 3: extract W3C trace context from Kafka headers → continue traces.
Phase 5 Day 4: manual dual-write spans (PG + ClickHouse) under kafka.consume.
Phase 5 complete: see docs/phase-5-graduation.md.
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

from backend.clickhouse_client import (
    close_client as close_clickhouse,
    ensure_schema as ensure_clickhouse_schema,
    insert_metrics as insert_clickhouse_metrics,
)
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
from backend import logship as backend_logship
from backend.tracing import (
    kafka_consume_span,
    manual_span,
    record_span_error,
    setup_tracing,
    shutdown_tracing,
)

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
    """
    Dual-write one batch: PostgreSQL then ClickHouse.

    Logic:
        - Expand payloads → row dicts.
        - Under a dual_write parent span: PG insert, then ClickHouse insert
          (Phase 5 Day 4 — shows where worker time goes).
        - Idempotent INSERT into PostgreSQL (ON CONFLICT DO NOTHING).
        - Append the same rows into ClickHouse.
        - Caller commits Kafka offsets only if this returns without error.

    Reason:
        PG first keeps the deduped source of truth. If CH fails, the offset is
        not committed and Kafka redelivers; PG ignores duplicates, CH retries.
        If both succeed but offset commit fails, CH may see duplicates — Day 2
        accepts that until ReplacingMergeTree / stronger dedup.
    """
    rows: list[dict] = []
    for payload in batch:
        rows.extend(_payload_to_rows(payload))

    if not rows:
        return

    event_ids = sorted(
        {str(r["event_id"]) for r in rows if r.get("event_id") is not None}
    )
    attrs = {
        "insightnode.row_count": len(rows),
        "insightnode.payload_count": len(batch),
    }
    if len(event_ids) == 1:
        attrs["insightnode.event_id"] = event_ids[0]
    elif event_ids:
        attrs["insightnode.event_id_count"] = len(event_ids)

    with manual_span("dual_write", attributes=attrs):
        with manual_span(
            "db.postgres.insert",
            attributes={"db.system": "postgresql", "insightnode.row_count": len(rows)},
        ) as pg_span:
            stmt = insert(MetricRecord).values(rows)
            stmt = stmt.on_conflict_do_nothing(
                index_elements=["machine_id", "event_id", "metric_name"],
                index_where=text("event_id IS NOT NULL"),
            )
            result = db.execute(stmt)
            db.commit()
            pg_span.set_attribute("insightnode.pg_new_rows", int(result.rowcount or 0))

        with manual_span(
            "db.clickhouse.insert",
            attributes={"db.system": "clickhouse", "insightnode.row_count": len(rows)},
        ):
            insert_clickhouse_metrics(rows)

    logger.info(
        "Persisted batch: %s row(s) submitted, %s new in PG (+ ClickHouse)",
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
        - Try each message alone (same dual-write path).
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
            with kafka_consume_span(
                msg.headers,
                destination=msg.topic,
                event_id=event_id,
                offset=msg.offset,
                partition=msg.partition,
            ) as span:
                try:
                    _flush_batch(db, [payload])
                    _commit_messages(consumer, [msg])
                    fail_counts.pop(event_id, None)
                    logger.info(
                        "Recovered message event_id=%s on per-item retry",
                        event_id,
                    )
                except Exception as item_error:
                    record_span_error(span, item_error)
                    db.rollback()
                    fail_counts[event_id] = fail_counts.get(event_id, 0) + 1
                    deliveries = fail_counts[event_id]
                    if deliveries >= MAX_DELIVERIES:
                        publish_dlq(
                            producer,
                            payload=payload,
                            reason=(
                                f"{reason} | last="
                                f"{type(item_error).__name__}: {item_error}"
                            ),
                            delivery_count=deliveries,
                        )
                        _commit_messages(consumer, [msg])
                        fail_counts.pop(event_id, None)
                        logger.error(
                            "Moved poison message event_id=%s to DLQ after %s deliveries",
                            event_id,
                            deliveries,
                        )
                        backend_logship.error(
                            "worker",
                            "Moved poison metrics message to DLQ",
                            event_id=event_id,
                            deliveries=deliveries,
                            max_deliveries=MAX_DELIVERIES,
                            reason=reason,
                        )
                    else:
                        logger.warning(
                            "Leaving message event_id=%s uncommitted (deliveries=%s/%s)",
                            event_id,
                            deliveries,
                            MAX_DELIVERIES,
                        )
                        backend_logship.warn(
                            "worker",
                            "Leaving metrics message uncommitted for retry",
                            event_id=event_id,
                            deliveries=deliveries,
                            max_deliveries=MAX_DELIVERIES,
                        )
        finally:
            db.close()


def run_ingest_worker(
    stop_event: threading.Event,
    consumer_name: str | None = None,
) -> None:
    """
    Poll Kafka ingest topic; commit offsets only after dual-write success.

    Logic:
        1. poll() up to BATCH_SIZE records.
        2. Flush payloads to PostgreSQL + ClickHouse.
        3. On success → commit offsets.
        4. On failure → per-message retry / DLQ.
        5. Exit when stop_event set and idle.
    """
    name = consumer_name or default_consumer_name()
    ensure_clickhouse_schema()
    consumer = get_consumer(group_id=CONSUMER_GROUP)
    producer = get_producer()
    fail_counts: dict[str, int] = defaultdict(int)

    logger.info(
        "Kafka ingest worker starting name=%s group=%s max_deliveries=%s dual_write=pg+ch",
        name,
        CONSUMER_GROUP,
        MAX_DELIVERIES,
    )

    try:
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

            db = SessionLocal()
            try:
                # Per-message consume spans keep parent→child timing correct in Jaeger
                # (each Kafka record may carry a different upstream trace).
                for m in messages:
                    event_id = str(m.value.get("event_id") or "")
                    with kafka_consume_span(
                        m.headers,
                        destination=m.topic,
                        event_id=event_id or None,
                        offset=m.offset,
                        partition=m.partition,
                    ) as span:
                        try:
                            _flush_batch(db, [m.value])
                        except Exception as exc:
                            record_span_error(span, exc)
                            raise
                    if event_id:
                        fail_counts.pop(event_id, None)

                _commit_messages(consumer, messages)
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
    finally:
        consumer.close()
        producer.close()
        close_clickhouse()
        logger.info("Kafka ingest worker stopped name=%s", name)


def main() -> None:
    """Standalone worker entrypoint: python -m backend.worker"""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    ensure_topics()
    # Distinct service.name so Jaeger shows API vs worker as separate nodes.
    setup_tracing(
        service_name=os.getenv("OTEL_SERVICE_NAME", "insightnode-worker")
    )

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
    try:
        run_ingest_worker(stop_event, consumer_name=consumer_name)
    finally:
        shutdown_tracing()


if __name__ == "__main__":
    main()
