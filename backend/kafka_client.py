"""
Kafka ingest bus for Phase 2 Day 5.

Replaces Redis Streams as the primary ingest pipeline:
  API  --produce-->  insightnode.ingest  --consumer group-->  worker  --> PostgreSQL
                                         └─ poison --> insightnode.ingest.dlq

Why Kafka after Redis Streams:
  - Partitioned log for ordered, scalable consume
  - Durable retention / replay beyond "queue length"
  - Same consumer-group idea you learned with Redis, at larger scale
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any

from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
INGEST_TOPIC = os.getenv("KAFKA_INGEST_TOPIC", "insightnode.ingest")
DLQ_TOPIC = os.getenv("KAFKA_DLQ_TOPIC", "insightnode.ingest.dlq")
CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "insightnode-ingest-workers")
QUEUE_MAX_LENGTH = int(os.getenv("KAFKA_QUEUE_MAX_LENGTH", "10000"))
MAX_DELIVERIES = int(os.getenv("MAX_DELIVERIES", "5"))


class QueueFullError(Exception):
    """Raised when lag exceeds QUEUE_MAX_LENGTH (soft backpressure)."""


def ensure_topics() -> None:
    """
    Create ingest + DLQ topics if missing.

    Logic:
        - List existing topics; create only missing ones (3 partitions / RF 1).
        - Ignore TopicAlreadyExistsError if another process races us.

    Reason:
        Partitions enable parallel consumers in one group. Local Redpanda/Kafka
        uses replication factor 1.
    """
    admin = KafkaAdminClient(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        client_id="insightnode-admin",
    )
    topics = [
        NewTopic(name=INGEST_TOPIC, num_partitions=3, replication_factor=1),
        NewTopic(name=DLQ_TOPIC, num_partitions=1, replication_factor=1),
    ]
    try:
        existing = set(admin.list_topics())
        to_create = [t for t in topics if t.name not in existing]
        if not to_create:
            logger.info("Kafka topics already exist")
            return

        admin.create_topics(new_topics=to_create, validate_only=False)
        for t in to_create:
            logger.info("Created Kafka topic: %s", t.name)
    except TopicAlreadyExistsError:
        # Race: another process created the topic between list and create
        pass
    finally:
        admin.close()


def get_producer() -> KafkaProducer:
    """
    Shared Kafka producer for the API (Phase 2 Day 6).

    Logic:
        - JSON serializers; key = machine_id for per-host partition affinity.
        - acks=all + retries; max_in_flight=1 so retries don't reorder.
        - linger_ms batches tiny produces without hurting agent latency much.

    Reason:
        kafka-python-ng has no enable_idempotence; acks=all + single in-flight
        request is the practical equivalent for safe local retries. Complements
        event_id at the DB layer for end-to-end dedupe.
    """
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        acks="all",
        retries=5,
        max_in_flight_requests_per_connection=1,
        linger_ms=5,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k is not None else None,
    )


def get_consumer(group_id: str | None = None) -> KafkaConsumer:
    """
    Worker consumer — manual commit after successful DB write.

    Logic:
        - enable_auto_commit=False (commit only after PostgreSQL success).
        - max_poll_records limits batch size.

    Reason:
        Same ACK-after-commit lesson as Redis XACK (Day 2), on Kafka offsets.
    """
    return KafkaConsumer(
        INGEST_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=group_id or CONSUMER_GROUP,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        max_poll_records=50,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        consumer_timeout_ms=1000,
    )


def enqueue_payload(producer: KafkaProducer, payload: dict[str, Any]) -> None:
    """
    Publish one metrics payload to the ingest topic.

    Logic:
        - Soft backpressure: if consumer lag estimate >= max → QueueFullError.
        - Start a PRODUCER span, inject W3C trace context into Kafka headers,
          then produce(key=machine_id, value=payload, headers=...) and flush.

    Reason:
        Agents get 503 when workers fall far behind — same idea as Redis XLEN cap.
        Headers carry the active HTTP/request span so the worker continues the
        same trace_id (Phase 5 Day 3).
    """
    from backend.tracing import kafka_headers_from_context, kafka_produce_span

    lag = approximate_lag()
    if lag >= QUEUE_MAX_LENGTH:
        raise QueueFullError(
            f"Ingest topic lag too high ({lag}/{QUEUE_MAX_LENGTH})"
        )

    key = payload.get("machine_id")
    with kafka_produce_span(INGEST_TOPIC) as span:
        event_id = payload.get("event_id")
        if event_id:
            span.set_attribute("insightnode.event_id", str(event_id))
        headers = kafka_headers_from_context()
        future = producer.send(
            INGEST_TOPIC,
            key=key,
            value=payload,
            headers=headers,
        )
        future.get(timeout=10)


def publish_dlq(
    producer: KafkaProducer,
    payload: dict[str, Any],
    reason: str,
    delivery_count: int,
) -> None:
    """
    Publish a poison payload to the DLQ topic.

    Logic:
        - Wrap original payload with reason / delivery_count / failed_at.
        - Produce to DLQ_TOPIC and flush.
    """
    envelope = {
        "payload": payload,
        "reason": reason,
        "delivery_count": delivery_count,
        "failed_at": datetime.now(timezone.utc).isoformat(),
    }
    future = producer.send(DLQ_TOPIC, value=envelope)
    future.get(timeout=10)


def approximate_lag() -> int:
    """Total consumer-group lag across all ingest partitions."""
    details = partition_lag_details()
    return int(sum(p["lag"] for p in details.get("partitions", [])))


def partition_lag_details() -> dict[str, Any]:
    """
    Per-partition lag for the ingest consumer group (Phase 2 Day 6).

    Logic:
        - For each partition: beginning, committed, end offsets.
        - lag = end - committed (or end - beginning if never committed).

    Reason:
        Total lag hides hot partitions. Ops need per-partition views when
        one machine_id key hashes to a busy partition.
    """
    try:
        from kafka import TopicPartition

        consumer = KafkaConsumer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            group_id=CONSUMER_GROUP,
            enable_auto_commit=False,
        )
        partitions = consumer.partitions_for_topic(INGEST_TOPIC) or set()
        if not partitions:
            consumer.close()
            return {
                "topic": INGEST_TOPIC,
                "group": CONSUMER_GROUP,
                "total_lag": 0,
                "partitions": [],
            }

        tps = [TopicPartition(INGEST_TOPIC, p) for p in sorted(partitions)]
        consumer.assign(tps)
        beginning = consumer.beginning_offsets(tps)
        end_offsets = consumer.end_offsets(tps)

        rows: list[dict[str, Any]] = []
        total = 0
        for tp in tps:
            committed = consumer.committed(tp)
            begin = beginning.get(tp, 0)
            end = end_offsets.get(tp, 0)
            if committed is None:
                lag = max(0, end - begin)
                committed_offset = None
            else:
                lag = max(0, end - committed)
                committed_offset = committed
            total += lag
            rows.append(
                {
                    "partition": tp.partition,
                    "beginning": begin,
                    "committed": committed_offset,
                    "end": end,
                    "lag": lag,
                }
            )
        consumer.close()
        return {
            "topic": INGEST_TOPIC,
            "group": CONSUMER_GROUP,
            "total_lag": total,
            "partitions": rows,
        }
    except Exception:
        logger.exception("Failed to compute partition lag details")
        return {
            "topic": INGEST_TOPIC,
            "group": CONSUMER_GROUP,
            "total_lag": 0,
            "partitions": [],
            "error": "unavailable",
        }

def dlq_size_estimate() -> int:
    """Estimate messages currently in the DLQ topic (end - beginning)."""
    try:
        consumer = KafkaConsumer(bootstrap_servers=KAFKA_BOOTSTRAP)
        parts = consumer.partitions_for_topic(DLQ_TOPIC) or set()
        if not parts:
            consumer.close()
            return 0
        from kafka import TopicPartition

        tps = [TopicPartition(DLQ_TOPIC, p) for p in parts]
        beginning = consumer.beginning_offsets(tps)
        end = consumer.end_offsets(tps)
        total = sum(end[tp] - beginning[tp] for tp in tps)
        consumer.close()
        return int(total)
    except Exception:
        return 0


def peek_dlq(limit: int = 20) -> list[dict[str, Any]]:
    """
    Read up to `limit` recent DLQ messages (best-effort, newest-ish).

    Logic:
        - Assign DLQ partitions, seek near end, poll.
    """
    try:
        from kafka import TopicPartition

        consumer = KafkaConsumer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            enable_auto_commit=False,
            value_deserializer=lambda b: json.loads(b.decode("utf-8")),
            consumer_timeout_ms=2000,
        )
        parts = consumer.partitions_for_topic(DLQ_TOPIC) or set()
        if not parts:
            consumer.close()
            return []

        tps = [TopicPartition(DLQ_TOPIC, p) for p in parts]
        consumer.assign(tps)
        end = consumer.end_offsets(tps)
        for tp in tps:
            start = max(end[tp] - limit, 0)
            consumer.seek(tp, start)

        entries: list[dict[str, Any]] = []
        for msg in consumer:
            value = msg.value if isinstance(msg.value, dict) else {"raw": msg.value}
            entries.append(
                {
                    "partition": msg.partition,
                    "offset": msg.offset,
                    "reason": value.get("reason"),
                    "delivery_count": value.get("delivery_count"),
                    "failed_at": value.get("failed_at"),
                    "payload": value.get("payload", value),
                }
            )
            if len(entries) >= limit:
                break
        consumer.close()
        return list(reversed(entries[-limit:]))
    except Exception:
        logger.exception("Failed to peek DLQ")
        return []


def ping() -> bool:
    """True if Kafka brokers are reachable."""
    try:
        admin = KafkaAdminClient(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            client_id="insightnode-ping",
            request_timeout_ms=3000,
        )
        admin.list_topics()
        admin.close()
        return True
    except Exception:
        return False
