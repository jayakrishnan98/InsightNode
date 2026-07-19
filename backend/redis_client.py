"""
Redis Streams ingest queue for Phase 2.

XADD (producer) + XREADGROUP / XACK (consumer) so messages are not lost
if the worker crashes between dequeue and PostgreSQL commit.

Day 3: multiple standalone workers share CONSUMER_GROUP with unique names.
Day 4: poison messages move to a Dead Letter Queue (DLQ) after MAX_DELIVERIES.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any

from redis import Redis
from redis.exceptions import RedisError, ResponseError

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
INGEST_STREAM_KEY = "insightnode:ingest"
DLQ_STREAM_KEY = "insightnode:ingest:dlq"
CONSUMER_GROUP = "ingest-workers"
QUEUE_MAX_LENGTH = 10000
MAX_DELIVERIES = int(os.getenv("MAX_DELIVERIES", "5"))


class QueueFullError(Exception):
    """Raised when the stream is at QUEUE_MAX_LENGTH (backpressure)."""


def get_redis() -> Redis:
    """
    Create a Redis client.

    Logic:
        - Connect via REDIS_URL.
        - decode_responses=True so values are str (not bytes).

    Reason:
        One shared client pattern keeps URL/config in one place.
        decode_responses avoids json.loads(bytes) friction.
    """
    return Redis.from_url(REDIS_URL, decode_responses=True)


def ensure_consumer_group(r: Redis) -> None:
    """
    Create the consumer group if it does not exist.

    Logic:
        - XGROUP CREATE stream group 0 MKSTREAM
        - Ignore BUSYGROUP if it already exists.

    Reason:
        Workers need a group before XREADGROUP. Creating at API startup
        keeps first-boot simple.
    """
    try:
        r.xgroup_create(
            name=INGEST_STREAM_KEY,
            groupname=CONSUMER_GROUP,
            id="0",
            mkstream=True,
        )
    except ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise


def enqueue_payload(r: Redis, payload: dict[str, Any]) -> str:
    """
    Append one payload to the stream.

    Logic:
        - If XLEN >= QUEUE_MAX_LENGTH → raise QueueFullError.
        - XADD stream * payload <json> (auto-generated ID).
        - Return the stream message ID.

    Reason:
        Approximate maxlen backpressure (same idea as List LLEN).
        Messages stay until consumers XACK after successful DB writes.
    """
    length = r.xlen(INGEST_STREAM_KEY)
    if length >= QUEUE_MAX_LENGTH:
        raise QueueFullError(
            f"Ingest stream full ({length}/{QUEUE_MAX_LENGTH})"
        )

    message_id = r.xadd(
        INGEST_STREAM_KEY,
        {"payload": json.dumps(payload)},
    )
    return message_id


def read_batch(
    r: Redis,
    consumer_name: str,
    count: int = 50,
    block_ms: int = 1000,
) -> list[tuple[str, dict[str, Any]]]:
    """
    Read up to `count` new messages for this consumer.

    Logic:
        - XREADGROUP GROUP ingest-workers <consumer> COUNT count BLOCK block_ms
          STREAMS insightnode:ingest >
        - Parse each entry's payload JSON.
        - Return list of (message_id, payload_dict).

    Reason:
        '>' means only never-delivered messages.
        BLOCK is the Stream equivalent of BRPOP timeout.
    """
    result = r.xreadgroup(
        groupname=CONSUMER_GROUP,
        consumername=consumer_name,
        streams={INGEST_STREAM_KEY: ">"},
        count=count,
        block=block_ms,
    )
    if not result:
        return []

    messages: list[tuple[str, dict[str, Any]]] = []
    for _stream_name, entries in result:
        for message_id, fields in entries:
            payload = json.loads(fields["payload"])
            messages.append((message_id, payload))
    return messages


def ack_messages(r: Redis, message_ids: list[str]) -> None:
    """
    Acknowledge successfully processed messages and remove them from the stream.

    Logic:
        - XACK stream group id1 id2 ...
        - XDEL the same IDs so XLEN reflects real backlog.

    Reason:
        XACK only clears the PEL; entries remain in the stream by default.
        Deleting after ACK keeps queue_size / backpressure meaningful.
    """
    if not message_ids:
        return
    r.xack(INGEST_STREAM_KEY, CONSUMER_GROUP, *message_ids)
    r.xdel(INGEST_STREAM_KEY, *message_ids)


def claim_stale_messages(
    r: Redis,
    consumer_name: str,
    min_idle_ms: int = 60_000,
    count: int = 50,
) -> list[tuple[str, dict[str, Any]]]:
    """
    Reclaim messages stuck in PEL (worker died after read, before ACK).

    Logic:
        - XAUTOCLAIM stream group consumer min_idle start 0-0 COUNT count
        - Return (message_id, payload) list.

    Reason:
        Day 2 win over Lists — crashed in-flight work is recoverable.
    """
    try:
        _next_id, messages, *_rest = r.xautoclaim(
            name=INGEST_STREAM_KEY,
            groupname=CONSUMER_GROUP,
            consumername=consumer_name,
            min_idle_time=min_idle_ms,
            start_id="0-0",
            count=count,
        )
    except ResponseError:
        # Group/stream not ready yet
        return []

    claimed: list[tuple[str, dict[str, Any]]] = []
    for message_id, fields in messages:
        if not fields:
            continue
        payload = json.loads(fields["payload"])
        claimed.append((message_id, payload))
    return claimed


def get_delivery_count(r: Redis, message_id: str) -> int:
    """
    Return how many times this message was delivered to the consumer group.

    Logic:
        - XPENDING range for a single message ID.
        - Read times_delivered from the entry.

    Reason:
        Used to decide when a poison message should move to the DLQ instead of
        being retried forever.
    """
    try:
        entries = r.xpending_range(
            name=INGEST_STREAM_KEY,
            groupname=CONSUMER_GROUP,
            min=message_id,
            max=message_id,
            count=1,
        )
    except ResponseError:
        return 0

    if not entries:
        return 0

    entry = entries[0]
    if isinstance(entry, dict):
        return int(entry.get("times_delivered", 0))
    # Fallback tuple shape: (message_id, consumer, idle_ms, deliveries)
    return int(entry[3])


def move_to_dlq(
    r: Redis,
    message_id: str,
    payload: dict[str, Any],
    reason: str,
    delivery_count: int,
) -> str:
    """
    Move a poison message from the ingest stream to the dead-letter stream.

    Logic:
        - XADD to DLQ with payload, reason, original_id, delivery_count, timestamp.
        - XACK + XDEL the original message so it stops being retried.

    Reason:
        Infinite retries waste workers and hide bad data. DLQ preserves the
        message for inspection without blocking the main pipeline.
    """
    dlq_id = r.xadd(
        DLQ_STREAM_KEY,
        {
            "payload": json.dumps(payload),
            "reason": reason,
            "original_id": message_id,
            "delivery_count": str(delivery_count),
            "failed_at": datetime.now(timezone.utc).isoformat(),
        },
    )
    ack_messages(r, [message_id])
    return dlq_id


def queue_length(r: Redis) -> int:
    """Approximate backlog = stream length."""
    return int(r.xlen(INGEST_STREAM_KEY))


def pending_count(r: Redis) -> int:
    """How many messages are delivered but not ACK'd."""
    try:
        info = r.xpending(INGEST_STREAM_KEY, CONSUMER_GROUP)
        return int(info.get("pending", 0))
    except ResponseError:
        return 0


def dlq_length(r: Redis) -> int:
    """Number of messages parked in the dead-letter stream."""
    return int(r.xlen(DLQ_STREAM_KEY))


def peek_dlq(r: Redis, count: int = 20) -> list[dict[str, Any]]:
    """
    Read recent DLQ entries for debugging (does not consume/delete them).

    Logic:
        - XREVRANGE dlq + - COUNT count (newest first).
        - Parse payload JSON and return structured dicts.
    """
    entries = r.xrevrange(DLQ_STREAM_KEY, max="+", min="-", count=count)
    results: list[dict[str, Any]] = []
    for message_id, fields in entries:
        payload_raw = fields.get("payload", "{}")
        try:
            payload = json.loads(payload_raw)
        except json.JSONDecodeError:
            payload = {"raw": payload_raw}
        results.append(
            {
                "dlq_id": message_id,
                "original_id": fields.get("original_id"),
                "reason": fields.get("reason"),
                "delivery_count": int(fields.get("delivery_count", 0)),
                "failed_at": fields.get("failed_at"),
                "payload": payload,
            }
        )
    return results


def ping(r: Redis) -> bool:
    """True if Redis answers PING."""
    try:
        return bool(r.ping())
    except RedisError:
        return False
