"""
Redis Streams ingest queue for Phase 2 Day 2.

XADD (producer) + XREADGROUP / XACK (consumer) so messages are not lost
if the worker crashes between dequeue and PostgreSQL commit.
"""

from __future__ import annotations

import json
import os
from typing import Any

from redis import Redis
from redis.exceptions import RedisError, ResponseError

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
INGEST_STREAM_KEY = "insightnode:ingest"
CONSUMER_GROUP = "ingest-workers"
QUEUE_MAX_LENGTH = 10000


class QueueFullError(Exception):
    """Raised when the stream is at QUEUE_MAX_LENGTH (backpressure)."""


def get_redis() -> Redis:
    return Redis.from_url(REDIS_URL, decode_responses=True)


def ensure_consumer_group(r: Redis) -> None:
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
    length = r.xlen(INGEST_STREAM_KEY)
    if length >= QUEUE_MAX_LENGTH:
        raise QueueFullError(
            f"Ingest stream full ({length}/{QUEUE_MAX_LENGTH})"
        )
    return r.xadd(INGEST_STREAM_KEY, {"payload": json.dumps(payload)})


def read_batch(
    r: Redis,
    consumer_name: str,
    count: int = 50,
    block_ms: int = 1000,
) -> list[tuple[str, dict[str, Any]]]:
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
            messages.append((message_id, json.loads(fields["payload"])))
    return messages


def ack_messages(r: Redis, message_ids: list[str]) -> None:
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
        return []
    claimed: list[tuple[str, dict[str, Any]]] = []
    for message_id, fields in messages:
        if not fields:
            continue
        claimed.append((message_id, json.loads(fields["payload"])))
    return claimed


def queue_length(r: Redis) -> int:
    return int(r.xlen(INGEST_STREAM_KEY))


def pending_count(r: Redis) -> int:
    try:
        info = r.xpending(INGEST_STREAM_KEY, CONSUMER_GROUP)
        return int(info.get("pending", 0))
    except ResponseError:
        return 0


def ping(r: Redis) -> bool:
    try:
        return bool(r.ping())
    except RedisError:
        return False
