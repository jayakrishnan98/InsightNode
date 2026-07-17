"""
Redis-backed ingest queue for Phase 2.

Replaces Python queue.Queue so buffered payloads survive API process restarts.
Uses a Redis List: LPUSH (producer) + BRPOP (consumer) = FIFO.
"""

from __future__ import annotations

import json
import os
from typing import Any

from redis import Redis
from redis.exceptions import RedisError

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
INGEST_QUEUE_KEY = "insightnode:ingest"
QUEUE_MAX_LENGTH = 10000


class QueueFullError(Exception):
    """Raised when the Redis list is at QUEUE_MAX_LENGTH (backpressure)."""


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


def enqueue_payload(r: Redis, payload: dict[str, Any]) -> None:
    """
    Push one validated payload onto the ingest list.

    Logic:
        - If LLEN >= QUEUE_MAX_LENGTH → raise QueueFullError (503 path).
        - Else LPUSH JSON string onto INGEST_QUEUE_KEY.

    Reason:
        Same backpressure idea as queue.Queue(maxsize=10000).
        Check-then-push is slightly racy under extreme concurrency;
        good enough for Day 1 learning (Streams/maxlen come later).
    """
    length = r.llen(INGEST_QUEUE_KEY)
    if length >= QUEUE_MAX_LENGTH:
        raise QueueFullError(
            f"Ingest queue full ({length}/{QUEUE_MAX_LENGTH})"
        )

    r.lpush(INGEST_QUEUE_KEY, json.dumps(payload))


def dequeue_payload(
    r: Redis, timeout_seconds: float = 1.0
) -> dict[str, Any] | None:
    """
    Block-pop one payload from the right of the list.

    Logic:
        - BRPOP key timeout → (key, value) or None on timeout.
        - json.loads(value) → dict.
        - Return None if timeout (empty queue).

    Reason:
        BRPOP is the Redis equivalent of queue.get(timeout=...).
        Worker can sleep efficiently without busy-polling.
    """
    # redis-py: timeout is int seconds for BRPOP
    timeout = max(1, int(timeout_seconds))
    result = r.brpop(INGEST_QUEUE_KEY, timeout=timeout)
    if result is None:
        return None

    _key, raw = result
    return json.loads(raw)


def enqueue_payload_retry(r: Redis, payload: dict[str, Any]) -> None:
    """
    Re-queue a failed batch item (worker retry path).

    Logic:
        - Same as enqueue, but if full → raise QueueFullError (caller logs/drops).

    Reason:
        Keep retry path explicit so Day 1 mirrors Phase 1 worker re-queue.
    """
    enqueue_payload(r, payload)


def try_dequeue_nowait(r: Redis) -> dict[str, Any] | None:
    """
    Non-blocking pop from the RIGHT (same end as BRPOP) to keep FIFO.

    Logic:
        - RPOP key → JSON string or None if empty.
        - json.loads when present.

    Reason:
        After BRPOP takes the oldest item, extras must also come from the right.
        LPOP would take newest items and break FIFO batching.
    """
    raw = r.rpop(INGEST_QUEUE_KEY)
    if raw is None:
        return None
    return json.loads(raw)


def queue_length(r: Redis) -> int:
    """Return current list length (for /health)."""
    return int(r.llen(INGEST_QUEUE_KEY))


def ping(r: Redis) -> bool:
    """True if Redis answers PING."""
    try:
        return bool(r.ping())
    except RedisError:
        return False
