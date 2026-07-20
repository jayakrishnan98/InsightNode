"""
Simple in-memory per-machine rate limiter (Phase 2 Day 6).

Teaches edge backpressure before multi-tenant metering (Phase 6).
Not distributed — each API process has its own counters (fine for local learning).
"""

from __future__ import annotations

import os
import threading
import time
from collections import defaultdict, deque


# Max accepted POSTs per machine_id in a rolling window
RATE_LIMIT_MAX = int(os.getenv("RATE_LIMIT_MAX", "30"))
RATE_LIMIT_WINDOW_SECONDS = float(os.getenv("RATE_LIMIT_WINDOW_SECONDS", "60"))


class RateLimiter:
    """
    Sliding-window rate limiter keyed by machine_id.

    Logic:
        - Keep timestamps of recent accepts per key.
        - Drop timestamps older than the window.
        - Allow if count < max; else reject.

    Reason:
        Protects Kafka/workers from a misbehaving or looping agent.
        Real SaaS uses Redis/token buckets per tenant — same idea, shared store.
    """

    def __init__(
        self,
        max_requests: int = RATE_LIMIT_MAX,
        window_seconds: float = RATE_LIMIT_WINDOW_SECONDS,
    ) -> None:
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self._hits: dict[str, deque[float]] = defaultdict(deque)
        self._lock = threading.Lock()

    def allow(self, key: str) -> bool:
        now = time.monotonic()
        cutoff = now - self.window_seconds
        with self._lock:
            q = self._hits[key]
            while q and q[0] < cutoff:
                q.popleft()
            if len(q) >= self.max_requests:
                return False
            q.append(now)
            return True

    def snapshot(self, key: str) -> dict:
        now = time.monotonic()
        cutoff = now - self.window_seconds
        with self._lock:
            q = self._hits[key]
            while q and q[0] < cutoff:
                q.popleft()
            return {
                "key": key,
                "count": len(q),
                "max": self.max_requests,
                "window_seconds": self.window_seconds,
            }


ingest_rate_limiter = RateLimiter()
