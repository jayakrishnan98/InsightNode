"""
Sliding-window rate limiter (Phase 2 Day 6 → Phase 6 Day 3).

Phase 2 keyed by machine_id.
Phase 6 Day 3 keys by tenant_id (SaaS plan ceiling) with optional per-tenant max.

Still in-process (not Redis) — fine for local learning; production shares a store.
"""

from __future__ import annotations

import os
import threading
import time
from collections import defaultdict, deque


# Default max accepted ingest POSTs per tenant in a rolling window
RATE_LIMIT_MAX = int(os.getenv("RATE_LIMIT_MAX", "30"))
RATE_LIMIT_WINDOW_SECONDS = float(os.getenv("RATE_LIMIT_WINDOW_SECONDS", "60"))


class RateLimiter:
    """
    Sliding-window rate limiter keyed by an opaque string (usually tenant_id).

    Logic:
        - Keep timestamps of recent accepts per key.
        - Drop timestamps older than the window.
        - Allow if count < max (per-call override or constructor default).

    Reason:
        Protects Kafka/workers from a runaway customer (all their agents share one
        tenant budget). Per-tenant max teaches plan tiers; Redis would share the
        counter across API replicas.
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

    def allow(self, key: str, *, max_requests: int | None = None) -> bool:
        limit = self.max_requests if max_requests is None else max_requests
        if limit <= 0:
            return False
        now = time.monotonic()
        cutoff = now - self.window_seconds
        with self._lock:
            q = self._hits[key]
            while q and q[0] < cutoff:
                q.popleft()
            if len(q) >= limit:
                return False
            q.append(now)
            return True

    def snapshot(self, key: str, *, max_requests: int | None = None) -> dict:
        limit = self.max_requests if max_requests is None else max_requests
        now = time.monotonic()
        cutoff = now - self.window_seconds
        with self._lock:
            q = self._hits[key]
            while q and q[0] < cutoff:
                q.popleft()
            return {
                "key": key,
                "count": len(q),
                "max": limit,
                "window_seconds": self.window_seconds,
            }


def tenant_rate_key(tenant_id: str) -> str:
    """Stable limiter key for a tenant (avoids colliding with other namespaces)."""
    return f"tenant:{tenant_id}"


ingest_rate_limiter = RateLimiter()
