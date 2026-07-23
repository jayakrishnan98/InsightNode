"""
Tenant sharding helpers (Phase 6 Day 5).

InsightNode still runs on a single PG / CH / OpenSearch locally. This module
teaches the *design* of tenant-based sharding: a stable hash maps tenant_id →
shard id so a future multi-node deploy can route without rewriting queries.
"""

from __future__ import annotations

import os
import zlib

# How many logical shards the learning model assumes (not physical DBs yet).
NUM_SHARDS = int(os.getenv("INSIGHTNODE_NUM_SHARDS", "4"))


def shard_for_tenant(tenant_id: str, *, num_shards: int | None = None) -> int:
    """
    Deterministic shard index for a tenant (Phase 6 Day 5).

    Logic:
        - CRC32(tenant_id) % N → shard in [0, N).
        - Same tenant always lands on the same shard (stable under restarts).

    Reason:
        SaaS scale-out almost always shards by tenant (or org) so one customer's
        queries stay on one cell. Hashing avoids a central assignment table for
        the common case; sticky assignment tables come later for large tenants.
    """
    n = NUM_SHARDS if num_shards is None else num_shards
    if n < 1:
        raise ValueError("num_shards must be >= 1")
    return zlib.crc32(tenant_id.encode("utf-8")) % n


def shard_info(tenant_id: str) -> dict:
    """Snapshot for /usage and docs — logical shard only."""
    shard = shard_for_tenant(tenant_id)
    return {
        "tenant_id": tenant_id,
        "shard_id": shard,
        "num_shards": NUM_SHARDS,
        "kafka_partition_key": "tenant_id",
        "note": (
            "Logical shard for learning — local stack is still single-node. "
            "Kafka produce keys by tenant_id for partition affinity."
        ),
    }
