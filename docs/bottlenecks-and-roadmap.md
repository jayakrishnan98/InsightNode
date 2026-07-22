# Bottlenecks & Roadmap

Phase 1 capstone analysis — recorded **2026-07-10** on a single MacBook agent + local PostgreSQL.

---

## Scale math

```
Collection interval:  5 seconds
Metrics per payload:  3 (cpu, memory, disk)

Rows per agent per hour = (3600 / 5) × 3 = 2,160
```

| Agents | Rows/hour | Rows/day |
|--------|-----------|----------|
| 1 | 2,160 | ~51,840 |
| 100 | 216,000 | ~5.2M |
| 1,000 | 2,160,000 | ~51.8M |

---

## What breaks first?

At scale, failures appear in this **approximate order**:

| Order | Component | Symptom |
|-------|-----------|---------|
| 1 | **In-memory queue** | API crash → queued metrics lost; queue fills under write pressure |
| 2 | **Single API process** | CPU/memory ceiling; cannot horizontally scale ingest |
| 3 | **PostgreSQL write throughput** | Worker commits lag; queue grows; 503 responses |
| 4 | **PostgreSQL read/aggregate** | `GROUP BY` over millions of rows becomes slow |
| 5 | **Disk** | Table bloat; index size; backup time |

**At 1,000 agents:** PostgreSQL write volume (~2.16M rows/hour) and the in-memory queue become the primary bottlenecks before the agent itself fails.

---

## Failure scenarios

| Scenario | What happens | Data lost? |
|----------|--------------|------------|
| Agent crash | Stops collecting; spool file survives on disk | No (if spooled) |
| API crash | In-memory queue lost | **Yes** — queued-not-yet-persisted metrics |
| PostgreSQL down | Worker re-queues; queue grows; 503 if full | Possible after max retries |
| Network blip | Agent retries; may spool | No (with spool) |
| Spool replay | May re-send same payload | No DB duplicates (Day 11 dedup) |

---

## Capstone experiment results (2026-07-10)

### Experiment 1 — End-to-end health

```bash
curl http://127.0.0.1:8001/health
```

**Result:**
```json
{"status":"ok","queue_size":0,"queue_maxsize":10000,"worker_alive":true}
```

Aggregate query (`interval=5m`, 90-minute window):

| Observation | Value |
|-------------|-------|
| Buckets returned | 13 |
| Typical `sample_count` | ~60 per 5m bucket (matches 5s interval) |
| Partial bucket | 22 samples (collection started mid-bucket) |
| `min <= avg <= max` | Verified in all buckets |

**Conclusion:** Pipeline healthy; aggregation semantics correct.

---

### Experiment 2 — Dedup proof

Posted identical payload twice with `event_id=aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee`.

```sql
SELECT COUNT(*) FROM metrics
WHERE event_id = 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee';
-- Result: 1
```

**Conclusion:** Day 11 idempotency working.

---

### Experiment 3 — Backpressure (analysis)

Not run with artificial worker delay (would disrupt live agent). Observed behavior:

- `queue_size` stays at 0–1 under normal load (1 agent, default batch settings)
- `block=False` + `maxsize=10000` → API returns 503 when queue full
- With slow worker or many agents, `queue_size` in `/health` is the early warning signal

**Manual test (optional):** Add `time.sleep(5)` to `_flush_batch`, watch `queue_size` climb.

---

### Experiment 4 — Current database scale

| Metric | Value |
|--------|-------|
| Total rows | 13,788 |
| Rows with event_id | 2,025 |
| Table size | 4,064 kB |

At 1,000 agents (51.8M rows/day), this table would grow to terabytes within weeks without retention or a specialized TSDB.

---

## Known limitations (Phase 1 — intentional)

| Limitation | Motivates |
|------------|-----------|
| In-memory queue | Phase 2 — Redis/Kafka |
| PostgreSQL for time-series | Phase 3 — ClickHouse |
| Query-time aggregation only | Phase 3 — rollups / columnar DB |
| Single API process | Phase 2+ — horizontal scaling |
| Agent retry storm on long outages | Phase 2 — durable buffer + smarter backoff |
| No retention policy | Future — data lifecycle management |
| No percentiles (p95) | Future — histogram/percentile support |

---

## Roadmap

### Phase 2 (Day 13+) — Durable buffering

- **Redis** replaces in-memory queue
- Survives API restarts
- Foundation for multiple consumers

### Phase 3 — Time-series storage ✅

- **Day 1:** ClickHouse up (Docker, MergeTree schema, health ping)
- **Day 2:** Dual-write from Kafka workers (PostgreSQL + ClickHouse)
- **Day 3:** Route `/metrics/aggregate` to ClickHouse
- **Day 4:** Compare PostgreSQL vs columnar at scale
- **Day 5:** Docs + graduation

See [phase-3-graduation.md](phase-3-graduation.md).

### Phase 4 — Logs

- **OpenSearch** for centralized log search

### Phase 5 — Traces

- **OpenTelemetry** + Jaeger concepts

### Phase 6 — Production SaaS concerns

- Sharding, multi-tenancy, rate limiting, usage metering
