# Request Flows (Phase 1)

Five flows that cover normal operation and failure modes in InsightNode.

---

## 1. Happy path — ingest

```mermaid
sequenceDiagram
    participant A as Agent
    participant API as POST /metrics
    participant Q as Queue
    participant W as Worker
    participant DB as PostgreSQL

    A->>A: collect_metrics() + event_id
    A->>API: POST JSON
    API->>API: Pydantic validate
    API->>Q: put (non-blocking)
    API-->>A: 202 Accepted
    W->>Q: dequeue batch (up to 50)
    W->>DB: INSERT ... ON CONFLICT DO NOTHING
```

| Step | Failure | Effect |
|------|---------|--------|
| POST | Network error | Agent retries, then spools |
| Enqueue | Queue full | API returns 503; agent retries/spools |
| Worker INSERT | DB down | Batch re-queued (max 3 attempts) |

---

## 2. API unreachable — agent spool

```mermaid
sequenceDiagram
    participant A as Agent
    participant S as spool.ndjson
    participant API as POST /metrics

    A->>API: POST (fails)
    A->>A: retry 5x with backoff
    A->>S: append payload (keeps event_id)
    Note over A,S: API comes back
    A->>S: read_all()
    A->>API: replay payloads (FIFO)
    A->>S: rewrite (remove sent)
```

**Key:** `event_id` in the spooled payload ensures replay does not create duplicate DB rows.

---

## 3. Queue full — backpressure

```mermaid
sequenceDiagram
    participant A as Agent
    participant API as POST /metrics
    participant Q as Queue

    A->>API: POST
    API->>Q: put (block=False)
    Q-->>API: Full
    API-->>A: 503 Service Unavailable
    A->>A: retry or spool to disk
```

**Why `block=False`:** HTTP threads must not hang indefinitely. Backpressure is pushed to the agent.

---

## 4. PostgreSQL down — worker retry

```mermaid
sequenceDiagram
    participant Q as Queue
    participant W as Worker
    participant DB as PostgreSQL

    W->>Q: dequeue batch
    W->>DB: INSERT (fails)
    W->>W: rollback
    W->>Q: re-queue payloads (_retry_count++)
    Note over W: After 3 failures → drop + log
```

---

## 5. Duplicate POST — idempotent storage

```mermaid
sequenceDiagram
    participant C as Client
    participant API as POST /metrics
    participant W as Worker
    participant DB as PostgreSQL

    C->>API: POST (event_id=X)
    API-->>C: 202
    W->>DB: INSERT → 1 row
    C->>API: POST (same event_id=X)
    API-->>C: 202
    W->>DB: INSERT ON CONFLICT DO NOTHING → 0 new rows
```

**Capstone result (2026-07-10):** Two identical POSTs with `event_id=aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee` → `COUNT(*) = 1` in database.

---

## Query flows

### Raw query — `GET /metrics`

Filter by `machine_id`, `metric_name`, `start_time`, `end_time`, `limit`. Returns individual samples.

### Aggregate query — `GET /metrics/aggregate`

Requires `machine_id`, `metric_name`, `start_time`, `end_time`. Groups by `date_bin(interval, timestamp)` and returns avg, min, max, sample_count per bucket.
