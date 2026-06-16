# Request Flows — Phase 1

## 1. Metric ingestion flow (POST /metrics)

This is the **write path** — runs every 5 seconds per agent.

```mermaid
sequenceDiagram
    participant Agent as Telemetry Agent
    participant API as FastAPI
    participant Pydantic as Pydantic Validator
    participant DB as PostgreSQL

    loop Every 5 seconds
        Agent->>Agent: psutil.cpu_percent(interval=5)
        Agent->>Agent: psutil.virtual_memory()
        Agent->>Agent: psutil.disk_usage("/")
        Agent->>Agent: Build JSON payload

        Agent->>API: POST /metrics (application/json)

        API->>Pydantic: Validate MetricsPayload
        alt Invalid payload
            Pydantic-->>API: ValidationError
            API-->>Agent: 422 Unprocessable Entity
            Note over Agent: Log error, continue loop
        else Valid payload
            API->>DB: INSERT 3 rows (cpu, memory, disk)
            DB->>DB: Update indexes
            DB-->>API: commit OK
            API-->>Agent: 201 Created + metric_count
        end
    end
```

### Step-by-step

| Step | Component | What happens |
|------|-----------|--------------|
| 1 | Agent | Block ~5s while sampling CPU |
| 2 | Agent | Read memory and disk (instant) |
| 3 | Agent | Build payload with UTC timestamp |
| 4 | Agent | `httpx.post()` — **waits for response** |
| 5 | API | Pydantic validates schema |
| 6 | API | Create 3 `MetricRecord` ORM objects |
| 7 | API | `db.add_all()` + `db.commit()` — **blocks until disk write** |
| 8 | API | Return `{ status, machine_id, metric_count }` |

### Failure: backend down

```mermaid
sequenceDiagram
    participant Agent as Telemetry Agent
    participant API as FastAPI

    Agent->>API: POST /metrics
    API--xAgent: Connection refused
    Agent->>Agent: print [ERROR]
    Note over Agent: Payload lost — no retry, no buffer
    Agent->>Agent: Wait 5s, collect next payload
```

**Observed behavior:** `[ERROR] Failed to send metrics: [Errno 61] Connection refused`

---

## 2. Metric query flow (GET /metrics)

This is the **read path** — used by curl, Swagger UI, future dashboards.

```mermaid
sequenceDiagram
    participant Client as Client (curl)
    participant API as FastAPI
    participant DB as PostgreSQL

    Client->>API: GET /metrics?machine_id=X&metric_name=cpu_usage&start_time=...&end_time=...

    API->>API: Build SQLAlchemy SELECT with filters
    API->>DB: SELECT ... ORDER BY timestamp ASC LIMIT 100
    DB->>DB: Index scan on (machine_id, metric_name, timestamp)
    DB-->>API: Rows
    API->>API: Map to MetricPoint response models
    API-->>Client: 200 OK { count, metrics[] }
```

### Query parameters

| Parameter | Required | Purpose |
|-----------|----------|---------|
| `machine_id` | No | Filter to one host |
| `metric_name` | No | Filter to one metric (e.g. `cpu_usage`) |
| `start_time` | No | Inclusive lower bound (ISO 8601) |
| `end_time` | No | Inclusive upper bound (ISO 8601) |
| `limit` | No (default 100) | Max rows returned (1–1000) |

### Example requests

```bash
# Latest metrics (up to 100 rows)
curl "http://127.0.0.1:8000/metrics"

# CPU only for one machine
curl "http://127.0.0.1:8000/metrics?machine_id=Jayakrishnans-MacBook-Air.local&metric_name=cpu_usage"

# Time range (URL-encode timestamps with +)
curl -G "http://127.0.0.1:8000/metrics" \
  --data-urlencode "metric_name=cpu_usage" \
  --data-urlencode "start_time=2026-06-14T19:00:00+00:00" \
  --data-urlencode "end_time=2026-06-14T20:00:00+00:00"
```

---

## 3. Health check flow (GET /health)

```mermaid
sequenceDiagram
    participant Client as Client
    participant API as FastAPI

    Client->>API: GET /health
    API-->>Client: 200 { "status": "ok" }
```

**Note:** `/health` does not check PostgreSQL connectivity today. A "deep" health check (`/health/ready`) that pings the DB is a future improvement.

---

## 4. Payload contract (POST /metrics)

### Request body

```json
{
  "machine_id": "Jayakrishnans-MacBook-Air.local",
  "timestamp": "2026-06-14T19:33:54.706344+00:00",
  "metrics": [
    { "name": "cpu_usage",    "value": 37.7, "unit": "percent" },
    { "name": "memory_usage", "value": 79.7, "unit": "percent" },
    { "name": "disk_usage",   "value": 37.7, "unit": "percent" }
  ]
}
```

### Success response (201)

```json
{
  "status": "accepted",
  "machine_id": "Jayakrishnans-MacBook-Air.local",
  "metric_count": 3
}
```

### Validation error (422)

Returned when required fields missing, empty `metrics` array, or invalid types.

---

## 5. Query response contract (GET /metrics)

```json
{
  "count": 2,
  "metrics": [
    {
      "machine_id": "Jayakrishnans-MacBook-Air.local",
      "metric_name": "cpu_usage",
      "value": 17.2,
      "unit": "percent",
      "timestamp": "2026-06-14T19:37:05.020857+00:00"
    }
  ]
}
```

Results ordered **oldest first** (`timestamp ASC`) — suitable for time-series charts left-to-right.
