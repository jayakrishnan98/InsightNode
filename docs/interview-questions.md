# Interview questions — InsightNode

Use these prompts in system-design interviews. Prefer **measured lab numbers** over invented production claims.

---

## Storage & evolution

1. **Why PostgreSQL initially?** Relational familiarity, ACID, easy local setup, good enough for low agent counts and raw point queries.
2. **What limitations appeared?** Write amplification, expensive long-range `GROUP BY`, retention/backup pain as rows grow.
3. **Why ClickHouse for metrics?** Columnar scans, time partitions, cheap aggregates for dashboards; dual-write keeps PG as idempotent source of truth.
4. **Why OpenSearch for logs?** Full-text + keyword filters; different access pattern than gauges.
5. **Why not one database for everything?** Metrics, logs, and relational metadata have different query shapes and SLAs.

## Ingest & reliability

6. **Where does backpressure occur?** Kafka lag ≥ `KAFKA_QUEUE_MAX_LENGTH` → API **503**; agents spool to disk.
7. **What if ClickHouse is unavailable?** Worker fails dual-write, does not commit offsets → lag grows; PG may already have rows (idempotent on retry).
8. **How are duplicates handled?** Agent `event_id` + PG unique index; CH may duplicate under rare redelivery (accepted for learning).
9. **How are late metrics handled?** Agent `timestamp` is observation time; `created_at` is ingest time — charts use observation time.
10. **What happens if the API restarts?** Kafka retains unconsumed messages; in-memory rate limiter resets (lab limitation).

## Visualization & product

11. **Why Grafana instead of building every chart?** Faster operational UX, variables, provisioning, alerting; portfolio story of buying vs building.
12. **What belongs in the custom UI?** Host inventory, onboarding, simplified log search, alert *events* — not a dashboard builder.
13. **Why `/prometheus` not `/metrics`?** `/metrics` is already the telemetry ingest/query API.
14. **Why Grafana-managed alerts first?** Demonstrates alerting without owning a rule engine; webhook stores events for the product UI.

## Tenancy & scale

15. **How would the system scale to more agents?** More API replicas, more Kafka partitions/workers, CH retention, optional drop of dual-write hot path.
16. **How would multi-tenancy go to production?** Hashed API keys, strict mode, per-tenant indexes/cells, separate Grafana orgs or row-level security.
17. **What is logical `shard_id` for?** Teaching affinity before multi-cell routing exists.
18. **What would change for production?** TLS, secrets manager, authn/z, backups, retention, CH dedup engine, managed services, no open OpenSearch.

## Observability of the platform

19. **How do you observe the observer?** Prometheus for process metrics; Grafana Platform dashboard; Jaeger for request traces; `/pipeline` / DLQ for Kafka.
20. **What is a realistic portfolio load target?** Tens of simulated hosts for tens of seconds — document measured RPS/latency; do not claim millions of agents without evidence.
