# Phase 6 Graduation Checklist

**Completed:** Phase 6 Day 5  
**Next:** Open-ended — deepen any pillar, or experiment with real multi-cell sharding

---

## Identity (Day 1)

- [x] `tenants` registry in PostgreSQL (+ seed `local` / `dev-local-key`)
- [x] `X-API-Key` → `TenantContext` (`require_tenant`)
- [x] Soft vs strict mode (`TENANCY_STRICT`)
- [x] `GET /tenants` (masked keys)

## Storage isolation (Day 2)

- [x] `tenant_id` on PostgreSQL `metrics` (+ per-tenant dedup index)
- [x] `tenant_id` on ClickHouse metrics + filtered aggregates
- [x] OpenSearch top-level `tenant_id`; search/get scoped
- [x] Worker dual-write persists `tenant_id` from Kafka payloads

## Rate limits (Day 3)

- [x] Ingest limiter keyed by `tenant:{id}` (metrics + logs share budget)
- [x] Optional `tenants.rate_limit_max` override
- [x] **429** + `Retry-After` / `X-RateLimit-*` headers

## Metering & quotas (Day 4)

- [x] `tenant_usage` monthly counters (UTC)
- [x] Check before accept; record after success
- [x] **402** when plan quota exceeded (distinct from 429)
- [x] `GET /usage` shows usage / quotas / remaining

## Sharding concepts (Day 5)

- [x] `backend/sharding.py` — stable `shard_id = crc32(tenant_id) % N`
- [x] Kafka produce key prefers `tenant_id` (partition affinity)
- [x] `/usage` includes `sharding` block
- [x] Docs explain logical vs physical sharding

## Understanding (explain without notes)

- [x] Why SaaS identity is **tenant**, not host
- [x] Why stamping `tenant_id` without query filters is not isolation
- [x] Difference between **rate limit** (burst) and **quota** (billable month)
- [x] Why 429 vs 402 communicate different operator actions
- [x] Why Kafka keys and CH `ORDER BY` should lead with tenant for affinity
- [x] Why a local `shard_id` is still useful before multi-DB routing exists

## Docs

- [x] docs/phase-6-architecture.md
- [x] docs/phase-6-graduation.md (this file)
- [x] README reflects Phase 6 complete

---

## Phase 6 complete

You can now run InsightNode as a **learning multi-tenant SaaS**: authenticate by API key, isolate data, throttle bursts, meter monthly usage, and reason about tenant-based sharding — on top of the metrics / logs / traces stack from Phases 1–5.
