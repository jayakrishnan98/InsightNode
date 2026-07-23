# Phase 7 Graduation Checklist

**Completed:** Phase 7 (visualization & product UI)  
**Next:** Open-ended — retention, percentiles, multi-cell routing, managed cloud DBs, custom alert-rule evaluator if product UX requires it

---

## Grafana (Days 1–2)

- [x] Grafana service in Docker Compose with persistent volume
- [x] ClickHouse datasource provisioned (env-based credentials)
- [x] Optional PostgreSQL datasource provisioned
- [x] Dashboard provider mounted from `infrastructure/grafana/`
- [x] Infrastructure Monitoring dashboard (CPU / memory / disk, fleet, ingest rate, variables)

## Platform observability (Day 3)

- [x] Prometheus in Compose scraping API + worker via `host.docker.internal`
- [x] `GET /prometheus` on API (path chosen so `/metrics` stays ingest)
- [x] Worker metrics HTTP server on `WORKER_METRICS_PORT` (default 8002)
- [x] Platform Observability dashboard (HTTP rate/latency, ingest, dual-write, lag)

## Alerting (Day 4)

- [x] `alert_events` table (migration + ensure on boot)
- [x] Grafana contact point → `POST /alert-events/webhook` with Bearer secret
- [x] Provisioned rules: high CPU / memory / disk + host not reporting
- [x] Firing insert + resolve update + dedupe on `(fingerprint, starts_at)`
- [x] `GET /alert-events` for verification / UI

## Product APIs (Day 5)

- [x] `GET /hosts` — inventory from ClickHouse
- [x] `GET /hosts/{machine_id}` — detail + warn logs + Grafana deep link
- [x] `GET /system/summary` — overview cards

## Custom UI (Day 6)

- [x] Next.js App Router + TypeScript + Tailwind in `frontend/`
- [x] Pages: Overview, Hosts, Host detail, Alerts, Logs, Setup
- [x] Server-side API key (never exposed to the browser)
- [x] Empty / error states; Grafana deep links

## Load + docs (Day 7)

- [x] `tests/load/fleet_simulator.py` (multi-machine, optional logs, fail-rate)
- [x] README updated for visualization stage + architecture evolution
- [x] `docs/phase-7-architecture.md`
- [x] `docs/phase-7-graduation.md` (this file)
- [x] Interview questions + cloud-readiness + screenshot checklist

## Understanding (explain without notes)

- [x] Why Grafana for ops charts and a thin UI for product workflows
- [x] Why `/prometheus` instead of colliding with ingest `/metrics`
- [x] Why Grafana-managed alerts first vs a custom rule engine
- [x] Why hosts are derived from metrics rather than a registry table
- [x] Where backpressure still lives (Kafka lag → 503)
- [x] What is lab-scale vs production-scale load

## Docs

- [x] docs/phase-7-architecture.md
- [x] docs/phase-7-graduation.md (this file)
- [x] README reflects Phase 7 complete

---

## Phase 7 complete

You can now demo InsightNode end-to-end: agents ingest telemetry, metrics appear in Grafana, the platform observes itself via Prometheus, alerts fire into PostgreSQL, and the custom UI supports hosts / logs / overview workflows suitable for GitHub, LinkedIn, and system-design interviews.
