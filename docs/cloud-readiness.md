# Cloud readiness (lab → first deploy)

InsightNode’s first cloud footprint should stay **one VM + Docker Compose** — not Kubernetes.

---

## Suggested layout

```text
Public (reverse proxy :443)
  ├── Custom UI (:3001)
  ├── Grafana (:3000) — strong admin password; optional IP allowlist
  └── API (:8001) — rate-limited; prefer private if UI proxies everything

Private (no public bind)
  ├── PostgreSQL
  ├── ClickHouse
  ├── OpenSearch
  ├── Redpanda / Kafka
  ├── Prometheus
  └── Jaeger (optional public UI)
```

---

## Checklist

| Topic | Guidance |
|-------|----------|
| **Firewall** | Only 80/443 public; databases private |
| **TLS / DNS** | Caddy or Nginx + Let’s Encrypt |
| **Env / secrets** | Host `.env` (not in git); rotate lab defaults |
| **Volumes** | Named volumes for CH / OS / Grafana / Prometheus / PG |
| **Restart** | `restart: unless-stopped` on Compose services |
| **Health** | Compose healthchecks + API `GET /health` |
| **Backups** | Volume snapshots; document RPO as best-effort for learning |
| **Log rotation** | Docker `json-file` `max-size` / `max-file` |
| **Grafana webhook** | Point contact URL at the API’s private/internal hostname |
| **Prometheus scrape** | Prefer container DNS over `host.docker.internal` once API is containerized |

---

## Not for v1

- Kubernetes / Helm
- Multi-region active-active
- Customer SSO
- OpenSearch security plugin (enable before any public exposure)

See also [phase-7-architecture.md](phase-7-architecture.md).
