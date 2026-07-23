# Screenshot checklist

Capture these for GitHub / LinkedIn / articles. Store PNGs here (optional; keep under a few MB each). Do **not** include real secrets or customer data.

| # | Shot | How |
|---|------|-----|
| 1 | Architecture diagram | Export from README / Phase 7 arch mermaid |
| 2 | Grafana Infrastructure dashboard | CPU / memory / disk with time range |
| 3 | Grafana host filter | Same dashboard, one `machine_id` selected |
| 4 | Platform dashboard | HTTP rate / ingest / lag |
| 5 | Custom UI Overview | http://localhost:3001 |
| 6 | Hosts list | Online/offline + gauges |
| 7 | Host detail | Gauges + Grafana link |
| 8 | Alert event (firing) | UI Alerts page or Grafana alert |
| 9 | Log search | UI Logs with a warn/error query |
| 10 | Agent setup page | Setup instructions (placeholders only) |

## Capture tips

- Use a dark theme consistently.
- Blur or omit API keys.
- Prefer 1600px-wide window for readable charts.
- Name files `01-architecture.png`, `02-grafana-infra.png`, …
