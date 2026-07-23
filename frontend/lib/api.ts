/**
 * Server-side InsightNode API client.
 * API key stays on the server — never expose to the browser.
 */

export type HostStatus = "online" | "offline";

export type HostLatest = {
  cpu_usage?: number | null;
  memory_usage?: number | null;
  disk_usage?: number | null;
};

export type Host = {
  machine_id: string;
  last_seen: string;
  status: HostStatus;
  latest: HostLatest;
};

export type HostDetail = Host & {
  grafana_url: string;
  recent_warn_logs: LogHit[];
};

export type SystemSummary = {
  active_hosts: number;
  offline_hosts: number;
  open_alerts: number;
  latest_metric_at: string | null;
  kafka_lag_total: number;
  grafana_url: string;
  active_within_seconds: number;
};

export type AlertEvent = {
  id: number;
  tenant_id: string;
  fingerprint: string;
  rule_name: string;
  status: string;
  severity: string | null;
  machine_id: string | null;
  metric_name: string | null;
  summary: string | null;
  starts_at: string;
  ends_at: string | null;
  created_at: string;
};

export type LogHit = {
  event_id: string;
  tenant_id?: string | null;
  machine_id: string;
  service: string;
  level: string;
  message: string;
  timestamp: string;
  attrs?: Record<string, unknown>;
  score?: number | null;
};

export class ApiError extends Error {
  status: number;
  body: string;

  constructor(status: number, body: string) {
    super(`API ${status}: ${body}`);
    this.status = status;
    this.body = body;
  }
}

function apiBase(): string {
  return (process.env.INSIGHTNODE_API_BASE || "http://127.0.0.1:8001").replace(
    /\/$/,
    "",
  );
}

function apiKey(): string {
  return process.env.INSIGHTNODE_API_KEY || "dev-local-key";
}

export async function apiGet<T>(
  path: string,
  query?: Record<string, string | number | undefined | null>,
): Promise<T> {
  const url = new URL(apiBase() + path);
  if (query) {
    for (const [k, v] of Object.entries(query)) {
      if (v !== undefined && v !== null && v !== "") {
        url.searchParams.set(k, String(v));
      }
    }
  }

  const res = await fetch(url.toString(), {
    headers: {
      Accept: "application/json",
      "X-API-Key": apiKey(),
    },
    cache: "no-store",
  });

  if (!res.ok) {
    const body = await res.text();
    throw new ApiError(res.status, body.slice(0, 500));
  }

  return res.json() as Promise<T>;
}

export function getSystemSummary() {
  return apiGet<SystemSummary>("/system/summary");
}

export function getHosts() {
  return apiGet<{ count: number; hosts: Host[] }>("/hosts");
}

export function getHost(machineId: string) {
  return apiGet<HostDetail>(`/hosts/${encodeURIComponent(machineId)}`);
}

export function getAlertEvents(status?: "firing" | "resolved") {
  return apiGet<{ count: number; alerts: AlertEvent[] }>("/alert-events", {
    status,
    limit: 50,
  });
}

export function searchLogs(params: {
  q?: string;
  machine_id?: string;
  service?: string;
  level?: string;
  start_time?: string;
  end_time?: string;
  limit?: number;
  offset?: number;
}) {
  return apiGet<{ total: number; count: number; logs: LogHit[] }>(
    "/logs/search",
    params,
  );
}

export function getHealth() {
  return apiGet<Record<string, unknown>>("/health");
}
