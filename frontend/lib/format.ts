export function formatWhen(iso: string | null | undefined): string {
  if (!iso) return "—";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  return d.toLocaleString(undefined, {
    dateStyle: "medium",
    timeStyle: "medium",
  });
}

export function formatPercent(value: number | null | undefined): string {
  if (value === null || value === undefined || Number.isNaN(value)) return "—";
  return `${value.toFixed(1)}%`;
}

export function grafanaInfraUrl(machineId?: string): string {
  const base = (
    process.env.NEXT_PUBLIC_GRAFANA_URL || "http://localhost:3000"
  ).replace(/\/$/, "");
  const path = `${base}/d/insightnode-infrastructure/infrastructure-monitoring`;
  if (!machineId) return path;
  return `${path}?var-machine_id=${encodeURIComponent(machineId)}`;
}
