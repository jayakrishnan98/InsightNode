import { ApiError, getHost } from "@/lib/api";
import { formatPercent, formatWhen, grafanaInfraUrl } from "@/lib/format";
import { GrafanaLink } from "@/components/GrafanaLink";
import { LogList } from "@/components/LogList";
import { EmptyState, ErrorBanner, HostStatusBadge } from "@/components/Status";

export const dynamic = "force-dynamic";

type Props = { params: Promise<{ id: string }> };

export default async function HostDetailPage({ params }: Props) {
  const { id } = await params;
  const machineId = decodeURIComponent(id);

  try {
    const host = await getHost(machineId);
    return (
      <div className="space-y-8">
        <div className="flex flex-wrap items-start justify-between gap-4">
          <div>
            <h1 className="text-2xl font-semibold tracking-tight text-mist-100">
              {host.machine_id}
            </h1>
            <p className="mt-2 flex flex-wrap items-center gap-3 text-sm text-mist-400">
              <HostStatusBadge status={host.status} />
              <span>Last seen {formatWhen(host.last_seen)}</span>
            </p>
          </div>
          <GrafanaLink href={host.grafana_url || grafanaInfraUrl(machineId)}>
            Open in Grafana
          </GrafanaLink>
        </div>

        <div className="grid gap-3 sm:grid-cols-3">
          {(
            [
              ["CPU", host.latest.cpu_usage],
              ["Memory", host.latest.memory_usage],
              ["Disk", host.latest.disk_usage],
            ] as const
          ).map(([label, value]) => (
            <div
              key={label}
              className="rounded-lg border border-mist-200/10 bg-ink-900 px-4 py-3"
            >
              <p className="text-xs uppercase tracking-wide text-mist-400">
                {label}
              </p>
              <p className="mt-1 font-mono text-2xl text-mist-100">
                {formatPercent(value)}
              </p>
            </div>
          ))}
        </div>

        <section className="space-y-3">
          <h2 className="text-lg font-medium text-mist-100">
            Recent warn logs
          </h2>
          {host.recent_warn_logs.length === 0 ? (
            <EmptyState
              title="No recent warnings"
              message="Threshold crossings from the agent appear here when present."
            />
          ) : (
            <LogList logs={host.recent_warn_logs} />
          )}
        </section>
      </div>
    );
  } catch (err) {
    if (err instanceof ApiError && err.status === 404) {
      return (
        <div className="space-y-4">
          <h1 className="text-2xl font-semibold text-mist-100">{machineId}</h1>
          <EmptyState
            title="Host not found"
            message="No metrics for this machine_id under the current tenant."
          />
        </div>
      );
    }
    const message =
      err instanceof ApiError
        ? `Could not load host (${err.status}).`
        : "Could not load host.";
    return (
      <div className="space-y-4">
        <h1 className="text-2xl font-semibold text-mist-100">{machineId}</h1>
        <ErrorBanner message={message} />
      </div>
    );
  }
}
