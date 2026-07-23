import type { AlertEvent } from "@/lib/api";
import { formatWhen } from "@/lib/format";
import { EmptyState } from "@/components/Status";

export function AlertEventList({ alerts }: { alerts: AlertEvent[] }) {
  if (alerts.length === 0) {
    return (
      <EmptyState
        title="No alert events"
        message="When Grafana fires or resolves a rule, events appear here."
      />
    );
  }

  return (
    <ul className="divide-y divide-mist-200/10 overflow-hidden rounded-lg border border-mist-200/10">
      {alerts.map((alert) => (
        <li key={alert.id} className="bg-ink-950/40 px-4 py-3">
          <div className="flex flex-wrap items-start justify-between gap-2">
            <div>
              <p className="font-medium text-mist-100">{alert.rule_name}</p>
              <p className="mt-1 text-sm text-mist-400">
                {alert.summary || "—"}
              </p>
            </div>
            <span
              className={`rounded-full px-2.5 py-0.5 text-xs font-medium ${
                alert.status === "firing"
                  ? "bg-signal-red/15 text-signal-red"
                  : "bg-signal-green/15 text-signal-green"
              }`}
            >
              {alert.status}
            </span>
          </div>
          <dl className="mt-2 grid gap-1 text-xs text-mist-400 sm:grid-cols-3">
            <div>
              <dt className="inline text-mist-400">Severity: </dt>
              <dd className="inline text-mist-200">{alert.severity || "—"}</dd>
            </div>
            <div>
              <dt className="inline text-mist-400">Metric: </dt>
              <dd className="inline text-mist-200">
                {alert.metric_name || "—"}
              </dd>
            </div>
            <div>
              <dt className="inline text-mist-400">Started: </dt>
              <dd className="inline text-mist-200">
                {formatWhen(alert.starts_at)}
              </dd>
            </div>
          </dl>
        </li>
      ))}
    </ul>
  );
}
