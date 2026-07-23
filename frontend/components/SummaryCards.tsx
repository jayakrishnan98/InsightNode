import type { SystemSummary } from "@/lib/api";
import { formatWhen } from "@/lib/format";
import { GrafanaLink } from "@/components/GrafanaLink";

type Card = {
  label: string;
  value: string;
  hint?: string;
};

export function SummaryCards({ summary }: { summary: SystemSummary }) {
  const cards: Card[] = [
    {
      label: "Active hosts",
      value: String(summary.active_hosts),
      hint: `within ${summary.active_within_seconds}s`,
    },
    {
      label: "Offline hosts",
      value: String(summary.offline_hosts),
    },
    {
      label: "Open alerts",
      value: String(summary.open_alerts),
    },
    {
      label: "Kafka lag",
      value: String(summary.kafka_lag_total),
    },
    {
      label: "Latest metrics",
      value: formatWhen(summary.latest_metric_at),
    },
  ];

  return (
    <div className="space-y-4">
      <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-5">
        {cards.map((card) => (
          <div
            key={card.label}
            className="rounded-lg border border-mist-200/10 bg-ink-900 px-4 py-3"
          >
            <p className="text-xs uppercase tracking-wide text-mist-400">
              {card.label}
            </p>
            <p className="mt-1 text-xl font-semibold text-mist-100">
              {card.value}
            </p>
            {card.hint ? (
              <p className="mt-1 text-xs text-mist-400">{card.hint}</p>
            ) : null}
          </div>
        ))}
      </div>
      <p className="text-sm text-mist-400">
        Detailed charts live in Grafana —{" "}
        <GrafanaLink href={summary.grafana_url}>
          open Grafana home
        </GrafanaLink>
      </p>
    </div>
  );
}
