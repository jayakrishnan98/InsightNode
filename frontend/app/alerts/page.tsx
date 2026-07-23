import { ApiError, getAlertEvents } from "@/lib/api";
import { grafanaInfraUrl } from "@/lib/format";
import { AlertEventList } from "@/components/AlertEventList";
import { GrafanaLink } from "@/components/GrafanaLink";
import { ErrorBanner } from "@/components/Status";

export const dynamic = "force-dynamic";

type Props = {
  searchParams: Promise<{ status?: string }>;
};

export default async function AlertsPage({ searchParams }: Props) {
  const sp = await searchParams;
  const status =
    sp.status === "firing" || sp.status === "resolved" ? sp.status : undefined;

  try {
    const data = await getAlertEvents(status);
    return (
      <div className="space-y-6">
        <div className="flex flex-wrap items-start justify-between gap-4">
          <div>
            <h1 className="text-2xl font-semibold tracking-tight text-mist-100">
              Alerts
            </h1>
            <p className="mt-1 text-sm text-mist-400">
              Event history from Grafana webhooks. Rules are managed in Grafana,
              not in this UI.
            </p>
          </div>
          <GrafanaLink href={`${grafanaInfraUrl().split("/d/")[0]}/alerting/list`}>
            Manage rules in Grafana
          </GrafanaLink>
        </div>

        <div className="flex gap-2 text-sm">
          <FilterLink href="/alerts" active={!status} label="All" />
          <FilterLink
            href="/alerts?status=firing"
            active={status === "firing"}
            label="Firing"
          />
          <FilterLink
            href="/alerts?status=resolved"
            active={status === "resolved"}
            label="Resolved"
          />
        </div>

        <AlertEventList alerts={data.alerts} />
      </div>
    );
  } catch (err) {
    const message =
      err instanceof ApiError
        ? `Could not load alert events (${err.status}).`
        : "Could not load alert events.";
    return (
      <div className="space-y-4">
        <h1 className="text-2xl font-semibold text-mist-100">Alerts</h1>
        <ErrorBanner message={message} />
      </div>
    );
  }
}

function FilterLink({
  href,
  active,
  label,
}: {
  href: string;
  active: boolean;
  label: string;
}) {
  return (
    <a
      href={href}
      className={`rounded-md px-3 py-1.5 ${
        active
          ? "bg-ink-700 text-mist-100"
          : "text-mist-400 hover:bg-ink-800 hover:text-mist-100"
      }`}
    >
      {label}
    </a>
  );
}
