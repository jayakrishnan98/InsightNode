import { ApiError, getAlertEvents, getSystemSummary } from "@/lib/api";
import { grafanaInfraUrl } from "@/lib/format";
import { AlertEventList } from "@/components/AlertEventList";
import { GrafanaLink } from "@/components/GrafanaLink";
import { SummaryCards } from "@/components/SummaryCards";
import { EmptyState, ErrorBanner } from "@/components/Status";

export const dynamic = "force-dynamic";

export default async function OverviewPage() {
  let summaryError: string | null = null;
  let alertsError: string | null = null;
  let summary = null;
  let alerts: Awaited<ReturnType<typeof getAlertEvents>>["alerts"] = [];

  try {
    summary = await getSystemSummary();
  } catch (err) {
    summaryError =
      err instanceof ApiError
        ? `Could not load system summary (${err.status}). Is the API running on :8001?`
        : "Could not load system summary.";
  }

  try {
    const res = await getAlertEvents("firing");
    alerts = res.alerts;
  } catch (err) {
    alertsError =
      err instanceof ApiError
        ? `Could not load alerts (${err.status}).`
        : "Could not load alerts.";
  }

  return (
    <div className="space-y-8">
      <div>
        <h1 className="text-2xl font-semibold tracking-tight text-mist-100">
          Overview
        </h1>
        <p className="mt-1 text-sm text-mist-400">
          Fleet health and open alerts. Charts stay in Grafana.
        </p>
      </div>

      {summaryError ? <ErrorBanner message={summaryError} /> : null}
      {summary ? <SummaryCards summary={summary} /> : null}

      <section className="space-y-3">
        <div className="flex flex-wrap items-center justify-between gap-2">
          <h2 className="text-lg font-medium text-mist-100">Open alerts</h2>
          <GrafanaLink href={grafanaInfraUrl()}>
            Infrastructure dashboard
          </GrafanaLink>
        </div>
        {alertsError ? <ErrorBanner message={alertsError} /> : null}
        {!alertsError && alerts.length === 0 ? (
          <EmptyState
            title="No firing alerts"
            message="Threshold rules are evaluated in Grafana; events land here via webhook."
          />
        ) : null}
        {!alertsError && alerts.length > 0 ? (
          <AlertEventList alerts={alerts} />
        ) : null}
      </section>
    </div>
  );
}
