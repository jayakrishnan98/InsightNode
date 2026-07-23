import { ApiError, getHealth, getSystemSummary } from "@/lib/api";
import { formatWhen } from "@/lib/format";
import { ErrorBanner, HostStatusBadge } from "@/components/Status";

export const dynamic = "force-dynamic";

export default async function SetupPage() {
  let healthError: string | null = null;
  let summaryError: string | null = null;
  let health: Record<string, unknown> | null = null;
  let activeHosts = 0;
  let latest: string | null = null;

  try {
    health = await getHealth();
  } catch (err) {
    healthError =
      err instanceof ApiError
        ? `API health check failed (${err.status}). Start uvicorn on :8001.`
        : "API health check failed.";
  }

  try {
    const summary = await getSystemSummary();
    activeHosts = summary.active_hosts;
    latest = summary.latest_metric_at;
  } catch (err) {
    summaryError =
      err instanceof ApiError
        ? `Could not read fleet status (${err.status}).`
        : "Could not read fleet status.";
  }

  return (
    <div className="space-y-8">
      <div>
        <h1 className="text-2xl font-semibold tracking-tight text-mist-100">
          Agent setup
        </h1>
        <p className="mt-1 text-sm text-mist-400">
          Connect a host agent. Secrets shown here are placeholders for local
          labs — never commit real keys.
        </p>
      </div>

      <section className="space-y-3 rounded-lg border border-mist-200/10 bg-ink-900 p-5">
        <h2 className="text-lg font-medium text-mist-100">
          Connected agent status
        </h2>
        {healthError ? <ErrorBanner message={healthError} /> : null}
        {summaryError ? <ErrorBanner message={summaryError} /> : null}
        {!healthError && health ? (
          <dl className="grid gap-2 text-sm sm:grid-cols-2">
            <div>
              <dt className="text-mist-400">API</dt>
              <dd className="mt-1">
                <HostStatusBadge
                  status={health.status === "ok" ? "online" : "offline"}
                />
              </dd>
            </div>
            <div>
              <dt className="text-mist-400">Active hosts</dt>
              <dd className="mt-1 text-mist-100">{activeHosts}</dd>
            </div>
            <div>
              <dt className="text-mist-400">Latest metric</dt>
              <dd className="mt-1 text-mist-100">{formatWhen(latest)}</dd>
            </div>
            <div>
              <dt className="text-mist-400">Kafka / CH / OS</dt>
              <dd className="mt-1 font-mono text-xs text-mist-200">
                kafka={String(health.kafka_ok)} clickhouse=
                {String(health.clickhouse_ok)} opensearch=
                {String(health.opensearch_ok)}
              </dd>
            </div>
          </dl>
        ) : null}
      </section>

      <section className="space-y-3">
        <h2 className="text-lg font-medium text-mist-100">Install</h2>
        <ol className="list-decimal space-y-2 pl-5 text-sm text-mist-200">
          <li>Create a Python venv and install root <code>requirements.txt</code>.</li>
          <li>
            Start infra:{" "}
            <code className="text-mist-100">docker compose up -d</code>
          </li>
          <li>
            Start API:{" "}
            <code className="text-mist-100">
              uvicorn backend.main:app --reload --port 8001
            </code>
          </li>
          <li>
            Start worker:{" "}
            <code className="text-mist-100">python -m backend.worker</code>
          </li>
          <li>
            Start agent from <code className="text-mist-100">agent/</code>
          </li>
        </ol>
      </section>

      <section className="space-y-3">
        <h2 className="text-lg font-medium text-mist-100">
          Environment variables
        </h2>
        <pre className="overflow-x-auto rounded-lg border border-mist-200/10 bg-ink-950 p-4 text-xs text-mist-200">{`# Agent (examples — replace for shared environments)
INSIGHTNODE_METRICS_URL=http://127.0.0.1:8001/metrics
INSIGHTNODE_LOGS_URL=http://127.0.0.1:8001/logs
INSIGHTNODE_API_KEY=<your-tenant-api-key>
COLLECTION_INTERVAL_SECONDS=5`}</pre>
      </section>

      <section className="space-y-3">
        <h2 className="text-lg font-medium text-mist-100">Sample command</h2>
        <pre className="overflow-x-auto rounded-lg border border-mist-200/10 bg-ink-950 p-4 text-xs text-mist-200">{`cd agent
export INSIGHTNODE_API_KEY=dev-local-key
python main.py`}</pre>
      </section>
    </div>
  );
}
