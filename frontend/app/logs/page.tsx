import { Suspense } from "react";
import Link from "next/link";
import { ApiError, searchLogs } from "@/lib/api";
import { LogList } from "@/components/LogList";
import { LogSearchForm } from "@/components/LogSearchForm";
import { EmptyState, ErrorBanner } from "@/components/Status";

export const dynamic = "force-dynamic";

type Props = {
  searchParams: Promise<Record<string, string | string[] | undefined>>;
};

function first(v: string | string[] | undefined): string | undefined {
  if (Array.isArray(v)) return v[0];
  return v;
}

export default async function LogsPage({ searchParams }: Props) {
  const sp = await searchParams;
  const q = first(sp.q);
  const machine_id = first(sp.machine_id);
  const service = first(sp.service);
  const level = first(sp.level);
  const offset = Number(first(sp.offset) || "0") || 0;
  const limit = 50;

  let error: string | null = null;
  let total = 0;
  let logs: Awaited<ReturnType<typeof searchLogs>>["logs"] = [];

  try {
    const data = await searchLogs({
      q,
      machine_id,
      service,
      level,
      limit,
      offset,
    });
    total = data.total;
    logs = data.logs;
  } catch (err) {
    error =
      err instanceof ApiError
        ? `Log search failed (${err.status}). Is OpenSearch up?`
        : "Log search failed.";
  }

  const nextOffset = offset + limit;
  const prevOffset = Math.max(0, offset - limit);
  const qs = new URLSearchParams();
  if (q) qs.set("q", q);
  if (machine_id) qs.set("machine_id", machine_id);
  if (service) qs.set("service", service);
  if (level) qs.set("level", level);

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-semibold tracking-tight text-mist-100">
          Logs
        </h1>
        <p className="mt-1 text-sm text-mist-400">
          Simplified search over OpenSearch via{" "}
          <code className="text-mist-200">/logs/search</code>.
        </p>
      </div>

      <Suspense fallback={<div className="text-sm text-mist-400">Loading form…</div>}>
        <LogSearchForm />
      </Suspense>

      {error ? <ErrorBanner message={error} /> : null}

      {!error && logs.length === 0 ? (
        <EmptyState
          title="No matching logs"
          message="Try a broader query, or ship logs from the agent / API."
        />
      ) : null}

      {!error && logs.length > 0 ? (
        <div className="space-y-3">
          <p className="text-sm text-mist-400">
            Showing {logs.length} of {total} (offset {offset})
          </p>
          <LogList logs={logs} />
          <div className="flex gap-3 text-sm">
            {offset > 0 ? (
              <Link
                href={`/logs?${qs.toString()}&offset=${prevOffset}`}
                className="text-signal-blue hover:underline"
              >
                Previous
              </Link>
            ) : null}
            {nextOffset < total ? (
              <Link
                href={`/logs?${qs.toString()}&offset=${nextOffset}`}
                className="text-signal-blue hover:underline"
              >
                Next
              </Link>
            ) : null}
          </div>
        </div>
      ) : null}
    </div>
  );
}
