import { ApiError, getHosts } from "@/lib/api";
import { HostTable } from "@/components/HostTable";
import { EmptyState, ErrorBanner } from "@/components/Status";

export const dynamic = "force-dynamic";

export default async function HostsPage() {
  try {
    const data = await getHosts();
    if (data.hosts.length === 0) {
      return (
        <div className="space-y-6">
          <Header />
          <EmptyState
            title="No hosts yet"
            message="Start the agent so metrics appear in ClickHouse, then refresh."
          />
        </div>
      );
    }
    return (
      <div className="space-y-6">
        <Header />
        <HostTable hosts={data.hosts} />
      </div>
    );
  } catch (err) {
    const message =
      err instanceof ApiError
        ? `Could not load hosts (${err.status}). Is ClickHouse and the API available?`
        : "Could not load hosts.";
    return (
      <div className="space-y-6">
        <Header />
        <ErrorBanner message={message} />
      </div>
    );
  }
}

function Header() {
  return (
    <div>
      <h1 className="text-2xl font-semibold tracking-tight text-mist-100">
        Hosts
      </h1>
      <p className="mt-1 text-sm text-mist-400">
        Derived from metric streams — no separate host registry.
      </p>
    </div>
  );
}
