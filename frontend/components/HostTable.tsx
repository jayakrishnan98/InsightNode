import Link from "next/link";
import type { Host } from "@/lib/api";
import { formatPercent, formatWhen } from "@/lib/format";
import { HostStatusBadge } from "@/components/Status";

export function HostTable({ hosts }: { hosts: Host[] }) {
  return (
    <div className="overflow-x-auto rounded-lg border border-mist-200/10">
      <table className="min-w-full text-left text-sm">
        <thead className="bg-ink-900 text-xs uppercase tracking-wide text-mist-400">
          <tr>
            <th className="px-4 py-3 font-medium">Machine</th>
            <th className="px-4 py-3 font-medium">Status</th>
            <th className="px-4 py-3 font-medium">Last seen</th>
            <th className="px-4 py-3 font-medium">CPU</th>
            <th className="px-4 py-3 font-medium">Memory</th>
            <th className="px-4 py-3 font-medium">Disk</th>
          </tr>
        </thead>
        <tbody>
          {hosts.map((host) => (
            <tr
              key={host.machine_id}
              className="border-t border-mist-200/10 bg-ink-950/40 hover:bg-ink-800/40"
            >
              <td className="px-4 py-3">
                <Link
                  href={`/hosts/${encodeURIComponent(host.machine_id)}`}
                  className="font-medium text-signal-blue hover:underline"
                >
                  {host.machine_id}
                </Link>
              </td>
              <td className="px-4 py-3">
                <HostStatusBadge status={host.status} />
              </td>
              <td className="px-4 py-3 text-mist-200">
                {formatWhen(host.last_seen)}
              </td>
              <td className="px-4 py-3 font-mono text-mist-100">
                {formatPercent(host.latest.cpu_usage)}
              </td>
              <td className="px-4 py-3 font-mono text-mist-100">
                {formatPercent(host.latest.memory_usage)}
              </td>
              <td className="px-4 py-3 font-mono text-mist-100">
                {formatPercent(host.latest.disk_usage)}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
