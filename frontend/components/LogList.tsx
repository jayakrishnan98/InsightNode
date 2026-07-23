"use client";

import { useState } from "react";
import type { LogHit } from "@/lib/api";
import { formatWhen } from "@/lib/format";

function levelClass(level: string): string {
  switch (level) {
    case "error":
      return "text-signal-red";
    case "warn":
      return "text-signal-amber";
    default:
      return "text-mist-400";
  }
}

export function LogRow({ log }: { log: LogHit }) {
  const [open, setOpen] = useState(false);

  return (
    <li className="border-t border-mist-200/10 bg-ink-950/40">
      <button
        type="button"
        onClick={() => setOpen((v) => !v)}
        className="flex w-full flex-wrap items-start gap-3 px-4 py-3 text-left hover:bg-ink-800/30"
      >
        <span
          className={`w-14 shrink-0 text-xs font-semibold uppercase ${levelClass(log.level)}`}
        >
          {log.level}
        </span>
        <span className="min-w-0 flex-1 text-sm text-mist-100">
          {log.message}
        </span>
        <span className="text-xs text-mist-400">
          {formatWhen(
            typeof log.timestamp === "string"
              ? log.timestamp
              : String(log.timestamp),
          )}
        </span>
      </button>
      {open ? (
        <div className="space-y-2 border-t border-mist-200/5 bg-ink-900/60 px-4 py-3 text-xs text-mist-400">
          <p>
            <span className="text-mist-400">event_id: </span>
            <span className="font-mono text-mist-200">{log.event_id}</span>
          </p>
          <p>
            <span className="text-mist-400">host: </span>
            <span className="text-mist-200">{log.machine_id}</span>
            <span className="mx-2 text-mist-400">·</span>
            <span className="text-mist-400">service: </span>
            <span className="text-mist-200">{log.service}</span>
          </p>
          {log.attrs && Object.keys(log.attrs).length > 0 ? (
            <pre className="overflow-x-auto rounded bg-ink-950 p-3 text-mist-200">
              {JSON.stringify(log.attrs, null, 2)}
            </pre>
          ) : null}
        </div>
      ) : null}
    </li>
  );
}

export function LogList({ logs }: { logs: LogHit[] }) {
  return (
    <ul className="overflow-hidden rounded-lg border border-mist-200/10">
      {logs.map((log) => (
        <LogRow key={log.event_id} log={log} />
      ))}
    </ul>
  );
}
