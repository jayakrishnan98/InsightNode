"use client";

import { useRouter, useSearchParams } from "next/navigation";
import { FormEvent, useState } from "react";

const LEVELS = ["", "debug", "info", "warn", "error"];

export function LogSearchForm() {
  const router = useRouter();
  const params = useSearchParams();
  const [q, setQ] = useState(params.get("q") || "");
  const [machineId, setMachineId] = useState(params.get("machine_id") || "");
  const [service, setService] = useState(params.get("service") || "");
  const [level, setLevel] = useState(params.get("level") || "");

  function onSubmit(e: FormEvent) {
    e.preventDefault();
    const next = new URLSearchParams();
    if (q.trim()) next.set("q", q.trim());
    if (machineId.trim()) next.set("machine_id", machineId.trim());
    if (service.trim()) next.set("service", service.trim());
    if (level) next.set("level", level);
    next.set("offset", "0");
    router.push(`/logs?${next.toString()}`);
  }

  return (
    <form
      onSubmit={onSubmit}
      className="grid gap-3 rounded-lg border border-mist-200/10 bg-ink-900 p-4 sm:grid-cols-2 lg:grid-cols-5"
    >
      <label className="block text-xs text-mist-400 lg:col-span-2">
        Query
        <input
          value={q}
          onChange={(e) => setQ(e.target.value)}
          placeholder="disk, error, …"
          className="mt-1 w-full rounded-md border border-mist-200/20 bg-ink-950 px-3 py-2 text-sm text-mist-100 outline-none focus:border-signal-blue"
        />
      </label>
      <label className="block text-xs text-mist-400">
        Host
        <input
          value={machineId}
          onChange={(e) => setMachineId(e.target.value)}
          className="mt-1 w-full rounded-md border border-mist-200/20 bg-ink-950 px-3 py-2 text-sm text-mist-100 outline-none focus:border-signal-blue"
        />
      </label>
      <label className="block text-xs text-mist-400">
        Service
        <input
          value={service}
          onChange={(e) => setService(e.target.value)}
          className="mt-1 w-full rounded-md border border-mist-200/20 bg-ink-950 px-3 py-2 text-sm text-mist-100 outline-none focus:border-signal-blue"
        />
      </label>
      <label className="block text-xs text-mist-400">
        Severity
        <select
          value={level}
          onChange={(e) => setLevel(e.target.value)}
          className="mt-1 w-full rounded-md border border-mist-200/20 bg-ink-950 px-3 py-2 text-sm text-mist-100 outline-none focus:border-signal-blue"
        >
          {LEVELS.map((l) => (
            <option key={l || "any"} value={l}>
              {l || "any"}
            </option>
          ))}
        </select>
      </label>
      <div className="flex items-end lg:col-span-5">
        <button
          type="submit"
          className="rounded-md bg-signal-blue px-4 py-2 text-sm font-medium text-ink-950 hover:opacity-90"
        >
          Search
        </button>
      </div>
    </form>
  );
}
