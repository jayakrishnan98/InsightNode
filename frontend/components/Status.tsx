type Props = {
  title?: string;
  message: string;
};

export function EmptyState({ title = "Nothing here yet", message }: Props) {
  return (
    <div className="rounded-lg border border-dashed border-mist-200/30 bg-ink-900/50 px-6 py-10 text-center">
      <p className="text-lg font-medium text-mist-100">{title}</p>
      <p className="mt-2 text-sm text-mist-400">{message}</p>
    </div>
  );
}

export function ErrorBanner({ message }: { message: string }) {
  return (
    <div
      role="alert"
      className="rounded-lg border border-signal-red/40 bg-signal-red/10 px-4 py-3 text-sm text-mist-100"
    >
      {message}
    </div>
  );
}

export function HostStatusBadge({ status }: { status: "online" | "offline" }) {
  const online = status === "online";
  return (
    <span
      className={`inline-flex items-center gap-1.5 rounded-full px-2.5 py-0.5 text-xs font-medium ${
        online
          ? "bg-signal-green/15 text-signal-green"
          : "bg-signal-red/15 text-signal-red"
      }`}
    >
      <span
        className={`h-1.5 w-1.5 rounded-full ${
          online ? "bg-signal-green" : "bg-signal-red"
        }`}
      />
      {status}
    </span>
  );
}
