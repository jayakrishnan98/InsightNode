"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";

const links = [
  { href: "/", label: "Overview" },
  { href: "/hosts", label: "Hosts" },
  { href: "/alerts", label: "Alerts" },
  { href: "/logs", label: "Logs" },
  { href: "/setup", label: "Setup" },
];

export function Nav() {
  const pathname = usePathname();

  return (
    <header className="border-b border-mist-200/10 bg-ink-950/80 backdrop-blur">
      <div className="mx-auto flex max-w-6xl flex-wrap items-center justify-between gap-4 px-4 py-4">
        <Link href="/" className="group flex items-baseline gap-2">
          <span className="text-xl font-semibold tracking-tight text-mist-100">
            InsightNode
          </span>
          <span className="text-xs uppercase tracking-[0.2em] text-mist-400">
            observability
          </span>
        </Link>
        <nav className="flex flex-wrap gap-1">
          {links.map((link) => {
            const active =
              link.href === "/"
                ? pathname === "/"
                : pathname.startsWith(link.href);
            return (
              <Link
                key={link.href}
                href={link.href}
                className={`rounded-md px-3 py-1.5 text-sm transition ${
                  active
                    ? "bg-ink-700 text-mist-100"
                    : "text-mist-400 hover:bg-ink-800 hover:text-mist-100"
                }`}
              >
                {link.label}
              </Link>
            );
          })}
        </nav>
      </div>
    </header>
  );
}
