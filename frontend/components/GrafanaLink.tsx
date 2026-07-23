import type { CSSProperties, ReactNode } from "react";
import Link from "next/link";

const linkStyle: CSSProperties = {
  color: "#4d8fd6",
  textDecoration: "underline",
  textUnderlineOffset: "3px",
};

type Props = {
  href: string;
  children?: ReactNode;
  className?: string;
};

export function GrafanaLink({ href, children, className }: Props) {
  return (
    <a
      href={href}
      target="_blank"
      rel="noopener noreferrer"
      className={className}
      style={linkStyle}
    >
      {children ?? "Open in Grafana"}
    </a>
  );
}

export function InternalLink({
  href,
  children,
}: {
  href: string;
  children: ReactNode;
}) {
  return (
    <Link href={href} style={linkStyle}>
      {children}
    </Link>
  );
}
