import Image from "next/image";
import Link from "next/link";

export function SiteHeader() {
  return (
    <header className="sticky top-0 z-50 border-b border-surface-border/60 bg-brand-tint/85 backdrop-blur-md">
      <nav className="mx-auto flex max-w-6xl items-center justify-between px-6 py-3">
        <Link
          href="/"
          aria-label="IGEM home"
          className="flex items-center gap-2.5"
        >
          <Image
            src="/logo.png"
            alt=""
            width={682}
            height={817}
            priority
            className="h-10 w-auto"
          />
          <span className="text-xl font-semibold tracking-tight text-brand">
            IG<span className="text-brand-accent">E</span>M
          </span>
        </Link>
        <ul className="flex gap-6 text-sm text-slate-700 dark:text-slate-300">
          <li>
            <a
              href="/docs/"
              target="_blank"
              rel="noopener noreferrer"
              className="transition hover:text-brand"
            >
              Documentation
            </a>
          </li>
          <li>
            <Link
              href="/publications"
              className="transition hover:text-brand"
            >
              Publications
            </Link>
          </li>
          <li>
            <Link href="/downloads" className="transition hover:text-brand">
              Downloads
            </Link>
          </li>
        </ul>
      </nav>
    </header>
  );
}
