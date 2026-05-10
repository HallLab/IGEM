"use client";

import { useState } from "react";

export function CopyableCode({
  code,
  className = "",
}: {
  code: string;
  className?: string;
}) {
  const [copied, setCopied] = useState(false);

  const onCopy = async () => {
    try {
      await navigator.clipboard.writeText(code);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    } catch {
      /* clipboard unavailable */
    }
  };

  return (
    <div className={`relative ${className}`}>
      <pre className="overflow-x-auto rounded-lg bg-zinc-900 p-4 text-sm leading-relaxed text-zinc-100">
        <code>{code}</code>
      </pre>
      <button
        onClick={onCopy}
        aria-label="Copy to clipboard"
        className="absolute right-2 top-2 rounded-md border border-zinc-700 bg-zinc-800 px-2 py-1 text-xs font-medium text-zinc-300 transition hover:bg-zinc-700"
      >
        {copied ? "Copied" : "Copy"}
      </button>
    </div>
  );
}
