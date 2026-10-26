import type { NextConfig } from "next";
import { PHASE_DEVELOPMENT_SERVER } from "next/constants";

const baseConfig: NextConfig = {
  trailingSlash: true,
  images: {
    unoptimized: true,
  },
};

export default function config(phase: string): NextConfig {
  // Dev server: serve Sphinx docs index at /docs/ (static hosts do this natively).
  if (phase === PHASE_DEVELOPMENT_SERVER) {
    return {
      ...baseConfig,
      async rewrites() {
        return [{ source: "/docs/", destination: "/docs/index.html" }];
      },
    };
  }

  // Build: static export for Cloudflare Pages.
  return { ...baseConfig, output: "export" };
}
