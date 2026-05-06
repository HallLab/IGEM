import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  trailingSlash: true,
  async rewrites() {
    return [
      { source: "/docs/", destination: "/docs/index.html" },
    ];
  },
};

export default nextConfig;
