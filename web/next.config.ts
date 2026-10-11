import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  /* config options here */
  // Cache Components' prerendering benefit doesn't apply here -- every
  // page reads live, per-request data (the signed-in user's trips, real-
  // time risk) that's never safe to prerender or statically cache, so
  // this is left off rather than adding Suspense boundaries purely to
  // satisfy a dev-overlay hint for an optimization this app can't use.
  agentRules: false,
  turbopack: {
    rules: {
      "*.css": {
        loaders: ["@tailwindcss/turbopack"],
        as: "*.css",
      },
    },
  },
};

export default nextConfig;
