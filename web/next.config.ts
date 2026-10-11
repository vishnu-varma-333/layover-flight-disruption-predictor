import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  /* config options here */
  // Cache Components' prerendering benefit doesn't apply here -- every
  // page reads live, per-request data (the signed-in user's trips, real-
  // time risk) that's never safe to prerender or statically cache, so
  // this is left off rather than adding Suspense boundaries purely to
  // satisfy a dev-overlay hint for an optimization this app can't use.
  agentRules: false,
  // Milestone 10: a self-contained server bundle (just node + this
  // output dir, no node_modules copy) for the production Docker image --
  // see web/Dockerfile.
  output: "standalone",
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
