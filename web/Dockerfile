# Milestone 10: multi-stage build -- `deps` and `builder` stages carry
# the full node_modules and build toolchain, but the final `runner`
# stage only copies Next.js's `output: "standalone"` bundle (see
# next.config.ts), which already includes just the production
# dependencies actually used. Much smaller final image than `npm ci
# --omit=dev` + a full copy would give.
FROM node:20-slim AS deps
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci

FROM node:20-slim AS builder
WORKDIR /app
COPY --from=deps /app/node_modules ./node_modules
COPY . .
RUN npm run build

FROM node:20-slim AS runner
WORKDIR /app
ENV NODE_ENV=production
COPY --from=builder /app/public ./public
COPY --from=builder /app/.next/standalone ./
COPY --from=builder /app/.next/static ./.next/static

EXPOSE 3001
ENV PORT=3001
CMD ["node", "server.js"]
