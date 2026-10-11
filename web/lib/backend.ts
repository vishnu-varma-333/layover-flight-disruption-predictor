// Server-side only: every call here runs in a Route Handler or Server
// Component, never in the browser -- the Python API (layover_web.api)
// is never exposed to the client directly. See DECISIONS.md for why
// this split (Next.js owns the session, the Python service stays an
// internal, unauthenticated-but-trusted bridge to Temporal/TimescaleDB).
const BACKEND_URL = process.env.BACKEND_URL || "http://localhost:8001";

export class BackendError extends Error {
  status: number;
  constructor(status: number, message: string) {
    super(message);
    this.status = status;
  }
}

export async function backendFetch<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`${BACKEND_URL}${path}`, {
    ...init,
    headers: { "Content-Type": "application/json", ...init?.headers },
    cache: "no-store",
  });
  if (!res.ok) {
    const body = await res.text();
    throw new BackendError(res.status, body || res.statusText);
  }
  return res.json() as Promise<T>;
}
