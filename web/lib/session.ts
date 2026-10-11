import { createHmac, timingSafeEqual } from "crypto";
import { cookies } from "next/headers";

// Lightweight, real sign-in (email identifies you, no password) rather
// than a fake "always logged in as demo-user" skip -- appropriate for a
// demo product with no sensitive data and no external auth provider
// credentials available. The cookie is HMAC-signed so it can't be
// trivially forged client-side; it does not verify email ownership
// (there's no mail-based confirmation step), which is a deliberate,
// named scope boundary for this milestone, not an oversight -- see
// DECISIONS.md.
const COOKIE_NAME = "layover_session";
const SECRET = process.env.SESSION_SECRET || "layover-dev-secret-not-for-production";

function sign(value: string): string {
  return createHmac("sha256", SECRET).update(value).digest("hex");
}

function pack(userId: string): string {
  return `${userId}.${sign(userId)}`;
}

function unpack(token: string): string | null {
  const lastDot = token.lastIndexOf(".");
  if (lastDot === -1) return null;
  const userId = token.slice(0, lastDot);
  const signature = token.slice(lastDot + 1);
  const expected = sign(userId);
  const a = Buffer.from(signature);
  const b = Buffer.from(expected);
  if (a.length !== b.length || !timingSafeEqual(a, b)) return null;
  return userId;
}

export async function getUserId(): Promise<string | null> {
  const store = await cookies();
  const token = store.get(COOKIE_NAME)?.value;
  if (!token) return null;
  return unpack(token);
}

export async function setSession(userId: string) {
  const store = await cookies();
  store.set(COOKIE_NAME, pack(userId), {
    httpOnly: true,
    sameSite: "lax",
    path: "/",
    maxAge: 60 * 60 * 24 * 30,
  });
}

export async function clearSession() {
  const store = await cookies();
  store.delete(COOKIE_NAME);
}
