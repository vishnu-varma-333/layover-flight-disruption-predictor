import { NextRequest, NextResponse } from "next/server";
import { clearSession, getUserId, setSession } from "@/lib/session";

export async function GET() {
  const userId = await getUserId();
  return NextResponse.json({ userId });
}

export async function POST(req: NextRequest) {
  const { email } = await req.json();
  if (typeof email !== "string" || !email.includes("@")) {
    return NextResponse.json({ error: "a valid email is required" }, { status: 400 });
  }
  await setSession(email.trim().toLowerCase());
  return NextResponse.json({ userId: email.trim().toLowerCase() });
}

export async function DELETE() {
  await clearSession();
  return NextResponse.json({ ok: true });
}
