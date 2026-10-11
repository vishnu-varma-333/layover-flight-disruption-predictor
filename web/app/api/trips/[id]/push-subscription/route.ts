import { NextRequest, NextResponse } from "next/server";
import { backendFetch } from "@/lib/backend";
import { getUserId } from "@/lib/session";

export async function POST(req: NextRequest, { params }: { params: Promise<{ id: string }> }) {
  const userId = await getUserId();
  if (!userId) return NextResponse.json({ error: "sign in required" }, { status: 401 });
  const { id } = await params;
  const body = await req.json();
  const result = await backendFetch(`/trips/${encodeURIComponent(id)}/push-subscription`, {
    method: "POST",
    body: JSON.stringify(body),
  });
  return NextResponse.json(result);
}
