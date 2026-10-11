import { NextResponse } from "next/server";
import { backendFetch } from "@/lib/backend";
import { getUserId } from "@/lib/session";

export async function POST(_req: Request, { params }: { params: Promise<{ id: string }> }) {
  const userId = await getUserId();
  if (!userId) return NextResponse.json({ error: "sign in required" }, { status: 401 });
  const { id } = await params;
  const result = await backendFetch(`/trips/${encodeURIComponent(id)}/stop`, { method: "POST" });
  return NextResponse.json(result);
}
