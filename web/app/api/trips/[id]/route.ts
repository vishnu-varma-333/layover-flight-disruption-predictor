import { NextResponse } from "next/server";
import { backendFetch } from "@/lib/backend";
import { getUserId } from "@/lib/session";
import { Trip } from "@/lib/types";

export async function GET(_req: Request, { params }: { params: Promise<{ id: string }> }) {
  const userId = await getUserId();
  if (!userId) return NextResponse.json({ error: "sign in required" }, { status: 401 });
  const { id } = await params;
  const trip = await backendFetch<Trip>(`/trips/${encodeURIComponent(id)}`);
  if (trip.user_id !== userId) {
    return NextResponse.json({ error: "not found" }, { status: 404 });
  }
  return NextResponse.json(trip);
}
