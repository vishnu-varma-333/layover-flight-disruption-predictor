import { NextRequest, NextResponse } from "next/server";
import { backendFetch } from "@/lib/backend";
import { getUserId } from "@/lib/session";
import { TripSummary } from "@/lib/types";

export async function GET() {
  const userId = await getUserId();
  if (!userId) return NextResponse.json({ error: "sign in required" }, { status: 401 });
  const trips = await backendFetch<TripSummary[]>(`/trips?user_id=${encodeURIComponent(userId)}`);
  return NextResponse.json(trips);
}

export async function POST(req: NextRequest) {
  const userId = await getUserId();
  if (!userId) return NextResponse.json({ error: "sign in required" }, { status: 401 });
  const body = await req.json();
  const result = await backendFetch<{ trip_id: string }>("/trips", {
    method: "POST",
    body: JSON.stringify({ ...body, user_id: userId, to_email: userId }),
  });
  return NextResponse.json(result);
}
