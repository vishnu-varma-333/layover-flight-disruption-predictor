import { NextRequest, NextResponse } from "next/server";
import { backendFetch, BackendError } from "@/lib/backend";
import { Flight } from "@/lib/types";

export async function GET(req: NextRequest) {
  const qs = req.nextUrl.searchParams;
  try {
    const results = await backendFetch<Flight[]>(`/flights/search?${qs.toString()}`);
    return NextResponse.json(results);
  } catch (err) {
    if (err instanceof BackendError) {
      return NextResponse.json({ error: err.message }, { status: err.status });
    }
    throw err;
  }
}
