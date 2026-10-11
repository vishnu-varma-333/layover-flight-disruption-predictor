import { NextResponse } from "next/server";
import { backendFetch } from "@/lib/backend";

export async function GET() {
  const result = await backendFetch<{ key: string }>("/vapid-public-key");
  return NextResponse.json(result);
}
