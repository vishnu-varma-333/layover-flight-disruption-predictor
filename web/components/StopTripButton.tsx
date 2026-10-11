"use client";

import { useRouter } from "next/navigation";
import { useState } from "react";

export function StopTripButton({ tripId }: { tripId: string }) {
  const [working, setWorking] = useState(false);
  const router = useRouter();

  return (
    <button
      onClick={async () => {
        setWorking(true);
        await fetch(`/api/trips/${tripId}/stop`, { method: "POST" });
        setWorking(false);
        router.refresh();
      }}
      disabled={working}
      className="text-xs text-zinc-400 underline-offset-2 hover:text-zinc-700 hover:underline disabled:opacity-50 dark:hover:text-zinc-200"
    >
      {working ? "Stopping..." : "Stop monitoring this trip"}
    </button>
  );
}
