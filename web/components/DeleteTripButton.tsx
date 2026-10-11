"use client";

import { useRouter } from "next/navigation";
import { useState } from "react";

export function DeleteTripButton({ tripId }: { tripId: string }) {
  const [working, setWorking] = useState(false);
  const [confirming, setConfirming] = useState(false);
  const router = useRouter();

  if (!confirming) {
    return (
      <button
        onClick={() => setConfirming(true)}
        className="text-xs text-zinc-400 underline-offset-2 hover:text-red-600 hover:underline dark:hover:text-red-400"
      >
        Delete this trip
      </button>
    );
  }

  return (
    <span className="inline-flex items-center gap-2 text-xs">
      <span className="text-zinc-500 dark:text-zinc-400">Delete all data for this trip?</span>
      <button
        onClick={async () => {
          setWorking(true);
          await fetch(`/api/trips/${tripId}`, { method: "DELETE" });
          router.push("/");
          router.refresh();
        }}
        disabled={working}
        className="font-medium text-red-600 underline-offset-2 hover:underline disabled:opacity-50 dark:text-red-400"
      >
        {working ? "Deleting..." : "Yes, delete"}
      </button>
      <button
        onClick={() => setConfirming(false)}
        disabled={working}
        className="text-zinc-400 underline-offset-2 hover:underline disabled:opacity-50"
      >
        Cancel
      </button>
    </span>
  );
}
