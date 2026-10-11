"use client";

import { useState } from "react";
import { urlBase64ToUint8Array } from "@/lib/push";

export function PushSubscribeButton({ tripId }: { tripId: string }) {
  const [state, setState] = useState<"idle" | "working" | "done" | "unsupported" | "error">(
    "idle"
  );

  async function subscribe() {
    if (!("serviceWorker" in navigator) || !("PushManager" in window)) {
      setState("unsupported");
      return;
    }
    setState("working");
    try {
      const { key } = await fetch("/api/vapid-public-key").then((r) => r.json());
      // `navigator.serviceWorker.ready` only resolves once a worker is
      // actually active -- if registration itself fails (confirmed live:
      // some sandboxed/automation browser contexts disable Service
      // Workers entirely, even though the API objects exist), `ready`
      // never resolves and this hangs on "Enabling..." forever instead
      // of failing. Registering directly here, with its own explicit
      // error path, is what actually surfaces that failure.
      const registration = await navigator.serviceWorker.register("/sw.js");
      await navigator.serviceWorker.ready;
      const subscription = await registration.pushManager.subscribe({
        userVisibleOnly: true,
        applicationServerKey: urlBase64ToUint8Array(key) as BufferSource,
      });
      await fetch(`/api/trips/${tripId}/push-subscription`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ subscription: subscription.toJSON() }),
      });
      setState("done");
    } catch {
      setState("error");
    }
  }

  if (state === "done") {
    return <p className="text-sm text-emerald-700 dark:text-emerald-400">Push notifications enabled for this trip.</p>;
  }
  if (state === "unsupported") {
    return (
      <p className="text-sm text-zinc-500">
        Push notifications aren&apos;t supported in this browser. Email alerts will still be sent.
      </p>
    );
  }
  if (state === "error") {
    return <p className="text-sm text-red-600 dark:text-red-400">Couldn&apos;t enable push notifications. Email alerts will still be sent.</p>;
  }

  return (
    <button
      onClick={subscribe}
      disabled={state === "working"}
      className="rounded-md border border-zinc-300 px-3 py-1.5 text-sm font-medium text-zinc-700 transition hover:bg-zinc-50 disabled:opacity-50 dark:border-zinc-700 dark:text-zinc-300 dark:hover:bg-zinc-900"
    >
      {state === "working" ? "Enabling..." : "Enable push notifications"}
    </button>
  );
}
