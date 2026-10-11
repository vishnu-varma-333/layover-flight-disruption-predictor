"use client";

import { useEffect } from "react";

export function ServiceWorkerRegistration() {
  useEffect(() => {
    if ("serviceWorker" in navigator) {
      navigator.serviceWorker.register("/sw.js").catch(() => {
        // Not every environment supports Service Workers (confirmed during
        // Milestone 8 that some sandboxed/automation browser contexts
        // don't) -- push notifications simply won't be available there;
        // the rest of the app works the same either way.
      });
    }
  }, []);
  return null;
}
