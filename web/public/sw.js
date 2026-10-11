// Real Web Push handling (Milestone 8's server side sends real
// VAPID-signed, AES128GCM-encrypted pushes to whatever subscription this
// registers) -- this is the piece Milestone 8's own live verification
// couldn't close (no web app existed yet to issue a real browser
// subscription). No offline caching beyond what's needed for
// installability -- this is a live-data app; stale cached risk data
// would be actively misleading, so there's no app-shell precache here.
self.addEventListener("push", (event) => {
  const text = event.data ? event.data.text() : "Your connection risk changed.";
  event.waitUntil(
    self.registration.showNotification("Layover", {
      body: text,
      icon: "/icon-192.png",
      badge: "/icon-192.png",
    })
  );
});

self.addEventListener("notificationclick", (event) => {
  event.notification.close();
  event.waitUntil(self.clients.openWindow("/"));
});
