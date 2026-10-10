self.addEventListener("push", (event) => {
  const text = event.data ? event.data.text() : "(empty push)";
  self.registration.showNotification("Layover alert", { body: text });
  self.clients.matchAll().then((clients) => {
    clients.forEach((client) => client.postMessage({ type: "push-received", text }));
  });
});
