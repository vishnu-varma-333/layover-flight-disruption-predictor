# Layover — web app

The Next.js PWA: add a trip, see live risk and explanations, watch the
aircraft's chain of flights for the day, enable push notifications.

See the repo root [`README.md`](../README.md) for how this fits into the
rest of the project and the full local-development setup
(`make web-api`, `make predict-service`, `make trip-worker`), and
[`DECISIONS.md`](../DECISIONS.md) for the Milestone 9 architecture and
design decisions (why a separate internal API, the session design, the
real bugs found building this).

```bash
npm install
npm run dev -- --port 3001
```
