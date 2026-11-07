# Interview walkthrough: Distributed request limiting

## Explain it in 45 seconds
I built a two-instance Go demonstration using shared Redis counters, with Pub/Sub and WebSockets for visibility. I later made increment and expiry atomic, added repeatable cross-instance tests, and tightened request identity handling.

## Demonstrate it
1. Run `docker compose up --build` and open http://localhost:8081.
2. Leave the limiter enabled; pause generated traffic and wait a second.
3. Run `go run ./cmd/demo`: 20 requests split across two instances should produce 10 allowed and 10 blocked in a fresh window.
4. Run `go test -run TestSharedLimitAndExpiry -v .` for a deterministic cross-instance/expiry check without Docker.
5. Explain that the test uses miniredis, a Redis-compatible emulator. It does not establish real Redis performance or deployment reliability.

## Questions to be ready for
- Why Redis? All instances need one shared counter; independent local counters multiply the effective limit.
- Why Lua? Increment and expiration must execute as one atomic server operation.
- Which algorithm? A counter with a one-second lifetime from its first request; it is not a sliding-window algorithm or token bucket.
- Redis down? Requests fail closed with 503; availability is traded for enforcement.
- Client identity? Socket peer by default. Forwarded headers are honored only with TRUST_PROXY=true, which requires a trusted proxy deployment.
- Limits? Single Redis dependency, unauthenticated local demo controls, and simulated overload. This is not a production DDoS mitigation service.

## Timeline and resume consistency
The submitted resume describes the original project's core behavior. These subsequent improvements strengthen the same project. If asked, distinguish the original implementation from later hardening. Practice the demo and inspect the source before describing decisions as your own. Do not claim production deployment, measured business impact, or autonomous agents from this repository.
