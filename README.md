# Distributed Rate Limiter
### Redis-backed request limiting with a live cluster dashboard
A Go demonstration of per-IP rate limiting across two application instances, with Redis counters and event streaming through Redis Pub/Sub and WebSockets.

## What I built
- Shared per-IP Redis counters used by both application instances.
- HTTP 429 responses above 10 requests during a counter's one-second lifetime.
- A live dashboard showing requests, blocks, cluster controls, and generated traffic.
- A Docker Compose setup with two Go services and one Redis service.
- A limiter-off mode that simulates overload using HTTP 503 responses.

![Architecture](architecture_dark.png)

## Run the demonstration
Requires Docker with Compose.

```bash
git clone https://github.com/theprideofwolves/Rate-Limiter-Project.git
cd Rate-Limiter-Project
docker compose up --build
```

Open [instance 1](http://localhost:8081) or [instance 2](http://localhost:8082). Both use the shared Redis service.

Use the dashboard to toggle the limiter and generate local traffic. The generator varies source IP headers, so its aggregate traffic is not a repeatable same-IP limit test. The 503 mode is a simulation; it does not crash the application process.

## Request and event flow
`request -> IP extraction -> Redis INCR / EXPIRE -> 200 or 429 -> Pub/Sub -> WebSocket dashboard`

![Request flow](flowchart_dark.png)

| Endpoint | Role |
| --- | --- |
| `/` | Dashboard |
| `/api/test` | Rate-limited demonstration endpoint |
| `/ws` | Dashboard events and control commands |

## Configuration
| Variable | Default |
| --- | --- |
| `INSTANCE_NAME` | `unknown-instance` |
| `REDIS_URL` | `localhost:6379` |

The application listens on container port 8080; Compose maps the instances to host ports 8081 and 8082.

## Current scope
This is an educational prototype. Counter increment and expiry are separate Redis operations, expiry errors are not checked, and IP headers are accepted without a trusted-proxy configuration. WebSocket controls are unauthenticated and accept all origins. The dashboard reset command flushes the selected Redis database; use a dedicated demo Redis instance.

The repository demonstrates coordination and observability, with no production reliability or load-capacity claim.

## Resume bullets
- Built a two-instance Go and Docker Compose demonstration using shared Redis counters to enforce per-IP request limits and return HTTP 429 responses above the configured threshold.
- Streamed cluster events through Redis Pub/Sub and WebSockets to a live dashboard with generated traffic, cluster controls, and simulated overload behavior.
