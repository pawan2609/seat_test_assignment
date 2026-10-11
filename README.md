# Seat Reservation at Scale

A small Go + Postgres service that sells assigned seats for a show and stays correct under an on-sale stampede: no seat sold twice, per-user limits enforced, retries never double-book, and the reconciliation invariant `available + held + confirmed == total_seats` holds at all times.

- **Live URL:** https://13-127-237-241.sslip.io (AWS EC2, ap-south-1; see [Deploying](#deploying))
- **Design write-up:** [WRITEUP.md](WRITEUP.md)
- **Metrics:** `GET /metrics` (Prometheus text format, public)
- **Logs:** `GET /logs?n=200&q=<substring>` (last 5,000 structured log lines, public). Filter by request id with `q=<request_id>`.

## Quick start

```bash
# Run the service and Postgres locally (http://localhost:8080; set APP_PORT to change)
docker compose up --build -d app

# Storm it: hot-seat storm + 20k-request stampede + correctness checks
docker compose run --rm burst
```

Run the concurrency integration tests against real Postgres:

```bash
make test
```

## One-command burst against a live deployment

```bash
./burst.sh https://13-127-237-241.sslip.io <ADMIN_TOKEN>
# or
make burst BASE_URL=https://13-127-237-241.sslip.io ADMIN_TOKEN=<ADMIN_TOKEN>
```

The script uses a local Go toolchain if you have one and falls back to Docker otherwise. It waits for `/readyz` before starting. It runs these phases:

1. **Hot-seat storm:** 5 seats × 500 distinct users, all released at the same instant. 10% of contenders also send a concurrent retry with the same idempotency key. Expect exactly one 201 per seat and 409 `seat_taken` for everyone else.
2. **Stampede:** 20,000 reserve requests from 4,000 users at concurrency 1,000. Traffic is skewed toward 40 "good" seats, 20% of requests ask for two adjacent seats, and 10% are duplicated with the same key. `GET /shows/{id}` is polled every 250 ms to check the invariant during the burst.
3. **Per-user limit:** one user fires 10 parallel reserves on a limit-4 show and must end with exactly 4.
4. **Idempotency:** 20 concurrent copies of one request produce one 201 and 19 replays of the same reservation. The same key with different seats gets a 409.
5. **Identity:** a spoofed `user_id` in the body is ignored. Cancelling another user's reservation gets a 403. A released seat can be rebooked, and a repeated cancel doesn't free someone else's seat.
6. **Reconciliation:** checks the invariant and that every seat any client was sold appears as confirmed on the server, with no seat in two reservations. Also checks that no user is over the limit and that the `/metrics` gauge equals the API count.

It prints the outcome distribution (confirmed / declines by reason / 5xx), latency percentiles and a PASS/FAIL line. It exits non-zero on any violation. Tune it with flags such as `-requests 50000 -concurrency 2000 -storm 1000`; see `go run ./cmd/burst -h`.

Example run (local Docker on a 2-vCPU box):

```
[1] Hot-seat storm: 5 seats x 500 users, all released at once
  ✓ A12: 1 x 201, 1 distinct winning reservations, 549 x 409 seat_taken, 0 other
  ...
[2] Stampede: 20000 reserve requests, 4000 users, concurrency 1000
    22073 requests in 11.061s (1996 req/s)
  ✓ invariant held in all 20 mid-burst polls of GET /shows
...
[6] Final reconciliation
  ✓ available(42) + held(0) + confirmed(958) == total(1000)
  ✓ seats confirmed per server (958) == seats in reservations clients received (958)
  ✓ metrics gauge seatres_seats{status=confirmed} (958) == API confirmed (958)
  ✓ zero 5xx, zero transport errors
PASS: all checks held
```

## API

All bodies are JSON. Money is always integer paise. Every response carries an `X-Request-ID` header; you can send your own to correlate.

| Method & path | Auth | Purpose |
|---|---|---|
| `POST /users` | none | Issue a new user id and bearer token |
| `POST /shows` | admin | Create a show (all seats `available`) |
| `GET /shows/{id}` | none | Per-seat status and counts |
| `POST /shows/{id}/reserve` | user | Reserve seats (all-or-nothing) |
| `GET /reservations/{id}` | owner | Fetch a reservation |
| `POST /reservations/{id}/cancel` | owner | Release a reservation's seats |
| `GET /healthz` | none | Liveness (process is up) |
| `GET /readyz` | none | Readiness (schema applied and DB answers a ping) |
| `GET /metrics` | none | Prometheus metrics |
| `GET /logs` | none | Recent structured logs (`n`, `q` filters) |

### Auth

- **Users:** `POST /users` returns `{"user_id": "u_…", "token": "…"}`. Send the token as `Authorization: Bearer <token>`. The server picks the user id and signs it with HMAC-SHA256. A client can't choose or forge an identity, and any `user_id` in a request body is ignored.
- **Admin:** `Authorization: Bearer <ADMIN_TOKEN>`. In the deployment it's a random secret in the server's `.env` and is supplied with the submission. Locally it's `dev-admin-token`.

### Examples

```bash
B=http://localhost:8080
ADMIN=dev-admin-token

SHOW=$(curl -s -XPOST $B/shows -H "Authorization: Bearer $ADMIN" \
  -d '{"name":"friday-night","seats":["A1","A2","A12","A13"],"price_paise":25000,"per_user_limit":4}' | jq -r .id)

TOKEN=$(curl -s -XPOST $B/users | jq -r .token)

curl -s -XPOST $B/shows/$SHOW/reserve -H "Authorization: Bearer $TOKEN" \
  -H "Idempotency-Key: 7f1c…" -d '{"seats":["A12"]}'
# 201 {"reservation_id":"…","show_id":"…","user_id":"u_…","seats":["A12"],"amount_paise":25000,"status":"confirmed",…}

curl -s $B/shows/$SHOW
# {"id":"…","counts":{"total_seats":4,"available":3,"held":0,"confirmed":1},"seats":[{"label":"A1","status":"available"},…]}
```

The idempotency key can go in the `Idempotency-Key` header or the `idempotency_key` body field. If both are sent they must match.

### Reserve outcomes

| Status | `error` | Meaning |
|---|---|---|
| 201 | none | New reservation confirmed |
| 200 | none | Idempotent replay: the original reservation is returned unchanged, with header `Idempotent-Replayed: true` |
| 409 | `seat_taken` | At least one requested seat is already confirmed. Nothing was reserved, and `seats` lists the taken ones |
| 409 | `per_user_limit` | This would take the user over the show's `per_user_limit` |
| 409 | `idempotency_conflict` | This key was already used for a different request (a different show or set of seats) |
| 422 | `unknown_seat` | A seat label isn't in this show |
| 404 | `show_not_found` | No such show |
| 400 | `invalid_request` | Malformed body, missing key, or duplicate seats |
| 401 | `unauthorized` | Missing or invalid token |
| 503 | `unavailable` | Database unreachable or overloaded. Safe to retry with the same key |

**Multi-seat requests are all-or-nothing.** A request for `["A12","A13"]` where only one is free reserves nothing and returns 409 `seat_taken` listing the taken seat. This holds under concurrency because all requested seats are locked together, in sorted order, inside one transaction.

**Hold model:** reservations are confirmed immediately and released explicitly with `POST /reservations/{id}/cancel` (owner only; a repeat cancel is a no-op). The `held` count is always 0 in this model and is reported for the invariant.

## Observability

Key metrics:

| Metric | Type | What it shows |
|---|---|---|
| `seatres_reservations_confirmed_total` | counter | New reservations |
| `seatres_seats_confirmed_total` | counter | Seats sold |
| `seatres_reservation_declines_total{reason}` | counter | `seat_taken`, `per_user_limit`, `idempotent_replay`, `idempotency_conflict`, `unknown_seat`, `show_not_found`, `invalid_request` |
| `seatres_cancellations_total`, `seatres_seats_released_total` | counter | Releases |
| `seatres_seats{show_id,status}` | gauge | available / held / confirmed / total for the 25 most recent shows, read from the DB at scrape time, so it always equals `GET /shows/{id}` |
| `seatres_http_requests_total{route,code}`, `seatres_http_request_duration_seconds` | counter, histogram | Traffic, errors, latency |
| `seatres_db_up`, `seatres_db_pool_conns{state}`, `seatres_db_pool_empty_acquire_total` | gauge, counter | Dependency health and pool saturation |
| `seatres_precheck_batch_size` | histogram | Requests answered per coalesced fast-path statement |

Counters are per process and reset when the process restarts. The gauges come straight from Postgres.

Logs are one JSON line per request: `request_id`, `route`, `status`, `duration_ms`, `user_id` and `outcome` (`confirmed` / `seat_taken` / …). Server errors also carry the underlying `error`. They go to stdout (`docker compose logs app`) and to `GET /logs`.

```bash
curl -s "$B/logs?n=50&q=seat_taken"
curl -s "$B/logs?q=<request_id>"
```

## Capacity

Measured on the deployment host (EC2 m7i-flex.large: 2 vCPU, 8 GB, app and Postgres on the same box) with `cmd/burst -concurrency 20000 -retries 0`, i.e. the whole stampede released at once and no client retries:

| Burst | Throughput | p99 latency | Result |
|---|---|---|---|
| 5 × 500 hot-seat storm, then 20k requests all at once | ~3,600 req/s | ~4.7 s | All checks pass, zero 5xx, zero transport errors |

Correctness doesn't depend on speed, but zero 5xx does. On a 0.1-CPU instance (a typical PaaS free tier) the same 20k-at-once burst stayed correct, but requests queued past the 60 s server deadline and came back as 503s. That is why the service runs on a full VM.

## Deploying

The live service runs on a single VM with Docker Compose: the same `docker-compose.yml` a clean checkout uses, plus [docker-compose.prod.yml](docker-compose.prod.yml), which adds:

- real secrets from `.env` (git-ignored; see [.env.example](.env.example));
- `restart: unless-stopped` on every container, so the stack comes back after a crash or reboot;
- a named volume for Postgres data, and no host ports except Caddy's;
- [Caddy](Caddyfile) on 80/443, which gets and renews a Let's Encrypt certificate and proxies to the app;
- a larger accept backlog (`somaxconn`), so thousands of simultaneous connections aren't dropped, and capped Docker log files.

```bash
cp .env.example .env    # set TOKEN_SECRET, ADMIN_TOKEN (openssl rand -hex 32), DOMAIN, BIND_IP
docker compose -f docker-compose.yml -f docker-compose.prod.yml up --build -d
curl https://$DOMAIN/readyz
```

The instance's security group must allow inbound 80 and 443. There's no cold start: the stack is always on. After a reboot, Docker starts it again, `/healthz` answers immediately and `/readyz` turns green once Postgres answers and the schema is applied.

## Layout

```
cmd/server        HTTP server entrypoint (config, pool, migrations, graceful shutdown)
cmd/burst         load generator + external correctness checker
internal/store    schema and every transactional decision (reserve, cancel, idempotency)
internal/api      handlers, auth, request-id/access-log middleware
internal/auth     HMAC bearer tokens
internal/obs      Prometheus metrics and the /logs ring buffer
```
