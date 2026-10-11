# Write-up

## 1. The atomic decision

**Mechanism:** one Postgres transaction at READ COMMITTED that takes row locks in a fixed global order. The decision itself is `SELECT … FOR UPDATE` on the seat rows followed by a guarded `UPDATE`. A schema constraint backs it up.

Every seat is exactly one row, `seats(show_id, label)` as the primary key. Ownership lives on that row (`status`, `reservation_id`, `user_id`). A `CHECK` constraint enforces that an available seat has no owner and a confirmed seat always has one. Since there is only one place where "who owns A12" can be written, the question becomes who wins the lock on that row.

`store.Reserve` runs these steps, in this order, in one transaction:

1. **Claim the idempotency key:** `INSERT INTO reservations … ON CONFLICT (user_id, idempotency_key) DO NOTHING RETURNING id`.
2. **Enforce the per-user limit** with a guarded upsert on `user_show_counts`:
   `INSERT … ON CONFLICT DO UPDATE SET seats_held = seats_held + n WHERE seats_held + n <= limit RETURNING`. No row returned means over the limit, so the request is declined with 409.
3. **Lock the requested seats in label order:** `SELECT label, status FROM seats WHERE show_id=$1 AND label = ANY($2) ORDER BY label FOR UPDATE`.
4. **Take them only if all are available:** `UPDATE seats SET status='confirmed', … WHERE … AND status='available'`. The code also checks that the number of updated rows equals the number of locked rows.
5. Commit. Any decline rolls back everything, including the key claim and the counter increment.

**Why it's race-free:** when 500 transactions hit A12, the first to lock the row wins. The other 499 block on the row lock. When the winner commits, Postgres re-reads the newest version of the row for each waiter before handing it the lock, so every waiter sees `status = 'confirmed'` and declines. A waiter never acts on the stale "available" it might have seen earlier. The availability check and the write happen under the same lock, so there is no separate read-then-write. If the application logic were somehow wrong, the `status='available'` guard on the `UPDATE` and the primary key still make a second owner impossible.

**Multi-seat and deadlock avoidance:** multi-seat requests are **all-or-nothing**. Every transaction locks in the same global order:
1. the idempotency key's index entry;
2. the user's counter row;
3. the seat rows, sorted by label (`ORDER BY label` in the locking `SELECT`, with labels also sorted in Go).

`Cancel` follows the same order:
1. the reservation row;
2. the counter row;
3. the seats, sorted by label, via an explicit `ORDER BY … FOR UPDATE` before its `UPDATE`.

A plain multi-row `UPDATE` doesn't promise a lock order, which is why the explicit locking `SELECT` comes first. With one total order no cycle can form, so no deadlock. Deadlock and serialization errors are still retried up to 3 times as insurance. A test fires 300 requests at overlapping seat pairs (A1+A2, A2+A3, …, A6+A1) and gets zero deadlocks and no partial sales.

**Per-user limit:** a `COUNT(*)` over the user's seats followed by an insert would be another read-then-write: two parallel requests would both see 3 and both insert. The counter row turns the limit into a single guarded write, and its row lock makes one user's parallel requests run one at a time while different users never wait on each other.

**Fast path for losers, batched:** most requests in a burst ask for a seat that is already sold. Before any transaction, a read-only check declines them. One statement, run in a single snapshot, returns both the request's idempotency-key reservation (if any) and which requested seats are taken. It is safe without locks:
- It can only decline or replay, never grant a seat.
- A committed snapshot showing a seat taken is a valid answer: the seat really was taken at that moment.
- A retry whose own original took the seat is still replayed correctly. The original's reservation row and its seat updates commit atomically, so any snapshot that shows the seat taken also shows that row.

Profiling under a 0.1 CPU Postgres (with `pg_stat_statements`) showed the database spending its CPU on per-statement overhead for thousands of these tiny reads. So concurrent checks are **coalesced**: a few worker goroutines each collect requests for up to 1 ms (max 256) and answer them all with one statement over arrays (`internal/store/batch.go`). Nothing is cached between batches, so Postgres stays the only source of truth.

Requests that pass then wait for one of a bounded number of transaction slots. If they had to wait, the seat may have sold meanwhile, so they go through the batched check again before opening a write transaction. Slots are fewer than pool connections, so the batcher always has connections and no request holds one while it queues. In the same default burst at 0.1 CPU this raised throughput from ~175 to ~520 req/s, with the hot-seat storm going from 15.6 s to 9–10 s.

**Capacity without 5xx:** the pool is capped at `DB_MAX_CONNS` (20). Extra requests wait in Go for a transaction slot; goroutines are cheap and nothing errors. Each request has a 60 s deadline, kept below the server's write timeout so a slow request gets an answer instead of a dropped connection. `statement_timeout` and `lock_timeout` cap the worst case. If the DB is unreachable or a timeout fires, the API returns **503 with `Retry-After`**, which is honest and safe to retry with the same key. It returns 500 only for genuine bugs.

## 2. Idempotency

- **Where the key is stored:** in the `reservations` table itself, with `UNIQUE (user_id, idempotency_key)` and a `request_hash` column, written in the same transaction as the seat change. "Reservation exists" and "key used" are one fact, so they can't diverge.
- **Scope:** per user, across all shows. The same key string from two users means two different requests.
- **Exactly once:** a concurrent duplicate's `INSERT … ON CONFLICT DO NOTHING` blocks on the uncommitted unique-index entry until the first transaction finishes.
  - If the first commits, the duplicate gets no row back, reads the committed reservation and returns it as a replay.
  - If the first rolls back, the duplicate proceeds as a fresh attempt.
- **Replays** return **200** with the original body and `Idempotent-Replayed: true`. I chose 200 over repeating 201 so that "exactly one 201 per seat" stays literally true for clients and metrics, and the replay is visible as such. It's counted under `declines_total{reason="idempotent_replay"}`.
- **Same key, different body:** the hash covers the show id and the sorted set of seats, so seat order doesn't matter. A different hash returns **409 `idempotency_conflict`**.
- **Declined requests don't consume a key.** A decline changed nothing, so there's nothing to deduplicate. A retry after a decline is evaluated fresh and may succeed if the seat was released in the meantime. This is a deliberate choice; storing declines would instead pin the first answer.

## 3. Holds and expiry

I chose **explicit cancel**: reservations are confirmed at once (matching the spec's 201 body with `status: "confirmed"`) and released with `POST /reservations/{id}/cancel`.
- **Owner only:** another user gets a 403, and identity comes from the token.
- **Repeat cancels** return the cancelled reservation unchanged.
- **No resurrection:** the release is `UPDATE seats … WHERE reservation_id = $this`. A cancel can only free seats this reservation still owns, so after a rebook, a second cancel by the original owner can't touch the new owner's seat. Tests and the burst cover this.
- **Re-bookable:** the same transaction decrements the per-user counter, so released seats and the user's quota both become available again.
- **Racing a rebook:** the cancel takes the same locks in the same order as reserves. A rebook racing the cancel either sees the seat still taken (409) or sees it freed and wins it cleanly. A test runs 50 rounds of one cancel against 20 rebooks and gets at most one winner each round.

With an expiring hold I would add `status='held'` with `expires_at`. Reserves would treat `held AND expires_at < now()` as available inside the same locked check, so expiry needs no background job to be correct. A sweeper would then flip expired holds back for accurate counts, and a confirm step would perform `held → confirmed` guarded on `reservation_id` and `expires_at > now()`.

## 4. Consistency vs availability under a partition

This service is **CP**. Postgres is the only system of record, and no decision is ever made from a cache or local state, apart from the fast path above, which can only decline and reads committed data. If the app can't reach the DB:
- `/readyz` fails, so the load balancer stops routing;
- reserves return 503 instead of guessing.

Selling a seat twice is far worse than being briefly unavailable at on-sale time.

The client-side partition (the request reached us, the response was lost) is handled by idempotency: the client retries with the same key and gets the original reservation, whether or not the first attempt committed.

## 5. Observability: what would page me at 2am

| Alert | Signal | Why |
|---|---|---|
| Invariant broken | `seatres_seats{status="available"} + held + confirmed != total` for any show | Should be impossible; means data corruption or a bug |
| Server errors | `rate(seatres_http_requests_total{code=~"5.."}[5m]) / rate(seatres_http_requests_total[5m]) > 0.5%` | Users getting errors instead of answers |
| Not ready | `/readyz` failing or `seatres_db_up == 0` for 2 minutes | DB unreachable, so nobody can buy |
| Sales stalled | reserve traffic high but `rate(seatres_reservations_confirmed_total) == 0` while seats remain available | Requests are being answered but nothing is selling |
| Latency | p99 of `seatres_http_request_duration_seconds{route="POST /shows/{id}/reserve"}` above 2 s | Users timing out at on-sale |

Warning-only, no page:
- pool saturation: a rising `seatres_db_pool_empty_acquire_total` rate with `acquired == max`;
- a surge in `per_user_limit` declines, which can mean bots.

Every response carries `X-Request-ID`, and the same id is in the JSON log line, so a user complaint can be traced to the exact decision via `GET /logs?q=<id>`.

## 6. What I'd do next

1. **Holds with expiry plus a payment step** (`held → confirmed`), as sketched in §3, so abandoned checkouts free their seats.
2. **A waiting room or admission control per show** at on-sale: cap concurrent reserve transactions per hot show and shed politely with 429, rather than letting one show's stampede take the whole pool.
3. **Fast rejection at the edge:** publish sold seats to a cache or CDN so most losers never reach the app. The DB stays the only place a win is decided.
4. **Multiple instances:** these counters are per process, so aggregate them with `sum()` in Prometheus. The gauges are already DB-derived, and the design has no in-memory state that affects correctness.
5. **Expire idempotency keys** (for example after 24 hours) by moving them to their own table with a TTL sweep, so the unique index doesn't grow forever.
6. **Tracing** (OpenTelemetry spans for lock wait and commit), plus Prometheus alert rules for §5 checked into the repo.
7. **Per-user rate limiting** and abuse controls on `POST /users`.
