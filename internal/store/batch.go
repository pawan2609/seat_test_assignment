package store

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// precheckBatcher coalesces the fast-path loser check of many concurrent
// requests into one SQL statement.
//
// In an on-sale burst Postgres spends its CPU on per-statement overhead for
// thousands of tiny identical reads, not on the reads themselves. Running
// them as one statement over arrays keeps the exact same semantics (still a
// single snapshot per request, still read-only, still DB-authoritative) at a
// fraction of the cost. No result is cached between batches.
//
// Why a precheck may decline without locks: it never grants a seat, only
// refuses one, and a refusal read from a committed snapshot is a valid answer
// (the seat was taken at that instant). The one trap is a retry whose own
// original request took the seat. The key lookup and the seat read are in
// the same statement, i.e. one snapshot, and a reservation row commits
// atomically with its seat updates: if the snapshot shows the seat taken by
// our original, it also shows our original's reservation row, and we replay.
type precheckBatcher struct {
	pool     *pgxpool.Pool
	reqs     chan *precheckReq
	maxBatch int
	window   time.Duration
	onBatch  func(size int)
}

type precheckReq struct {
	in   ReserveInput
	hash string
	resp chan precheckResult
}

type precheckResult struct {
	res      Reservation
	replayed bool
	err      error
	done     bool
}

func newPrecheckBatcher(pool *pgxpool.Pool, workers, maxBatch int, window time.Duration) *precheckBatcher {
	b := &precheckBatcher{
		pool:     pool,
		reqs:     make(chan *precheckReq, maxBatch*workers),
		maxBatch: maxBatch,
		window:   window,
		onBatch:  func(int) {},
	}
	for range workers {
		go b.loop()
	}
	return b
}

// check returns done=false when the request may win and must go on to the
// transaction; otherwise it carries the final answer.
func (b *precheckBatcher) check(ctx context.Context, in ReserveInput, hash string) precheckResult {
	req := &precheckReq{in: in, hash: hash, resp: make(chan precheckResult, 1)}
	select {
	case b.reqs <- req:
	case <-ctx.Done():
		return precheckResult{err: ctx.Err(), done: true}
	}
	select {
	case r := <-req.resp:
		return r
	case <-ctx.Done():
		return precheckResult{err: ctx.Err(), done: true}
	}
}

func (b *precheckBatcher) loop() {
	batch := make([]*precheckReq, 0, b.maxBatch)
	for first := range b.reqs {
		batch = append(batch[:0], first)
		timer := time.NewTimer(b.window)
	collect:
		for len(batch) < b.maxBatch {
			select {
			case r := <-b.reqs:
				batch = append(batch, r)
			case <-timer.C:
				break collect
			}
		}
		timer.Stop()
		b.onBatch(len(batch))
		b.run(batch)
	}
}

func (b *precheckBatcher) run(batch []*precheckReq) {
	idx := make([]int32, len(batch))
	users := make([]string, len(batch))
	keys := make([]string, len(batch))
	shows := make([]string, len(batch))
	var seatIdx []int32
	var seatLabels []string
	for i, r := range batch {
		idx[i], users[i], keys[i], shows[i] = int32(i), r.in.UserID, r.in.IdempotencyKey, r.in.ShowID
		for _, l := range r.in.Seats {
			seatIdx = append(seatIdx, int32(i))
			seatLabels = append(seatLabels, l)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// One statement, one snapshot: for every request, its idempotency-key
	// row (if any) and which of its seats are not available.
	rows, err := b.pool.Query(ctx, `
		WITH req AS (
			SELECT * FROM unnest($1::int[], $2::text[], $3::text[], $4::uuid[]) AS r(i, user_id, idem_key, show_id)
		), wanted AS (
			SELECT * FROM unnest($5::int[], $6::text[]) AS w(i, label)
		), taken AS (
			SELECT w.i, array_agg(s.label ORDER BY s.label) AS labels
			FROM wanted w
			JOIN req ON req.i = w.i
			JOIN seats s ON s.show_id = req.show_id AND s.label = w.label AND s.status <> 'available'
			GROUP BY w.i
		)
		SELECT req.i, res.id, res.show_id, res.user_id, res.seats, res.amount_paise, res.status,
		       res.created_at, res.cancelled_at, res.request_hash, taken.labels
		FROM req
		LEFT JOIN reservations res ON res.user_id = req.user_id AND res.idempotency_key = req.idem_key
		LEFT JOIN taken ON taken.i = req.i`,
		idx, users, keys, shows, seatIdx, seatLabels)
	if err != nil {
		b.fail(batch, err)
		return
	}
	defer rows.Close()
	answered := make([]bool, len(batch))
	for rows.Next() {
		var i int32
		var row precheckRow
		if err := rows.Scan(append([]any{&i}, row.dest()...)...); err != nil {
			rows.Close()
			b.failUnanswered(batch, answered, err)
			return
		}
		batch[i].resp <- row.decide(batch[i].hash)
		answered[i] = true
	}
	if err := rows.Err(); err != nil {
		b.failUnanswered(batch, answered, err)
		return
	}
	b.failUnanswered(batch, answered, errors.New("precheck batch: request missing from result"))
}

func (b *precheckBatcher) failUnanswered(batch []*precheckReq, answered []bool, err error) {
	for i, r := range batch {
		if !answered[i] {
			r.resp <- precheckResult{err: err, done: true}
		}
	}
}

func (b *precheckBatcher) fail(batch []*precheckReq, err error) {
	b.failUnanswered(batch, make([]bool, len(batch)), err)
}

// precheckRow is one request's view: its idempotency-key reservation (all
// NULL if none) and the requested seats that are not available.
type precheckRow struct {
	id, showID, userID *string
	seats              []string
	amount             *int64
	status, hash       *string
	createdAt          *time.Time
	cancelledAt        *time.Time
	taken              []string
}

func (p *precheckRow) dest() []any {
	return []any{&p.id, &p.showID, &p.userID, &p.seats, &p.amount, &p.status,
		&p.createdAt, &p.cancelledAt, &p.hash, &p.taken}
}

// decide turns a snapshot into an answer. It can replay, reject a reused key,
// or decline a taken seat, but never grant a seat: that only happens in the
// locking transaction.
func (p *precheckRow) decide(hash string) precheckResult {
	switch {
	case p.id != nil && *p.hash != hash:
		return precheckResult{err: ErrIdempotencyMismatch, done: true}
	case p.id != nil:
		return precheckResult{res: Reservation{
			ID: *p.id, ShowID: *p.showID, UserID: *p.userID, Seats: p.seats,
			AmountPaise: *p.amount, Status: *p.status, CreatedAt: *p.createdAt, CancelledAt: p.cancelledAt,
		}, replayed: true, done: true}
	case len(p.taken) > 0:
		return precheckResult{err: &SeatTakenError{Seats: p.taken}, done: true}
	default:
		return precheckResult{}
	}
}
