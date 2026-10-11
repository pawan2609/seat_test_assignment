// Package store is the system of record. Every reservation decision is made
// inside a single Postgres transaction; see WRITEUP.md for the reasoning.
package store

import (
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed schema.sql
var schemaSQL string

// migrationLockID is an arbitrary constant so concurrent instances don't race
// each other applying the schema.
const migrationLockID = 727_001

var (
	ErrShowNotFound        = errors.New("show not found")
	ErrReservationNotFound = errors.New("reservation not found")
	ErrNotOwner            = errors.New("reservation belongs to another user")
	ErrPerUserLimit        = errors.New("per-user seat limit exceeded")
	ErrIdempotencyMismatch = errors.New("idempotency key reused with a different request")
)

// SeatTakenError lists the requested seats that were already taken.
type SeatTakenError struct{ Seats []string }

func (e *SeatTakenError) Error() string {
	return "seats already taken: " + strings.Join(e.Seats, ",")
}

// UnknownSeatError lists requested seats that don't exist in the show.
type UnknownSeatError struct{ Seats []string }

func (e *UnknownSeatError) Error() string {
	return "unknown seats: " + strings.Join(e.Seats, ",")
}

type Show struct {
	ID           string    `json:"id"`
	Name         string    `json:"name"`
	PricePaise   int64     `json:"price_paise"`
	PerUserLimit int       `json:"per_user_limit"`
	TotalSeats   int       `json:"total_seats"`
	CreatedAt    time.Time `json:"created_at"`
}

type Seat struct {
	Label  string `json:"label"`
	Status string `json:"status"`
}

type SeatCounts struct {
	Total     int `json:"total_seats"`
	Available int `json:"available"`
	Held      int `json:"held"`
	Confirmed int `json:"confirmed"`
}

type ShowState struct {
	Show
	Counts SeatCounts `json:"counts"`
	Seats  []Seat     `json:"seats"`
}

type Reservation struct {
	ID          string     `json:"reservation_id"`
	ShowID      string     `json:"show_id"`
	UserID      string     `json:"user_id"`
	Seats       []string   `json:"seats"`
	AmountPaise int64      `json:"amount_paise"`
	Status      string     `json:"status"`
	CreatedAt   time.Time  `json:"created_at"`
	CancelledAt *time.Time `json:"cancelled_at,omitempty"`
}

type Store struct {
	pool     *pgxpool.Pool
	shows    sync.Map // show id -> showMeta
	batcher  *precheckBatcher
	txnSlots chan struct{} // bounds concurrent reserve transactions
}

// Precheck batching: a few workers, each running one coalesced statement at a
// time, so at most precheckWorkers pool connections are used for it.
const (
	precheckWorkers  = 4
	precheckMaxBatch = 256
	precheckWindow   = time.Millisecond
)

func New(pool *pgxpool.Pool) *Store {
	// Leave connections for the batcher plus a couple for reads, cancels and
	// health checks, so the reserve path can't starve them.
	slots := max(1, int(pool.Config().MaxConns)-precheckWorkers-2)
	return &Store{
		pool:     pool,
		batcher:  newPrecheckBatcher(pool, precheckWorkers, precheckMaxBatch, precheckWindow),
		txnSlots: make(chan struct{}, slots),
	}
}

// OnPrecheckBatch registers a callback with each coalesced batch's size (for
// metrics). Call before serving traffic.
func (s *Store) OnPrecheckBatch(f func(size int)) { s.batcher.onBatch = f }

func (s *Store) Pool() *pgxpool.Pool { return s.pool }

// Migrate applies the schema. Safe to run from several instances at once.
func (s *Store) Migrate(ctx context.Context) error {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, migrationLockID); err != nil {
		return err
	}
	defer conn.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, migrationLockID)
	_, err = conn.Exec(ctx, schemaSQL)
	return err
}

func (s *Store) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

// CreateShow inserts the show and all of its seats in one transaction.
// Seat labels must already be validated as unique and non-empty.
func (s *Store) CreateShow(ctx context.Context, name string, seats []string, pricePaise int64, perUserLimit int) (ShowState, error) {
	var st ShowState
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		err := tx.QueryRow(ctx, `
			INSERT INTO shows (name, price_paise, per_user_limit, total_seats)
			VALUES ($1, $2, $3, $4)
			RETURNING id, name, price_paise, per_user_limit, total_seats, created_at`,
			name, pricePaise, perUserLimit, len(seats),
		).Scan(&st.ID, &st.Name, &st.PricePaise, &st.PerUserLimit, &st.TotalSeats, &st.CreatedAt)
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `
			INSERT INTO seats (show_id, label, position)
			SELECT $1, t.label, t.ord
			FROM unnest($2::text[]) WITH ORDINALITY AS t(label, ord)`,
			st.ID, seats)
		return err
	})
	if err != nil {
		return ShowState{}, err
	}
	st.Seats = make([]Seat, len(seats))
	for i, l := range seats {
		st.Seats[i] = Seat{Label: l, Status: "available"}
	}
	st.Counts = SeatCounts{Total: len(seats), Available: len(seats)}
	return st, nil
}

// GetShow returns per-seat state. The counts are derived from a single
// statement (one snapshot), so available+held+confirmed == total always holds
// in the response.
func (s *Store) GetShow(ctx context.Context, showID string) (ShowState, error) {
	var st ShowState
	err := s.pool.QueryRow(ctx, `
		SELECT id, name, price_paise, per_user_limit, total_seats, created_at
		FROM shows WHERE id = $1`, showID,
	).Scan(&st.ID, &st.Name, &st.PricePaise, &st.PerUserLimit, &st.TotalSeats, &st.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return ShowState{}, ErrShowNotFound
	}
	if err != nil {
		return ShowState{}, err
	}

	rows, err := s.pool.Query(ctx, `
		SELECT label, status FROM seats WHERE show_id = $1 ORDER BY position`, showID)
	if err != nil {
		return ShowState{}, err
	}
	st.Seats, err = pgx.CollectRows(rows, pgx.RowToStructByPos[Seat])
	if err != nil {
		return ShowState{}, err
	}
	st.Counts.Total = st.TotalSeats
	for _, seat := range st.Seats {
		switch seat.Status {
		case "available":
			st.Counts.Available++
		case "held":
			st.Counts.Held++
		case "confirmed":
			st.Counts.Confirmed++
		}
	}
	return st, nil
}

type ReserveInput struct {
	ShowID         string
	UserID         string
	IdempotencyKey string
	// Seats must be de-duplicated and sorted; the sort order is the lock order.
	Seats []string
}

// RequestHash fingerprints the parts of a reserve request that define "the
// same request" for idempotency purposes.
func RequestHash(showID string, sortedSeats []string) string {
	h := sha256.New()
	h.Write([]byte(showID))
	for _, seat := range sortedSeats {
		h.Write([]byte{0})
		h.Write([]byte(seat))
	}
	return hex.EncodeToString(h.Sum(nil))
}

// Reserve atomically confirms all requested seats for the user, or none.
//
// Within one transaction, and always in this lock order:
//  1. Insert the reservation row. The unique (user_id, idempotency_key)
//     constraint makes a concurrent duplicate wait for us, then see our row.
//  2. Guarded upsert on the user's per-show counter (enforces the limit and
//     serialises one user's parallel requests).
//  3. SELECT ... ORDER BY label FOR UPDATE on the requested seats, so
//     multi-seat requests lock rows in one global order (no deadlocks).
//  4. Flip them to confirmed only if every one is still available.
//
// It returns replayed=true when the idempotency key already produced a
// reservation for this exact request.
//
// Before the transaction, a lock-free fast path answers the common losing
// case (seat already sold) with reads only; concurrent checks are coalesced
// into one statement by precheckBatcher.
//
// Requests that pass then wait for a transaction slot. If they had to wait,
// the seat may have sold meanwhile, so they go through the batched check
// again before opening a write transaction. Slots are fewer than pool
// connections, so the batcher always has connections of its own and no
// request holds a connection while it queues.
func (s *Store) Reserve(ctx context.Context, in ReserveInput) (res Reservation, replayed bool, err error) {
	hash := RequestHash(in.ShowID, in.Seats)
	if r := s.batcher.check(ctx, in, hash); r.done {
		return r.res, r.replayed, r.err
	}

	waitStart := time.Now()
	select {
	case s.txnSlots <- struct{}{}:
	case <-ctx.Done():
		return Reservation{}, false, ctx.Err()
	}
	defer func() { <-s.txnSlots }()
	if time.Since(waitStart) > 2*time.Millisecond {
		if r := s.batcher.check(ctx, in, hash); r.done {
			return r.res, r.replayed, r.err
		}
	}

	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return Reservation{}, false, err
	}
	defer conn.Release()
	for attempt := 1; ; attempt++ {
		res, replayed, err = s.reserveOnce(ctx, conn, in, hash)
		if attempt < 3 && isRetryable(err) {
			continue
		}
		return res, replayed, err
	}
}

type showMeta struct {
	price int64
	limit int
}

// showMeta returns a show's price and limit. Both are immutable after
// creation, so they're cached for the life of the process.
func (s *Store) showMeta(ctx context.Context, q querier, showID string) (showMeta, error) {
	if m, ok := s.shows.Load(showID); ok {
		return m.(showMeta), nil
	}
	var m showMeta
	err := q.QueryRow(ctx, `SELECT price_paise, per_user_limit FROM shows WHERE id = $1`, showID).
		Scan(&m.price, &m.limit)
	if errors.Is(err, pgx.ErrNoRows) {
		return showMeta{}, ErrShowNotFound
	}
	if err != nil {
		return showMeta{}, err
	}
	s.shows.Store(showID, m)
	return m, nil
}

func (s *Store) reserveOnce(ctx context.Context, conn *pgxpool.Conn, in ReserveInput, hash string) (Reservation, bool, error) {
	meta, err := s.showMeta(ctx, conn, in.ShowID)
	if err != nil {
		return Reservation{}, false, err
	}
	price, limit := meta.price, meta.limit

	tx, err := conn.Begin(ctx)
	if err != nil {
		return Reservation{}, false, err
	}
	defer tx.Rollback(ctx)

	res := Reservation{
		ShowID:      in.ShowID,
		UserID:      in.UserID,
		Seats:       in.Seats,
		AmountPaise: price * int64(len(in.Seats)),
		Status:      "confirmed",
	}

	// 1. Idempotency: claim the key. If another transaction is inserting the
	// same key right now, Postgres blocks this insert until that one commits
	// or rolls back.
	err = tx.QueryRow(ctx, `
		INSERT INTO reservations (show_id, user_id, idempotency_key, request_hash, seats, amount_paise, status)
		VALUES ($1, $2, $3, $4, $5, $6, 'confirmed')
		ON CONFLICT (user_id, idempotency_key) DO NOTHING
		RETURNING id, created_at`,
		in.ShowID, in.UserID, in.IdempotencyKey, hash, in.Seats, res.AmountPaise,
	).Scan(&res.ID, &res.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		existing, existingHash, err := s.reservationByKey(ctx, tx, in.UserID, in.IdempotencyKey)
		if err != nil {
			return Reservation{}, false, err
		}
		if existingHash != hash {
			return Reservation{}, false, ErrIdempotencyMismatch
		}
		return existing, true, nil
	}
	if err != nil {
		return Reservation{}, false, err
	}

	// 2. Per-user limit. The upsert row-locks the counter; the WHERE makes it
	// a no-op (no row returned) when the user would go over the limit.
	if len(in.Seats) > limit {
		return Reservation{}, false, ErrPerUserLimit
	}
	var held int
	err = tx.QueryRow(ctx, `
		INSERT INTO user_show_counts AS c (show_id, user_id, seats_held)
		VALUES ($1, $2, $3)
		ON CONFLICT (show_id, user_id) DO UPDATE
			SET seats_held = c.seats_held + EXCLUDED.seats_held
			WHERE c.seats_held + EXCLUDED.seats_held <= $4
		RETURNING seats_held`,
		in.ShowID, in.UserID, len(in.Seats), limit,
	).Scan(&held)
	if errors.Is(err, pgx.ErrNoRows) {
		return Reservation{}, false, ErrPerUserLimit
	}
	if err != nil {
		return Reservation{}, false, err
	}

	// 3. Lock the requested seats in label order. FOR UPDATE re-reads the
	// latest committed version once the lock is granted, so a loser of the
	// race sees status='confirmed' here, never a stale 'available'.
	rows, err := tx.Query(ctx, `
		SELECT label, status FROM seats
		WHERE show_id = $1 AND label = ANY($2)
		ORDER BY label
		FOR UPDATE`, in.ShowID, in.Seats)
	if err != nil {
		return Reservation{}, false, err
	}
	locked, err := pgx.CollectRows(rows, pgx.RowToStructByPos[Seat])
	if err != nil {
		return Reservation{}, false, err
	}
	if len(locked) != len(in.Seats) {
		return Reservation{}, false, &UnknownSeatError{Seats: missing(in.Seats, locked)}
	}
	var taken []string
	for _, seat := range locked {
		if seat.Status != "available" {
			taken = append(taken, seat.Label)
		}
	}
	if len(taken) > 0 {
		return Reservation{}, false, &SeatTakenError{Seats: taken}
	}

	// 4. Take them. The status guard is redundant under the lock, but it means
	// this statement alone could never overwrite someone else's seat.
	tag, err := tx.Exec(ctx, `
		UPDATE seats
		SET status = 'confirmed', reservation_id = $3, user_id = $4, updated_at = now()
		WHERE show_id = $1 AND label = ANY($2) AND status = 'available'`,
		in.ShowID, in.Seats, res.ID, in.UserID)
	if err != nil {
		return Reservation{}, false, err
	}
	if int(tag.RowsAffected()) != len(in.Seats) {
		return Reservation{}, false, fmt.Errorf("invariant violated: updated %d of %d locked seats", tag.RowsAffected(), len(in.Seats))
	}

	if err := tx.Commit(ctx); err != nil {
		return Reservation{}, false, err
	}
	return res, false, nil
}

type querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

func (s *Store) reservationByKey(ctx context.Context, q querier, userID, key string) (Reservation, string, error) {
	var r Reservation
	var hash string
	err := q.QueryRow(ctx, `
		SELECT id, show_id, user_id, seats, amount_paise, status, created_at, cancelled_at, request_hash
		FROM reservations WHERE user_id = $1 AND idempotency_key = $2`, userID, key,
	).Scan(&r.ID, &r.ShowID, &r.UserID, &r.Seats, &r.AmountPaise, &r.Status, &r.CreatedAt, &r.CancelledAt, &hash)
	return r, hash, err
}

// GetReservation returns a reservation only to its owner.
func (s *Store) GetReservation(ctx context.Context, id, userID string) (Reservation, error) {
	var r Reservation
	err := s.pool.QueryRow(ctx, `
		SELECT id, show_id, user_id, seats, amount_paise, status, created_at, cancelled_at
		FROM reservations WHERE id = $1`, id,
	).Scan(&r.ID, &r.ShowID, &r.UserID, &r.Seats, &r.AmountPaise, &r.Status, &r.CreatedAt, &r.CancelledAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Reservation{}, ErrReservationNotFound
	}
	if err != nil {
		return Reservation{}, err
	}
	if r.UserID != userID {
		return Reservation{}, ErrNotOwner
	}
	return r, nil
}

// Cancel releases a reservation's seats. Only the owner may cancel, and a
// second cancel is a no-op that returns the cancelled reservation.
//
// Lock order matches Reserve (counter, then seats in label order), and the
// seat update is guarded on reservation_id, so a cancel can only ever free
// seats that this reservation actually owns.
func (s *Store) Cancel(ctx context.Context, id, userID string) (res Reservation, changed bool, err error) {
	for attempt := 1; ; attempt++ {
		res, changed, err = s.cancelOnce(ctx, id, userID)
		if attempt < 3 && isRetryable(err) {
			continue
		}
		return res, changed, err
	}
}

func (s *Store) cancelOnce(ctx context.Context, id, userID string) (Reservation, bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Reservation{}, false, err
	}
	defer tx.Rollback(ctx)

	var r Reservation
	err = tx.QueryRow(ctx, `
		SELECT id, show_id, user_id, seats, amount_paise, status, created_at, cancelled_at
		FROM reservations WHERE id = $1 FOR UPDATE`, id,
	).Scan(&r.ID, &r.ShowID, &r.UserID, &r.Seats, &r.AmountPaise, &r.Status, &r.CreatedAt, &r.CancelledAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Reservation{}, false, ErrReservationNotFound
	}
	if err != nil {
		return Reservation{}, false, err
	}
	if r.UserID != userID {
		return Reservation{}, false, ErrNotOwner
	}
	if r.Status == "cancelled" {
		return r, false, nil
	}

	if _, err := tx.Exec(ctx, `
		UPDATE user_show_counts SET seats_held = seats_held - $3
		WHERE show_id = $1 AND user_id = $2`, r.ShowID, r.UserID, len(r.Seats)); err != nil {
		return Reservation{}, false, err
	}
	if _, err := tx.Exec(ctx, `
		SELECT 1 FROM seats WHERE show_id = $1 AND label = ANY($2)
		ORDER BY label FOR UPDATE`, r.ShowID, r.Seats); err != nil {
		return Reservation{}, false, err
	}
	tag, err := tx.Exec(ctx, `
		UPDATE seats
		SET status = 'available', reservation_id = NULL, user_id = NULL, updated_at = now()
		WHERE show_id = $1 AND reservation_id = $2`, r.ShowID, r.ID)
	if err != nil {
		return Reservation{}, false, err
	}
	if int(tag.RowsAffected()) != len(r.Seats) {
		return Reservation{}, false, fmt.Errorf("invariant violated: reservation %s owned %d of %d seats", r.ID, tag.RowsAffected(), len(r.Seats))
	}
	err = tx.QueryRow(ctx, `
		UPDATE reservations SET status = 'cancelled', cancelled_at = now()
		WHERE id = $1 RETURNING status, cancelled_at`, r.ID,
	).Scan(&r.Status, &r.CancelledAt)
	if err != nil {
		return Reservation{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Reservation{}, false, err
	}
	return r, true, nil
}

// SeatCountsByShow reports seat counts for the most recent shows, for the
// metrics gauge. Read straight from the DB so it always matches the API.
func (s *Store) SeatCountsByShow(ctx context.Context, recentShows int) (map[string]SeatCounts, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT s.show_id::text, s.status, count(*)
		FROM seats s
		WHERE s.show_id IN (SELECT id FROM shows ORDER BY created_at DESC LIMIT $1)
		GROUP BY 1, 2`, recentShows)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]SeatCounts{}
	for rows.Next() {
		var showID, status string
		var n int
		if err := rows.Scan(&showID, &status, &n); err != nil {
			return nil, err
		}
		c := out[showID]
		c.Total += n
		switch status {
		case "available":
			c.Available += n
		case "held":
			c.Held += n
		case "confirmed":
			c.Confirmed += n
		}
		out[showID] = c
	}
	return out, rows.Err()
}

func missing(want []string, got []Seat) []string {
	have := make(map[string]bool, len(got))
	for _, s := range got {
		have[s.Label] = true
	}
	var out []string
	for _, w := range want {
		if !have[w] {
			out = append(out, w)
		}
	}
	return out
}

// isRetryable reports deadlock / serialization failures. Our lock ordering
// should make these impossible, but retrying is cheap insurance.
func isRetryable(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == "40P01" || pgErr.Code == "40001"
	}
	return false
}
