package store

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// These tests need a real Postgres: TEST_DATABASE_URL=postgres://... go test ./...
func newTestStore(t *testing.T) *Store {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatal(err)
	}
	cfg.MaxConns = 40
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	s := New(pool)
	if err := s.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	// User ids and idempotency keys are reused across tests, so start clean.
	if _, err := pool.Exec(context.Background(),
		`TRUNCATE seats, user_show_counts, reservations, shows`); err != nil {
		t.Fatal(err)
	}
	return s
}

func seatLabels(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("A%d", i+1)
	}
	return out
}

func mustShow(t *testing.T, s *Store, seats, limit int) ShowState {
	t.Helper()
	st, err := s.CreateShow(context.Background(), t.Name(), seatLabels(seats), 25000, limit)
	if err != nil {
		t.Fatal(err)
	}
	return st
}

func assertInvariant(t *testing.T, s *Store, showID string) ShowState {
	t.Helper()
	st, err := s.GetShow(context.Background(), showID)
	if err != nil {
		t.Fatal(err)
	}
	c := st.Counts
	if c.Available+c.Held+c.Confirmed != c.Total || len(st.Seats) != c.Total {
		t.Fatalf("reconciliation broken: %+v (rows=%d)", c, len(st.Seats))
	}
	return st
}

func TestHotSeatExactlyOneWinner(t *testing.T) {
	s := newTestStore(t)
	show := mustShow(t, s, 10, 4)
	const contenders = 500

	var wins, taken atomic.Int32
	var wg sync.WaitGroup
	for i := range contenders {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, err := s.Reserve(context.Background(), ReserveInput{
				ShowID: show.ID, UserID: fmt.Sprintf("u%d", i),
				IdempotencyKey: fmt.Sprintf("k%d", i), Seats: []string{"A5"},
			})
			var st *SeatTakenError
			switch {
			case err == nil:
				wins.Add(1)
			case errors.As(err, &st):
				taken.Add(1)
			default:
				t.Errorf("unexpected error: %v", err)
			}
		}()
	}
	wg.Wait()
	if wins.Load() != 1 || taken.Load() != contenders-1 {
		t.Fatalf("wins=%d taken=%d", wins.Load(), taken.Load())
	}
	st := assertInvariant(t, s, show.ID)
	if st.Counts.Confirmed != 1 {
		t.Fatalf("confirmed=%d", st.Counts.Confirmed)
	}
}

func TestPerUserLimitUnderConcurrency(t *testing.T) {
	s := newTestStore(t)
	show := mustShow(t, s, 20, 4)

	var wins, limited atomic.Int32
	var wg sync.WaitGroup
	for i := range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, err := s.Reserve(context.Background(), ReserveInput{
				ShowID: show.ID, UserID: "greedy",
				IdempotencyKey: fmt.Sprintf("k%d", i), Seats: []string{fmt.Sprintf("A%d", i+1)},
			})
			switch {
			case err == nil:
				wins.Add(1)
			case errors.Is(err, ErrPerUserLimit):
				limited.Add(1)
			default:
				t.Errorf("unexpected error: %v", err)
			}
		}()
	}
	wg.Wait()
	if wins.Load() != 4 || limited.Load() != 6 {
		t.Fatalf("wins=%d limited=%d", wins.Load(), limited.Load())
	}
	assertInvariant(t, s, show.ID)
}

func TestIdempotentRetriesReserveOnce(t *testing.T) {
	s := newTestStore(t)
	show := mustShow(t, s, 10, 4)
	in := ReserveInput{ShowID: show.ID, UserID: "u1", IdempotencyKey: "same", Seats: []string{"A1", "A2"}}

	ids := make(chan string, 50)
	var fresh atomic.Int32
	var wg sync.WaitGroup
	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, replayed, err := s.Reserve(context.Background(), in)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			if !replayed {
				fresh.Add(1)
			}
			ids <- r.ID
		}()
	}
	wg.Wait()
	close(ids)
	first := ""
	for id := range ids {
		if first == "" {
			first = id
		}
		if id != first {
			t.Fatalf("retries returned different reservations: %s vs %s", first, id)
		}
	}
	if fresh.Load() != 1 {
		t.Fatalf("expected exactly one non-replayed reservation, got %d", fresh.Load())
	}

	// A later sequential retry goes through the lock-free fast path and must
	// still replay, even though the seats now read as taken.
	again, replayed, err := s.Reserve(context.Background(), in)
	if err != nil || !replayed || again.ID != first {
		t.Fatalf("sequential retry: id=%s replayed=%v err=%v", again.ID, replayed, err)
	}

	_, _, err = s.Reserve(context.Background(), ReserveInput{
		ShowID: show.ID, UserID: "u1", IdempotencyKey: "same", Seats: []string{"A3"},
	})
	if !errors.Is(err, ErrIdempotencyMismatch) {
		t.Fatalf("expected mismatch, got %v", err)
	}
	st := assertInvariant(t, s, show.ID)
	if st.Counts.Confirmed != 2 {
		t.Fatalf("confirmed=%d", st.Counts.Confirmed)
	}
}

func TestMultiSeatAllOrNothingNoDeadlock(t *testing.T) {
	s := newTestStore(t)
	show := mustShow(t, s, 6, 4)

	// Overlapping pairs requested by many users; with sorted lock order there
	// must be no deadlock errors and no partially-applied reservations.
	pairs := [][]string{{"A1", "A2"}, {"A2", "A3"}, {"A3", "A4"}, {"A4", "A1"}, {"A5", "A6"}, {"A6", "A1"}}
	owners := make(map[string]string)
	var mu sync.Mutex
	var wg sync.WaitGroup
	for i := range 300 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			seats := append([]string(nil), pairs[i%len(pairs)]...)
			sort.Strings(seats)
			r, _, err := s.Reserve(context.Background(), ReserveInput{
				ShowID: show.ID, UserID: fmt.Sprintf("u%d", i),
				IdempotencyKey: "k", Seats: seats,
			})
			var st *SeatTakenError
			if err != nil && !errors.As(err, &st) {
				t.Errorf("unexpected error: %v", err)
				return
			}
			if err == nil {
				mu.Lock()
				defer mu.Unlock()
				for _, seat := range r.Seats {
					if prev, ok := owners[seat]; ok {
						t.Errorf("seat %s sold twice: %s and %s", seat, prev, r.ID)
					}
					owners[seat] = r.ID
				}
			}
		}()
	}
	wg.Wait()
	st := assertInvariant(t, s, show.ID)
	if st.Counts.Confirmed != len(owners) {
		t.Fatalf("db confirmed=%d, winners own %d", st.Counts.Confirmed, len(owners))
	}
}

func TestCancelOwnerOnlyAndRebookable(t *testing.T) {
	s := newTestStore(t)
	show := mustShow(t, s, 4, 4)
	ctx := context.Background()

	r, _, err := s.Reserve(ctx, ReserveInput{ShowID: show.ID, UserID: "alice", IdempotencyKey: "a", Seats: []string{"A1"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Cancel(ctx, r.ID, "mallory"); !errors.Is(err, ErrNotOwner) {
		t.Fatalf("expected ErrNotOwner, got %v", err)
	}
	if _, changed, err := s.Cancel(ctx, r.ID, "alice"); err != nil || !changed {
		t.Fatalf("cancel: changed=%v err=%v", changed, err)
	}
	// Bob takes the released seat; a repeated cancel by alice must not free it.
	if _, _, err := s.Reserve(ctx, ReserveInput{ShowID: show.ID, UserID: "bob", IdempotencyKey: "b", Seats: []string{"A1"}}); err != nil {
		t.Fatalf("rebook: %v", err)
	}
	if _, changed, err := s.Cancel(ctx, r.ID, "alice"); err != nil || changed {
		t.Fatalf("second cancel: changed=%v err=%v", changed, err)
	}
	st := assertInvariant(t, s, show.ID)
	if st.Seats[0].Status != "confirmed" {
		t.Fatalf("A1 should still be bob's, got %s", st.Seats[0].Status)
	}
	// Alice's counter was released, so she can book up to the limit again.
	for i, seat := range []string{"A2", "A3", "A4"} {
		if _, _, err := s.Reserve(ctx, ReserveInput{ShowID: show.ID, UserID: "alice", IdempotencyKey: fmt.Sprint("x", i), Seats: []string{seat}}); err != nil {
			t.Fatalf("alice %s: %v", seat, err)
		}
	}
}

func TestCancelRacesWithRebook(t *testing.T) {
	s := newTestStore(t)
	show := mustShow(t, s, 2, 4)
	ctx := context.Background()

	for round := range 50 {
		r, _, err := s.Reserve(ctx, ReserveInput{ShowID: show.ID, UserID: "owner", IdempotencyKey: fmt.Sprint("o", round), Seats: []string{"A1"}})
		if err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		var wins atomic.Int32
		var winner Reservation
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, _, err := s.Cancel(ctx, r.ID, "owner"); err != nil {
				t.Errorf("cancel: %v", err)
			}
		}()
		for i := range 20 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				rr, _, err := s.Reserve(ctx, ReserveInput{ShowID: show.ID, UserID: fmt.Sprintf("r%d-%d", round, i), IdempotencyKey: "k", Seats: []string{"A1"}})
				var st *SeatTakenError
				if err == nil {
					wins.Add(1)
					winner = rr
				} else if !errors.As(err, &st) {
					t.Errorf("reserve: %v", err)
				}
			}()
		}
		wg.Wait()
		if wins.Load() > 1 {
			t.Fatalf("round %d: %d winners for one released seat", round, wins.Load())
		}
		assertInvariant(t, s, show.ID)
		if winner.ID != "" {
			// Release so the next round starts clean.
			if _, _, err := s.Cancel(ctx, winner.ID, winner.UserID); err != nil {
				t.Fatal(err)
			}
		}
	}
}

// Fires replays, key conflicts, losers and winners in one concurrent wave so
// they share precheck batches, and checks each gets its own correct answer.
func TestPrecheckBatchMixedOutcomes(t *testing.T) {
	s := newTestStore(t)
	show := mustShow(t, s, 300, 4)
	ctx := context.Background()
	orig, _, err := s.Reserve(ctx, ReserveInput{ShowID: show.ID, UserID: "owner", IdempotencyKey: "k", Seats: []string{"A1"}})
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	for i := range 400 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			switch i % 4 {
			case 0: // replay of the original
				r, replayed, err := s.Reserve(ctx, ReserveInput{ShowID: show.ID, UserID: "owner", IdempotencyKey: "k", Seats: []string{"A1"}})
				if err != nil || !replayed || r.ID != orig.ID {
					t.Errorf("replay: id=%s replayed=%v err=%v", r.ID, replayed, err)
				}
			case 1: // same key, different seat
				_, _, err := s.Reserve(ctx, ReserveInput{ShowID: show.ID, UserID: "owner", IdempotencyKey: "k", Seats: []string{"A2"}})
				if !errors.Is(err, ErrIdempotencyMismatch) {
					t.Errorf("mismatch: %v", err)
				}
			case 2: // someone else wants A1
				_, _, err := s.Reserve(ctx, ReserveInput{ShowID: show.ID, UserID: fmt.Sprint("l", i), IdempotencyKey: "k", Seats: []string{"A1"}})
				var st *SeatTakenError
				if !errors.As(err, &st) || st.Seats[0] != "A1" {
					t.Errorf("loser: %v", err)
				}
			case 3: // a free seat of its own
				seat := fmt.Sprintf("A%d", 100+i/4)
				r, replayed, err := s.Reserve(ctx, ReserveInput{ShowID: show.ID, UserID: fmt.Sprint("w", i), IdempotencyKey: "k", Seats: []string{seat}})
				if err != nil || replayed || r.Seats[0] != seat {
					t.Errorf("winner %s: replayed=%v err=%v", seat, replayed, err)
				}
			}
		}()
	}
	wg.Wait()
	st := assertInvariant(t, s, show.ID)
	if st.Counts.Confirmed != 101 {
		t.Fatalf("confirmed=%d, want 101", st.Counts.Confirmed)
	}
}
