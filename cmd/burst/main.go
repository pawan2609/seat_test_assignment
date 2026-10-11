// Command burst reproduces an on-sale stampede against a running service and
// checks every correctness rule from the outside.
//
//	go run ./cmd/burst -admin <ADMIN_TOKEN> https://<service>
//
// Exit status is non-zero if any invariant is violated or any 5xx is seen.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"os"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	adminToken  = flag.String("admin", os.Getenv("ADMIN_TOKEN"), "admin token (or ADMIN_TOKEN env)")
	numSeats    = flag.Int("seats", 1000, "seats in the show")
	hotSeats    = flag.Int("hot", 5, "number of hot seats stormed in phase 1")
	stormSize   = flag.Int("storm", 500, "distinct users fighting over each hot seat")
	stampede    = flag.Int("requests", 20000, "reserve requests in the phase 2 stampede")
	numUsers    = flag.Int("users", 4000, "users taking part in the stampede")
	concurrency = flag.Int("concurrency", 1000, "max in-flight requests in phase 2")
	retryFrac   = flag.Float64("retry", 0.1, "fraction of stampede requests duplicated with the same idempotency key")
	timeout     = flag.Duration("timeout", 60*time.Second, "per-request timeout")
	retries     = flag.Int("retries", 3, "retries (same idempotency key) after a timeout or 5xx, like a real client")
)

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: burst [flags] <BASE_URL>\n")
		flag.PrintDefaults()
	}
	flag.Parse()
	if flag.NArg() != 1 || *adminToken == "" {
		flag.Usage()
		os.Exit(2)
	}
	b := newBurst(strings.TrimRight(flag.Arg(0), "/"))
	if err := b.run(); err != nil {
		fmt.Fprintf(os.Stderr, "\nFAIL: %v\n", err)
		os.Exit(1)
	}
}

// ---- HTTP plumbing ----

type result struct {
	phase   string
	status  int // 0 = transport error
	errCode string
	resID   string
	seats   []string
	user    string
	latency time.Duration
	err     error
	// firstFail describes a failed first attempt (timeout/transport error or
	// 5xx) that was then retried with the same key; "" if the first attempt
	// got a real answer.
	firstFail string
}

type reservation struct {
	ID     string   `json:"reservation_id"`
	UserID string   `json:"user_id"`
	Seats  []string `json:"seats"`
	Amount int64    `json:"amount_paise"`
	Status string   `json:"status"`
}

type user struct{ id, token string }

type burst struct {
	base    string
	client  *http.Client
	results []result
	mu      sync.Mutex
	fails   []string
}

func newBurst(base string) *burst {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.MaxIdleConns = 4096
	tr.MaxIdleConnsPerHost = 4096
	tr.MaxConnsPerHost = 0
	return &burst{base: base, client: &http.Client{Transport: tr, Timeout: *timeout}}
}

func (b *burst) do(method, path, token string, body any, out any) (int, []byte, error) {
	var rd io.Reader
	if body != nil {
		buf, _ := json.Marshal(body)
		rd = bytes.NewReader(buf)
	}
	req, _ := http.NewRequestWithContext(context.Background(), method, b.base+path, rd)
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := b.client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if out != nil && resp.StatusCode < 300 {
		if err := json.Unmarshal(raw, out); err != nil {
			return resp.StatusCode, raw, fmt.Errorf("decode: %w", err)
		}
	}
	return resp.StatusCode, raw, nil
}

func (b *burst) reserve(phase, showID string, u user, seats []string, key string, extra map[string]any) result {
	body := map[string]any{"seats": seats, "idempotency_key": key}
	for k, v := range extra {
		body[k] = v
	}
	start := time.Now()
	var r reservation
	var status int
	var raw []byte
	var err error
	firstFail := ""
	for attempt := 0; ; attempt++ {
		status, raw, err = b.do("POST", "/shows/"+showID+"/reserve", u.token, body, &r)
		failed := err != nil || status >= 500
		if !failed || attempt >= *retries {
			break
		}
		if attempt == 0 {
			if err != nil {
				firstFail = "transport: " + trimURL(err.Error())
			} else {
				firstFail = "http " + strconv.Itoa(status)
			}
		}
		time.Sleep(time.Duration(attempt+1) * 500 * time.Millisecond)
	}
	res := result{phase: phase, status: status, user: u.id, latency: time.Since(start), err: err, firstFail: firstFail}
	if status == 200 || status == 201 {
		res.resID, res.seats = r.ID, r.Seats
		if r.UserID != u.id {
			b.fail("reservation %s returned user %s, token user is %s", r.ID, r.UserID, u.id)
		}
	} else if status > 0 {
		var e struct {
			Error string `json:"error"`
		}
		json.Unmarshal(raw, &e)
		res.errCode = e.Error
	}
	b.mu.Lock()
	b.results = append(b.results, res)
	b.mu.Unlock()
	return res
}

func (b *burst) fail(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	b.mu.Lock()
	b.fails = append(b.fails, msg)
	b.mu.Unlock()
	fmt.Printf("  ✗ %s\n", msg)
}

func (b *burst) check(ok bool, format string, args ...any) {
	if ok {
		fmt.Printf("  ✓ %s\n", fmt.Sprintf(format, args...))
	} else {
		b.fail(format, args...)
	}
}

type showState struct {
	ID           string `json:"id"`
	PerUserLimit int    `json:"per_user_limit"`
	Counts       struct {
		Total     int `json:"total_seats"`
		Available int `json:"available"`
		Held      int `json:"held"`
		Confirmed int `json:"confirmed"`
	} `json:"counts"`
	Seats []struct {
		Label  string `json:"label"`
		Status string `json:"status"`
	} `json:"seats"`
}

func (b *burst) getShow(id string) (showState, error) {
	var st showState
	status, raw, err := b.do("GET", "/shows/"+id, "", nil, &st)
	if err == nil && status != 200 {
		err = fmt.Errorf("GET /shows/%s: %d %s", id, status, raw)
	}
	return st, err
}

func (b *burst) newUsers(n int) []user {
	users := make([]user, n)
	sem := make(chan struct{}, 200)
	var wg sync.WaitGroup
	for i := range users {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			var u struct{ UserID, Token string }
			for attempt := 0; attempt < 5; attempt++ {
				var raw struct {
					UserID string `json:"user_id"`
					Token  string `json:"token"`
				}
				if status, _, err := b.do("POST", "/users", "", nil, &raw); err == nil && status == 201 {
					u.UserID, u.Token = raw.UserID, raw.Token
					break
				}
				time.Sleep(200 * time.Millisecond)
			}
			users[i] = user{id: u.UserID, token: u.Token}
		}()
	}
	wg.Wait()
	for _, u := range users {
		if u.token == "" {
			fmt.Fprintln(os.Stderr, "could not register users")
			os.Exit(1)
		}
	}
	return users
}

func seatLabel(i int) string {
	// 50 seats per row: A1..A50, B1..B50, ...
	row := ""
	for n := i / 50; ; n = n/26 - 1 {
		row = string(rune('A'+n%26)) + row
		if n < 26 {
			break
		}
	}
	return row + strconv.Itoa(i%50+1)
}

// ---- the run ----

func (b *burst) run() error {
	fmt.Printf("Target: %s\n\n", b.base)

	// Cold start: wait for readiness rather than failing on a sleeping instance.
	fmt.Print("Waiting for /readyz ")
	deadline := time.Now().Add(3 * time.Minute)
	for {
		status, _, _ := b.do("GET", "/readyz", "", nil, nil)
		if status == 200 {
			fmt.Println("ready")
			break
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("service not ready after 3 minutes (last status %d)", status)
		}
		fmt.Print(".")
		time.Sleep(2 * time.Second)
	}
	before := b.scrape()

	// Seats: 0..n-51 for the stampede; the last 50 are kept clean for the
	// deterministic checks in phases 3-5. Hot seats start at A12.
	labels := make([]string, *numSeats)
	for i := range labels {
		labels[i] = seatLabel(i)
	}
	var show showState
	status, raw, err := b.do("POST", "/shows", *adminToken,
		map[string]any{"name": fmt.Sprintf("burst-%d", time.Now().Unix()), "seats": labels, "price_paise": 25000, "per_user_limit": 4}, &show)
	if err != nil || status != 201 {
		return fmt.Errorf("create show: %d %s %v", status, raw, err)
	}
	fmt.Printf("Show %s: %d seats, per_user_limit=%d\n", show.ID, *numSeats, show.PerUserLimit)
	stampedeSeats := labels[:len(labels)-50]
	reserved := labels[len(labels)-50:]
	hot := labels[11 : 11+*hotSeats]

	fmt.Printf("Registering %d users...\n", *numUsers+*hotSeats**stormSize)
	stormUsers := b.newUsers(*hotSeats * *stormSize)
	users := b.newUsers(*numUsers)

	// Poll the reconciliation invariant throughout phases 1-2.
	stopPoll := make(chan struct{})
	var polls, pollBad atomic.Int64
	var pollWG sync.WaitGroup
	pollWG.Add(1)
	go func() {
		defer pollWG.Done()
		for {
			select {
			case <-stopPoll:
				return
			case <-time.After(250 * time.Millisecond):
			}
			st, err := b.getShow(show.ID)
			if err != nil {
				continue
			}
			polls.Add(1)
			c := st.Counts
			if c.Available+c.Held+c.Confirmed != c.Total || c.Total != *numSeats {
				pollBad.Add(1)
				b.fail("mid-burst invariant broken: %+v", c)
			}
		}
	}()

	// Phase 1: hot-seat storm. Every contender is released at the same instant;
	// 10% of them also fire a concurrent retry with the same key.
	fmt.Printf("\n[1] Hot-seat storm: %d seats x %d users, all released at once\n", *hotSeats, *stormSize)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for h, seat := range hot {
		for i := range *stormSize {
			u := stormUsers[h**stormSize+i]
			key := fmt.Sprintf("storm-%s-%d", seat, i)
			copies := 1
			if i%10 == 0 {
				copies = 2
			}
			for range copies {
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-start
					b.reserve("storm:"+seat, show.ID, u, []string{seat}, key, nil)
				}()
			}
		}
	}
	t0 := time.Now()
	close(start)
	wg.Wait()
	fmt.Printf("    %d requests in %s\n", b.count("storm:"), time.Since(t0).Round(time.Millisecond))
	for _, seat := range hot {
		winners := map[string]bool{}
		var created, taken, other int
		for _, r := range b.phase("storm:" + seat) {
			switch {
			case r.status == 201:
				created++
				winners[r.resID] = true
			case r.status == 200:
				winners[r.resID] = true
			case r.status == 409 && r.errCode == "seat_taken":
				taken++
			default:
				other++
			}
		}
		b.check(created == 1 && len(winners) == 1 && other == 0,
			"%s: %d x 201, %d distinct winning reservations, %d x 409 seat_taken, %d other", seat, created, len(winners), taken, other)
	}

	// Phase 2: stampede. Skewed toward a small set of "good" seats; some
	// requests are for two adjacent seats; some are duplicated concurrently.
	fmt.Printf("\n[2] Stampede: %d reserve requests, %d users, concurrency %d\n", *stampede, *numUsers, *concurrency)
	good := stampedeSeats[:min(40, len(stampedeSeats))]
	sem := make(chan struct{}, *concurrency)
	t0 = time.Now()
	for i := range *stampede {
		u := users[rand.IntN(len(users))]
		var seats []string
		if rand.Float64() < 0.6 {
			seats = []string{good[rand.IntN(len(good))]}
		} else {
			seats = []string{stampedeSeats[rand.IntN(len(stampedeSeats))]}
		}
		if rand.Float64() < 0.2 {
			idx := slices.Index(stampedeSeats, seats[0])
			if idx+1 < len(stampedeSeats) {
				seats = append(seats, stampedeSeats[idx+1])
			}
		}
		key := fmt.Sprintf("stampede-%d", i)
		copies := 1
		if rand.Float64() < *retryFrac {
			copies = 2
		}
		for range copies {
			wg.Add(1)
			sem <- struct{}{}
			go func() {
				defer wg.Done()
				defer func() { <-sem }()
				b.reserve("stampede", show.ID, u, seats, key, nil)
			}()
		}
	}
	wg.Wait()
	elapsed := time.Since(t0)
	n := b.count("stampede")
	fmt.Printf("    %d requests in %s (%.0f req/s)\n", n, elapsed.Round(time.Millisecond), float64(n)/elapsed.Seconds())
	close(stopPoll)
	pollWG.Wait()
	b.check(pollBad.Load() == 0 && polls.Load() > 0, "invariant held in all %d mid-burst polls of GET /shows", polls.Load())

	// Phase 3: per-user limit under concurrency.
	fmt.Println("\n[3] Per-user limit: one user fires 10 parallel reserves (limit 4)")
	greedy := b.newUsers(1)[0]
	for i := range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			b.reserve("limit", show.ID, greedy, []string{reserved[i]}, fmt.Sprintf("greedy-%d", i), nil)
		}()
	}
	wg.Wait()
	var won, limited int
	for _, r := range b.phase("limit") {
		if r.status == 201 {
			won++
		} else if r.errCode == "per_user_limit" {
			limited++
		}
	}
	b.check(won == 4 && limited == 6, "%d confirmed, %d declined per_user_limit", won, limited)

	// Phase 4: idempotency.
	fmt.Println("\n[4] Idempotency: 20 concurrent retries of one request, then same key / different seat")
	idem := b.newUsers(1)[0]
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			b.reserve("idem", show.ID, idem, []string{reserved[20], reserved[21]}, "idem-key", nil)
		}()
	}
	wg.Wait()
	ids := map[string]bool{}
	created := 0
	for _, r := range b.phase("idem") {
		if r.status == 201 {
			created++
		}
		if r.status == 200 || r.status == 201 {
			ids[r.resID] = true
		}
	}
	b.check(created == 1 && len(ids) == 1 && len(b.phase("idem")) == 20,
		"one 201, %d replays, all with the same reservation id", 20-created)
	r := b.reserve("idem-diff", show.ID, idem, []string{reserved[22]}, "idem-key", nil)
	b.check(r.status == 409 && r.errCode == "idempotency_conflict", "same key + different seats -> %d %s", r.status, r.errCode)

	// Phase 5: identity is token-derived; cancel is owner-only; release never
	// resurrects someone else's seat.
	fmt.Println("\n[5] Identity & cancel")
	trio := b.newUsers(3)
	alice, mallory, carol := trio[0], trio[1], trio[2]
	ra := b.reserve("ident", show.ID, alice, []string{reserved[30]}, "a", nil)
	rm := b.reserve("ident", show.ID, mallory, []string{reserved[31]}, "m", map[string]any{"user_id": alice.id})
	b.check(rm.status == 201 && rm.user == mallory.id, "body user_id spoof ignored: reservation belongs to the token's user")
	status, _, _ = b.do("POST", "/reservations/"+ra.resID+"/cancel", mallory.token, nil, nil)
	b.check(status == 403, "another user cancelling alice's hold -> %d", status)
	status, _, _ = b.do("POST", "/reservations/"+ra.resID+"/cancel", alice.token, nil, nil)
	b.check(status == 200, "owner cancel -> %d", status)
	rc := b.reserve("ident", show.ID, carol, []string{reserved[30]}, "c", nil)
	b.check(rc.status == 201, "released seat is re-bookable -> %d", rc.status)
	status, _, _ = b.do("POST", "/reservations/"+ra.resID+"/cancel", alice.token, nil, nil)
	st, _ := b.getShow(show.ID)
	seatStatus := ""
	for _, s := range st.Seats {
		if s.Label == reserved[30] {
			seatStatus = s.Status
		}
	}
	b.check(status == 200 && seatStatus == "confirmed", "repeat cancel by alice -> %d, carol's seat still %s", status, seatStatus)

	// Phase 6: reconcile everything we saw against the server's view.
	fmt.Println("\n[6] Final reconciliation")
	b.reconcile(show.ID, before, ra.resID)

	b.report()
	if len(b.fails) > 0 {
		return fmt.Errorf("%d check(s) failed", len(b.fails))
	}
	fmt.Println("\nPASS: all checks held")
	return nil
}

func (b *burst) phase(name string) []result {
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []result
	for _, r := range b.results {
		if r.phase == name {
			out = append(out, r)
		}
	}
	return out
}

func (b *burst) count(prefix string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	n := 0
	for _, r := range b.results {
		if strings.HasPrefix(r.phase, prefix) {
			n++
		}
	}
	return n
}

func (b *burst) reconcile(showID string, before map[string]float64, cancelledID string) {
	st, err := b.getShow(showID)
	if err != nil {
		b.fail("final GET show: %v", err)
		return
	}
	c := st.Counts
	b.check(c.Available+c.Held+c.Confirmed == c.Total && c.Total == len(st.Seats),
		"available(%d) + held(%d) + confirmed(%d) == total(%d)", c.Available, c.Held, c.Confirmed, c.Total)

	// Client-side ledger: every distinct reservation we were told about.
	b.mu.Lock()
	resSeats := map[string][]string{}
	resUser := map[string]string{}
	created := 0
	for _, r := range b.results {
		if r.status == 201 {
			created++
		}
		if r.status == 200 || r.status == 201 {
			resSeats[r.resID] = r.seats
			resUser[r.resID] = r.user
		}
	}
	b.mu.Unlock()
	delete(resSeats, cancelledID)

	owner := map[string]string{}
	perUser := map[string]int{}
	for id, seats := range resSeats {
		perUser[resUser[id]] += len(seats)
		for _, s := range seats {
			if prev, ok := owner[s]; ok {
				b.fail("seat %s in two reservations: %s and %s", s, prev, id)
			}
			owner[s] = id
		}
	}
	b.check(len(owner) == c.Confirmed, "seats confirmed per server (%d) == seats in reservations clients received (%d)", c.Confirmed, len(owner))
	confirmedOnServer := map[string]bool{}
	for _, s := range st.Seats {
		if s.Status == "confirmed" {
			confirmedOnServer[s.Label] = true
		}
	}
	mismatch := 0
	for s := range owner {
		if !confirmedOnServer[s] {
			mismatch++
		}
	}
	b.check(mismatch == 0, "every seat a client was sold is confirmed on the server (%d mismatches)", mismatch)
	maxHeld := 0
	for _, n := range perUser {
		maxHeld = max(maxHeld, n)
	}
	b.check(maxHeld <= st.PerUserLimit, "max seats held by any user: %d (limit %d)", maxHeld, st.PerUserLimit)

	after := b.scrape()
	if after == nil {
		fmt.Println("  - /metrics not reachable, skipping metric reconciliation")
		return
	}
	gauge := after[`seatres_seats{show_id="`+showID+`",status="confirmed"}`]
	b.check(int(gauge) == c.Confirmed, "metrics gauge seatres_seats{status=confirmed} (%d) == API confirmed (%d)", int(gauge), c.Confirmed)
	dConfirmed := after["seatres_reservations_confirmed_total"] - before["seatres_reservations_confirmed_total"]
	// Counters are process-wide: only an exact match if nobody else is
	// hitting the service, so report instead of failing.
	fmt.Printf("  - seatres_reservations_confirmed_total increased by %d; this run received %d x 201", int(dConfirmed), created)
	if int(dConfirmed) == created {
		fmt.Println(" (match)")
	} else {
		fmt.Println(" (differs: other traffic or an instance restart during the run)")
	}
}

// scrape returns un-labelled and labelled sample values from /metrics.
func (b *burst) scrape() map[string]float64 {
	status, raw, err := b.do("GET", "/metrics", "", nil, nil)
	if err != nil || status != 200 {
		return nil
	}
	out := map[string]float64{}
	for _, line := range strings.Split(string(raw), "\n") {
		if !strings.HasPrefix(line, "seatres_") {
			continue
		}
		i := strings.LastIndexByte(line, ' ')
		v, err := strconv.ParseFloat(line[i+1:], 64)
		if err == nil {
			out[line[:i]] = v
		}
	}
	return out
}

func (b *burst) report() {
	b.mu.Lock()
	defer b.mu.Unlock()
	byStatus := map[string]int{}
	byReason := map[string]int{}
	var lat []time.Duration
	fivexx, transport := 0, 0
	transportErrs := map[string]int{}
	firstFails := map[string]int{}
	for _, r := range b.results {
		if r.err != nil {
			transportErrs[trimURL(r.err.Error())]++
		}
		if r.firstFail != "" {
			firstFails[r.firstFail]++
		}
		switch {
		case r.status == 0:
			transport++
			byStatus["transport error"]++
		default:
			byStatus[strconv.Itoa(r.status)]++
		}
		if r.status >= 500 {
			fivexx++
		}
		switch {
		case r.status == 201:
			byReason["confirmed"]++
		case r.status == 200:
			byReason["idempotent_replay"]++
		case r.errCode != "":
			byReason[r.errCode]++
		}
		if r.status != 0 {
			lat = append(lat, r.latency)
		}
	}
	fmt.Printf("\nOutcome distribution (%d reserve requests)\n", len(b.results))
	printMap("  by status", byStatus)
	printMap("  by outcome", byReason)
	if len(lat) > 0 {
		sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
		p := func(q float64) time.Duration { return lat[int(q*float64(len(lat)-1))].Round(time.Millisecond) }
		fmt.Printf("  latency p50=%s p95=%s p99=%s max=%s\n", p(.5), p(.95), p(.99), lat[len(lat)-1].Round(time.Millisecond))
	}
	if len(transportErrs) > 0 {
		printMap("  transport errors", transportErrs)
	}
	if len(firstFails) > 0 {
		n := 0
		for _, c := range firstFails {
			n += c
		}
		printMap("  failed first attempts (retried with the same key)", firstFails)
		b.fails = append(b.fails, fmt.Sprintf("%d requests failed on the first attempt", n))
		fmt.Printf("  ✗ %d requests needed a retry; a client that doesn't retry would have seen these\n", n)
	}
	if fivexx > 0 || transport > 0 {
		b.fails = append(b.fails, fmt.Sprintf("%d x 5xx, %d transport errors", fivexx, transport))
		fmt.Printf("  ✗ %d x 5xx, %d transport errors\n", fivexx, transport)
	} else {
		fmt.Println("  ✓ zero 5xx, zero transport errors")
	}
}

// trimURL drops the "Post \"<url>\": " prefix so identical errors group.
func trimURL(msg string) string {
	if i := strings.LastIndex(msg, "\": "); i >= 0 {
		return msg[i+3:]
	}
	return msg
}

func printMap(title string, m map[string]int) {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	fmt.Println(title)
	for _, k := range keys {
		fmt.Printf("    %-22s %7d\n", k, m[k])
	}
}
