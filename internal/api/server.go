// Package api is the JSON HTTP layer. Handlers validate input, derive
// identity from the bearer token, and map store outcomes to HTTP statuses.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/pawan2609/seat-reservation/internal/auth"
	"github.com/pawan2609/seat-reservation/internal/obs"
	"github.com/pawan2609/seat-reservation/internal/store"
)

const (
	maxSeatsPerShow  = 20000
	maxSeatLabelLen  = 16
	maxIdemKeyLen    = 200
	defaultUserLimit = 4
	maxBodyBytes     = 1 << 20
)

type Server struct {
	store      *store.Store
	signer     *auth.Signer
	adminToken string
	metrics    *obs.Metrics
	logs       *obs.LogRing
	log        *slog.Logger

	ready    atomic.Bool // schema migrated
	draining atomic.Bool // shutting down
}

func NewServer(st *store.Store, signer *auth.Signer, adminToken string, m *obs.Metrics, logs *obs.LogRing, log *slog.Logger) *Server {
	return &Server{store: st, signer: signer, adminToken: adminToken, metrics: m, logs: logs, log: log}
}

func (s *Server) SetReady(v bool)    { s.ready.Store(v) }
func (s *Server) SetDraining(v bool) { s.draining.Store(v) }

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", s.handleIndex)
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	mux.HandleFunc("GET /readyz", s.handleReadyz)
	mux.Handle("GET /metrics", promhttp.HandlerFor(s.metrics.Registry, promhttp.HandlerOpts{}))
	mux.HandleFunc("GET /logs", s.handleLogs)

	mux.HandleFunc("POST /users", s.handleCreateUser)
	mux.HandleFunc("POST /shows", s.requireAdmin(s.handleCreateShow))
	mux.HandleFunc("GET /shows/{id}", s.handleGetShow)
	mux.HandleFunc("POST /shows/{id}/reserve", s.requireUser(s.handleReserve))
	mux.HandleFunc("GET /reservations/{id}", s.requireUser(s.handleGetReservation))
	mux.HandleFunc("POST /reservations/{id}/cancel", s.requireUser(s.handleCancel))
	return s.observe(mux)
}

// ---- auth ----

func bearer(r *http.Request) string {
	h := r.Header.Get("Authorization")
	if t, ok := strings.CutPrefix(h, "Bearer "); ok {
		return strings.TrimSpace(t)
	}
	return ""
}

type userHandler func(w http.ResponseWriter, r *http.Request, userID string)

// requireUser derives identity solely from the token. Nothing in the request
// body is ever consulted for "who is this".
func (s *Server) requireUser(h userHandler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		userID, ok := s.signer.Verify(bearer(r))
		if !ok {
			info(r.Context()).outcome = obs.ReasonUnauthorized
			writeError(w, r, http.StatusUnauthorized, obs.ReasonUnauthorized, "missing or invalid bearer token; get one from POST /users")
			return
		}
		info(r.Context()).userID = userID
		h(w, r, userID)
	}
}

func (s *Server) requireAdmin(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !auth.AdminOK(bearer(r), s.adminToken) {
			info(r.Context()).outcome = obs.ReasonUnauthorized
			writeError(w, r, http.StatusUnauthorized, obs.ReasonUnauthorized, "admin token required")
			return
		}
		info(r.Context()).userID = "admin"
		h(w, r)
	}
}

// ---- ops endpoints ----

func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"service": "seat-reservation",
		"endpoints": []string{
			"POST /users", "POST /shows (admin)", "GET /shows/{id}",
			"POST /shows/{id}/reserve", "GET /reservations/{id}", "POST /reservations/{id}/cancel",
			"GET /healthz", "GET /readyz", "GET /metrics", "GET /logs?n=200&q=<substring>",
		},
	})
}

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// handleReadyz fails closed: not ready until the schema is applied, while
// draining, or whenever the DB doesn't answer a ping within a second.
func (s *Server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	status := func(code int, state, reason string) {
		body := map[string]string{"status": state}
		if reason != "" {
			body["reason"] = reason
		}
		writeJSON(w, code, body)
	}
	switch {
	case s.draining.Load():
		status(http.StatusServiceUnavailable, "unavailable", "draining")
		return
	case !s.ready.Load():
		status(http.StatusServiceUnavailable, "unavailable", "migrations not applied yet")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), time.Second)
	defer cancel()
	if err := s.store.Ping(ctx); err != nil {
		info(r.Context()).err = err
		status(http.StatusServiceUnavailable, "unavailable", "database unreachable")
		return
	}
	status(http.StatusOK, "ready", "")
}

func (s *Server) handleLogs(w http.ResponseWriter, r *http.Request) {
	n, _ := strconv.Atoi(r.URL.Query().Get("n"))
	if n <= 0 || n > 5000 {
		n = 200
	}
	w.Header().Set("Content-Type", "application/x-ndjson")
	for _, line := range s.logs.Tail(n, r.URL.Query().Get("q")) {
		w.Write(line)
		w.Write([]byte("\n"))
	}
}

// ---- users ----

func (s *Server) handleCreateUser(w http.ResponseWriter, r *http.Request) {
	userID := auth.NewUserID()
	info(r.Context()).userID = userID
	writeJSON(w, http.StatusCreated, map[string]string{
		"user_id": userID,
		"token":   s.signer.Issue(userID),
	})
}

// ---- shows ----

type createShowReq struct {
	Name         string   `json:"name"`
	Seats        []string `json:"seats"`
	PricePaise   *int64   `json:"price_paise"`
	PerUserLimit *int     `json:"per_user_limit"`
}

func (s *Server) handleCreateShow(w http.ResponseWriter, r *http.Request) {
	var req createShowReq
	if !s.decode(w, r, &req) {
		return
	}
	limit := defaultUserLimit
	if req.PerUserLimit != nil {
		limit = *req.PerUserLimit
	}
	var problems []string
	if req.Name = strings.TrimSpace(req.Name); req.Name == "" || len(req.Name) > 200 {
		problems = append(problems, "name must be 1-200 characters")
	}
	if req.PricePaise == nil || *req.PricePaise < 0 {
		problems = append(problems, "price_paise must be a non-negative integer (paise)")
	}
	if limit < 1 {
		problems = append(problems, "per_user_limit must be >= 1")
	}
	if len(req.Seats) == 0 || len(req.Seats) > maxSeatsPerShow {
		problems = append(problems, fmt.Sprintf("seats must contain 1-%d labels", maxSeatsPerShow))
	}
	seen := make(map[string]bool, len(req.Seats))
	for _, l := range req.Seats {
		if l == "" || len(l) > maxSeatLabelLen || strings.TrimSpace(l) != l {
			problems = append(problems, fmt.Sprintf("invalid seat label %q", l))
			break
		}
		if seen[l] {
			problems = append(problems, fmt.Sprintf("duplicate seat label %q", l))
			break
		}
		seen[l] = true
	}
	if len(problems) > 0 {
		writeError(w, r, http.StatusBadRequest, obs.ReasonInvalidRequest, strings.Join(problems, "; "))
		return
	}

	st, err := s.store.CreateShow(r.Context(), req.Name, req.Seats, *req.PricePaise, limit)
	if err != nil {
		s.serverError(w, r, err)
		return
	}
	s.log.Info("show created", "request_id", info(r.Context()).requestID, "show_id", st.ID, "seats", st.TotalSeats)
	writeJSON(w, http.StatusCreated, st)
}

func (s *Server) handleGetShow(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !validUUID(id) {
		writeError(w, r, http.StatusNotFound, obs.ReasonShowNotFound, "show not found")
		return
	}
	st, err := s.store.GetShow(r.Context(), id)
	if errors.Is(err, store.ErrShowNotFound) {
		writeError(w, r, http.StatusNotFound, obs.ReasonShowNotFound, "show not found")
		return
	}
	if err != nil {
		s.serverError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, st)
}

// ---- reservations ----

// reserveReq deliberately has no user field: any "user_id" a client sends is
// ignored by the decoder, and identity comes only from the token.
type reserveReq struct {
	Seats          []string `json:"seats"`
	IdempotencyKey string   `json:"idempotency_key"`
}

func (s *Server) handleReserve(w http.ResponseWriter, r *http.Request, userID string) {
	showID := r.PathValue("id")
	decline := func(status int, reason, msg string, extra map[string]any) {
		s.metrics.Declines.WithLabelValues(reason).Inc()
		info(r.Context()).outcome = reason
		writeErrorExtra(w, r, status, reason, msg, extra)
	}

	var req reserveReq
	if !s.decode(w, r, &req) {
		s.metrics.Declines.WithLabelValues(obs.ReasonInvalidRequest).Inc()
		return
	}
	key := req.IdempotencyKey
	if hk := r.Header.Get("Idempotency-Key"); hk != "" {
		if key != "" && key != hk {
			decline(http.StatusBadRequest, obs.ReasonInvalidRequest, "Idempotency-Key header and idempotency_key body field differ", nil)
			return
		}
		key = hk
	}
	switch {
	case key == "" || len(key) > maxIdemKeyLen:
		decline(http.StatusBadRequest, obs.ReasonInvalidRequest,
			fmt.Sprintf("idempotency key required (Idempotency-Key header or idempotency_key field, max %d chars)", maxIdemKeyLen), nil)
		return
	case len(req.Seats) == 0:
		decline(http.StatusBadRequest, obs.ReasonInvalidRequest, "seats must name at least one seat", nil)
		return
	case !validUUID(showID):
		decline(http.StatusNotFound, obs.ReasonShowNotFound, "show not found", nil)
		return
	}
	seats := slices.Clone(req.Seats)
	slices.Sort(seats)
	if len(slices.Compact(seats)) != len(req.Seats) {
		decline(http.StatusBadRequest, obs.ReasonInvalidRequest, "seats must not contain duplicates", nil)
		return
	}

	res, replayed, err := s.store.Reserve(r.Context(), store.ReserveInput{
		ShowID: showID, UserID: userID, IdempotencyKey: key, Seats: seats,
	})
	var taken *store.SeatTakenError
	var unknown *store.UnknownSeatError
	switch {
	case err == nil && replayed:
		s.metrics.Declines.WithLabelValues(obs.ReasonIdempotentReplay).Inc()
		info(r.Context()).outcome = obs.ReasonIdempotentReplay
		w.Header().Set("Idempotent-Replayed", "true")
		writeJSON(w, http.StatusOK, res)
	case err == nil:
		s.metrics.ReservationsConfirmed.Inc()
		s.metrics.SeatsConfirmed.Add(float64(len(res.Seats)))
		info(r.Context()).outcome = "confirmed"
		writeJSON(w, http.StatusCreated, res)
	case errors.As(err, &taken):
		decline(http.StatusConflict, obs.ReasonSeatTaken, "seat already taken", map[string]any{"seats": taken.Seats})
	case errors.Is(err, store.ErrPerUserLimit):
		decline(http.StatusConflict, obs.ReasonPerUserLimit, "per-user seat limit for this show reached", nil)
	case errors.Is(err, store.ErrIdempotencyMismatch):
		decline(http.StatusConflict, obs.ReasonIdempotencyConflict, "idempotency key already used for a different request", nil)
	case errors.As(err, &unknown):
		decline(http.StatusUnprocessableEntity, obs.ReasonUnknownSeat, "no such seat in this show", map[string]any{"seats": unknown.Seats})
	case errors.Is(err, store.ErrShowNotFound):
		decline(http.StatusNotFound, obs.ReasonShowNotFound, "show not found", nil)
	default:
		s.serverError(w, r, err)
	}
}

func (s *Server) handleGetReservation(w http.ResponseWriter, r *http.Request, userID string) {
	id := r.PathValue("id")
	if !validUUID(id) {
		s.writeReservationResult(w, r, store.Reservation{}, store.ErrReservationNotFound)
		return
	}
	res, err := s.store.GetReservation(r.Context(), id, userID)
	s.writeReservationResult(w, r, res, err)
}

func (s *Server) handleCancel(w http.ResponseWriter, r *http.Request, userID string) {
	id := r.PathValue("id")
	if !validUUID(id) {
		s.writeReservationResult(w, r, store.Reservation{}, store.ErrReservationNotFound)
		return
	}
	res, changed, err := s.store.Cancel(r.Context(), id, userID)
	if err == nil {
		if changed {
			s.metrics.Cancellations.Inc()
			s.metrics.SeatsReleased.Add(float64(len(res.Seats)))
			info(r.Context()).outcome = "cancelled"
		} else {
			info(r.Context()).outcome = "already_cancelled"
		}
	}
	s.writeReservationResult(w, r, res, err)
}

func (s *Server) writeReservationResult(w http.ResponseWriter, r *http.Request, res store.Reservation, err error) {
	switch {
	case err == nil:
		writeJSON(w, http.StatusOK, res)
	case errors.Is(err, store.ErrReservationNotFound):
		writeError(w, r, http.StatusNotFound, "reservation_not_found", "reservation not found")
	case errors.Is(err, store.ErrNotOwner):
		info(r.Context()).outcome = "forbidden"
		writeError(w, r, http.StatusForbidden, "forbidden", "reservation belongs to another user")
	default:
		s.serverError(w, r, err)
	}
}

// ---- helpers ----

func (s *Server) decode(w http.ResponseWriter, r *http.Request, dst any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	if err := json.NewDecoder(r.Body).Decode(dst); err != nil {
		info(r.Context()).outcome = obs.ReasonInvalidRequest
		writeError(w, r, http.StatusBadRequest, obs.ReasonInvalidRequest, "invalid JSON body: "+err.Error())
		return false
	}
	return true
}

// serverError distinguishes "we are overloaded / DB unavailable" (503, safe to
// retry) from genuine bugs (500). Both are logged with the request id.
func (s *Server) serverError(w http.ResponseWriter, r *http.Request, err error) {
	info(r.Context()).err = err
	var pgErr *pgconn.PgError
	var connErr *pgconn.ConnectError
	var netErr net.Error
	unavailable := errors.Is(err, context.DeadlineExceeded) ||
		errors.As(err, &connErr) || errors.As(err, &netErr) ||
		(errors.As(err, &pgErr) && (pgErr.Code == "55P03" || pgErr.Code == "57014" || pgErr.Code == "53300" || pgErr.Code == "57P01"))
	if unavailable || pgconn.SafeToRetry(err) || pgconn.Timeout(err) {
		w.Header().Set("Retry-After", "1")
		writeError(w, r, http.StatusServiceUnavailable, "unavailable", "temporarily unable to process; retry with the same idempotency key")
		return
	}
	writeError(w, r, http.StatusInternalServerError, "internal_error", "internal error")
}

func validUUID(s string) bool {
	_, err := uuid.Parse(s)
	return err == nil && len(s) == 36
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, r *http.Request, status int, code, msg string) {
	writeErrorExtra(w, r, status, code, msg, nil)
}

func writeErrorExtra(w http.ResponseWriter, r *http.Request, status int, code, msg string, extra map[string]any) {
	body := map[string]any{"error": code, "message": msg, "request_id": info(r.Context()).requestID}
	for k, v := range extra {
		body[k] = v
	}
	writeJSON(w, status, body)
}
