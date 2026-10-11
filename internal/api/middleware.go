package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"net/http"
	"strconv"
	"time"
)

type ctxKey int

const reqInfoKey ctxKey = iota

// RequestTimeout bounds how long a request may queue for and use the DB.
// When it expires the handler answers 503 + Retry-After rather than letting
// the server's write timeout cut the connection.
const RequestTimeout = 60 * time.Second

// reqInfo is filled in by handlers and read by the access log, so each request
// produces exactly one log line carrying its outcome.
type reqInfo struct {
	requestID string
	userID    string
	outcome   string
	err       error
}

func info(ctx context.Context) *reqInfo {
	if ri, ok := ctx.Value(reqInfoKey).(*reqInfo); ok {
		return ri
	}
	return &reqInfo{}
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

func newRequestID() string {
	b := make([]byte, 8)
	rand.Read(b)
	return hex.EncodeToString(b)
}

// observe assigns a request id, recovers panics, and records metrics and one
// structured access-log line per request.
func (s *Server) observe(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rid := r.Header.Get("X-Request-ID")
		if rid == "" || len(rid) > 64 {
			rid = newRequestID()
		}
		w.Header().Set("X-Request-ID", rid)
		ri := &reqInfo{requestID: rid}
		ctx, cancel := context.WithTimeout(context.WithValue(r.Context(), reqInfoKey, ri), RequestTimeout)
		defer cancel()
		r = r.WithContext(ctx)
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}

		s.metrics.InFlight.Inc()
		defer func() {
			s.metrics.InFlight.Dec()
			if p := recover(); p != nil {
				s.log.Error("panic", "request_id", rid, "panic", p)
				writeError(rec, r, http.StatusInternalServerError, "internal_error", "internal error")
			}
			route := r.Pattern // set in place by ServeMux
			if route == "" {
				route = "unmatched"
			}
			elapsed := time.Since(start)
			s.metrics.HTTPRequests.WithLabelValues(route, strconv.Itoa(rec.status)).Inc()
			s.metrics.HTTPDuration.WithLabelValues(route).Observe(elapsed.Seconds())

			if route == "GET /metrics" || route == "GET /logs" || route == "GET /healthz" {
				return // keep scrapes and probes out of the log ring
			}
			level := slog.LevelInfo
			if rec.status >= 500 {
				level = slog.LevelError
			}
			attrs := []any{
				"request_id", rid,
				"method", r.Method,
				"route", route,
				"path", r.URL.Path,
				"status", rec.status,
				"duration_ms", float64(elapsed.Microseconds()) / 1000,
			}
			if ri.userID != "" {
				attrs = append(attrs, "user_id", ri.userID)
			}
			if ri.outcome != "" {
				attrs = append(attrs, "outcome", ri.outcome)
			}
			if ri.err != nil {
				attrs = append(attrs, "error", ri.err.Error())
			}
			s.log.Log(r.Context(), level, "request", attrs...)
		}()
		next.ServeHTTP(rec, r)
	})
}
