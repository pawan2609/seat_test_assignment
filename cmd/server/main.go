package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/pawan2609/seat-reservation/internal/api"
	"github.com/pawan2609/seat-reservation/internal/auth"
	"github.com/pawan2609/seat-reservation/internal/obs"
	"github.com/pawan2609/seat-reservation/internal/store"
)

func main() {
	ring := obs.NewLogRing(5000)
	log := obs.NewLogger(ring, slog.LevelInfo)
	if err := run(log, ring); err != nil {
		log.Error("fatal", "error", err)
		os.Exit(1)
	}
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func run(log *slog.Logger, ring *obs.LogRing) error {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		return errors.New("DATABASE_URL is required")
	}
	secret := os.Getenv("TOKEN_SECRET")
	adminToken := os.Getenv("ADMIN_TOKEN")
	if secret == "" || adminToken == "" {
		return errors.New("TOKEN_SECRET and ADMIN_TOKEN are required")
	}
	maxConns, _ := strconv.Atoi(env("DB_MAX_CONNS", "20"))

	cfg, err := pgxpool.ParseConfig(dbURL)
	if err != nil {
		return err
	}
	cfg.MaxConns = int32(maxConns)
	cfg.MinConns = 2
	cfg.MaxConnIdleTime = 5 * time.Minute
	cfg.HealthCheckPeriod = 15 * time.Second
	// Bound how long any statement or lock wait can run, so a stuck DB turns
	// into a fast 503 rather than an ever-growing queue.
	cfg.ConnConfig.RuntimeParams["statement_timeout"] = "10000"
	cfg.ConnConfig.RuntimeParams["lock_timeout"] = "8000"
	cfg.ConnConfig.RuntimeParams["idle_in_transaction_session_timeout"] = "15000"

	// NewWithConfig doesn't dial; the process comes up even if the DB is still
	// waking, and /readyz reports the truth until it is reachable.
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		return err
	}
	defer pool.Close()

	st := store.New(pool)
	metrics := obs.NewMetrics(st)
	srv := api.NewServer(st, auth.NewSigner(secret), adminToken, metrics, ring, log)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go migrateUntilReady(ctx, log, st, srv)

	httpSrv := &http.Server{
		Addr:              ":" + env("PORT", "8080"),
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		// Must exceed api.RequestTimeout: past WriteTimeout Go drops the
		// connection without a response, which clients see as a bare EOF.
		WriteTimeout: api.RequestTimeout + 30*time.Second,
		IdleTimeout:  120 * time.Second,
	}
	errCh := make(chan error, 1)
	go func() {
		log.Info("listening", "addr", httpSrv.Addr, "db_max_conns", maxConns)
		errCh <- httpSrv.ListenAndServe()
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
	}

	// Fail readiness first so the load balancer stops sending traffic, then
	// let in-flight transactions finish.
	log.Info("shutting down")
	srv.SetDraining(true)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	return httpSrv.Shutdown(shutdownCtx)
}

func migrateUntilReady(ctx context.Context, log *slog.Logger, st *store.Store, srv *api.Server) {
	backoff := 500 * time.Millisecond
	for {
		mctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		err := st.Migrate(mctx)
		cancel()
		if err == nil {
			log.Info("schema ready")
			srv.SetReady(true)
			return
		}
		log.Warn("migration failed, retrying", "error", err, "retry_in", backoff.String())
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, 10*time.Second)
	}
}
