// Package obs holds metrics and structured-log plumbing.
package obs

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"

	"github.com/pawan2609/seat-reservation/internal/store"
)

// Decline reasons, used as the "reason" label and in API error codes.
const (
	ReasonSeatTaken           = "seat_taken"
	ReasonPerUserLimit        = "per_user_limit"
	ReasonIdempotentReplay    = "idempotent_replay"
	ReasonIdempotencyConflict = "idempotency_conflict"
	ReasonUnknownSeat         = "unknown_seat"
	ReasonShowNotFound        = "show_not_found"
	ReasonInvalidRequest      = "invalid_request"
	ReasonUnauthorized        = "unauthorized"
)

type Metrics struct {
	Registry *prometheus.Registry

	ReservationsConfirmed prometheus.Counter
	SeatsConfirmed        prometheus.Counter
	Declines              *prometheus.CounterVec
	Cancellations         prometheus.Counter
	SeatsReleased         prometheus.Counter
	HTTPRequests          *prometheus.CounterVec
	HTTPDuration          *prometheus.HistogramVec
	InFlight              prometheus.Gauge
	PrecheckBatchSize     prometheus.Histogram
}

func NewMetrics(st *store.Store) *Metrics {
	m := &Metrics{
		Registry: prometheus.NewRegistry(),
		ReservationsConfirmed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "seatres_reservations_confirmed_total",
			Help: "Reservations newly confirmed (idempotent replays not counted).",
		}),
		SeatsConfirmed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "seatres_seats_confirmed_total",
			Help: "Seats newly confirmed across all reservations.",
		}),
		Declines: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "seatres_reservation_declines_total",
			Help: "Reserve requests that did not create a new reservation, by reason.",
		}, []string{"reason"}),
		Cancellations: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "seatres_cancellations_total",
			Help: "Reservations cancelled (repeat cancels not counted).",
		}),
		SeatsReleased: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "seatres_seats_released_total",
			Help: "Seats returned to available by cancellations.",
		}),
		HTTPRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "seatres_http_requests_total",
			Help: "HTTP requests by route and status code.",
		}, []string{"route", "code"}),
		HTTPDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "seatres_http_request_duration_seconds",
			Help:    "HTTP request latency by route.",
			Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30},
		}, []string{"route"}),
		InFlight: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "seatres_http_in_flight_requests",
			Help: "Requests currently being served.",
		}),
		PrecheckBatchSize: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "seatres_precheck_batch_size",
			Help:    "Reserve prechecks answered per coalesced DB statement.",
			Buckets: []float64{1, 2, 4, 8, 16, 32, 64, 128, 256},
		}),
	}
	// Pre-create every reason so dashboards show explicit zeros.
	for _, r := range []string{ReasonSeatTaken, ReasonPerUserLimit, ReasonIdempotentReplay,
		ReasonIdempotencyConflict, ReasonUnknownSeat, ReasonShowNotFound, ReasonInvalidRequest} {
		m.Declines.WithLabelValues(r)
	}
	m.Registry.MustRegister(
		m.ReservationsConfirmed, m.SeatsConfirmed, m.Declines, m.Cancellations,
		m.SeatsReleased, m.HTTPRequests, m.HTTPDuration, m.InFlight, m.PrecheckBatchSize,
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		&dbCollector{store: st},
	)
	st.OnPrecheckBatch(func(n int) { m.PrecheckBatchSize.Observe(float64(n)) })
	return m
}

// dbCollector reads seat state from Postgres at scrape time, so the gauges are
// exactly what GET /shows/{id} would report, not a cached copy that can drift.
type dbCollector struct {
	store *store.Store
}

// recentShows bounds label cardinality on the per-show gauge.
const recentShows = 25

var (
	seatsDesc = prometheus.NewDesc("seatres_seats",
		"Seats by status for the most recent shows (read from the DB at scrape time).",
		[]string{"show_id", "status"}, nil)
	dbUpDesc = prometheus.NewDesc("seatres_db_up",
		"1 if the database answered the last scrape query.", nil, nil)
	poolDesc = prometheus.NewDesc("seatres_db_pool_conns",
		"Database pool connections by state.", []string{"state"}, nil)
	poolWaitDesc = prometheus.NewDesc("seatres_db_pool_empty_acquire_total",
		"Acquires that had to wait for a free connection.", nil, nil)
)

func (c *dbCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- seatsDesc
	ch <- dbUpDesc
	ch <- poolDesc
	ch <- poolWaitDesc
}

func (c *dbCollector) Collect(ch chan<- prometheus.Metric) {
	collectPool(ch, c.store.Pool())

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	counts, err := c.store.SeatCountsByShow(ctx, recentShows)
	if err != nil {
		ch <- prometheus.MustNewConstMetric(dbUpDesc, prometheus.GaugeValue, 0)
		return
	}
	ch <- prometheus.MustNewConstMetric(dbUpDesc, prometheus.GaugeValue, 1)
	for showID, sc := range counts {
		ch <- prometheus.MustNewConstMetric(seatsDesc, prometheus.GaugeValue, float64(sc.Available), showID, "available")
		ch <- prometheus.MustNewConstMetric(seatsDesc, prometheus.GaugeValue, float64(sc.Held), showID, "held")
		ch <- prometheus.MustNewConstMetric(seatsDesc, prometheus.GaugeValue, float64(sc.Confirmed), showID, "confirmed")
		ch <- prometheus.MustNewConstMetric(seatsDesc, prometheus.GaugeValue, float64(sc.Total), showID, "total")
	}
}

func collectPool(ch chan<- prometheus.Metric, pool *pgxpool.Pool) {
	s := pool.Stat()
	ch <- prometheus.MustNewConstMetric(poolDesc, prometheus.GaugeValue, float64(s.AcquiredConns()), "acquired")
	ch <- prometheus.MustNewConstMetric(poolDesc, prometheus.GaugeValue, float64(s.IdleConns()), "idle")
	ch <- prometheus.MustNewConstMetric(poolDesc, prometheus.GaugeValue, float64(s.MaxConns()), "max")
	ch <- prometheus.MustNewConstMetric(poolWaitDesc, prometheus.CounterValue, float64(s.EmptyAcquireCount()))
}
