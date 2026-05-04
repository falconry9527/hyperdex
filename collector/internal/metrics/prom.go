package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	KlinesIngested = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "collector_klines_ingested_total",
			Help: "Number of finalized klines persisted to DB.",
		},
		[]string{"coin", "interval"},
	)
	KlinesLive = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "collector_klines_live_total",
			Help: "Number of live (in-progress) kline updates pushed to Redis.",
		},
		[]string{"coin", "interval"},
	)
	BatchFlushDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "collector_batch_flush_seconds",
			Help:    "DB batch flush latency.",
			Buckets: prometheus.DefBuckets,
		},
	)
	RetentionDeleted = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "collector_retention_deleted_total",
			Help: "Rows pruned by the retention janitor, by table.",
		},
		[]string{"table"},
	)
	RetentionSweepDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "collector_retention_sweep_seconds",
			Help:    "End-to-end duration of a single retention sweep, per table.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"table"},
	)
)

func init() {
	prometheus.MustRegister(
		KlinesIngested, KlinesLive, BatchFlushDuration,
		RetentionDeleted, RetentionSweepDuration,
	)
}

func Handler() http.Handler {
	return promhttp.Handler()
}
