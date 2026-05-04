// Command backfill-intraday pulls REST history for sub-day kline intervals
// (1m / 5m / 15m / 1h / 4h) into their respective hypertables, then exits.
// Intended to be cron-scheduled at sub-daily cadence (e.g., hourly) so each
// interval stays fresh against HL's per-interval retention window.
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/seabond/collector/internal/config"
	"github.com/seabond/collector/internal/db"
	"github.com/seabond/collector/internal/hl"
	"github.com/seabond/collector/internal/ingest/kline"
)

var intradayIntervals = []string{"1m", "5m", "15m", "1h", "4h"}

func main() {
	configPath := flag.String("config", "configs/config.toml", "path to config")
	days := flag.Int("days", 30, "backfill window in days (paginates back from now)")
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(log)

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Error("load config", "err", err)
		os.Exit(1)
	}

	// Restrict backfill to sub-day intervals; ignore whatever cfg lists.
	cfg.Kline.Intervals = filterIntervals(cfg.Kline.Intervals, intradayIntervals)
	if len(cfg.Kline.Intervals) == 0 {
		log.Error("no intraday intervals enabled in config", "want", intradayIntervals)
		os.Exit(1)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	pool, err := db.NewPostgres(ctx, cfg.Postgres)
	if err != nil {
		log.Error("open pg", "err", err)
		os.Exit(1)
	}
	defer pool.Close()

	rdb := db.NewRedis(cfg.Redis)
	if err := rdb.Ping(ctx); err != nil {
		log.Error("ping redis", "err", err)
		os.Exit(1)
	}
	defer rdb.Close()

	rest := hl.NewRESTClient(cfg.HL.APIURL)
	ws := hl.NewWSClient(cfg.HL.WSURL, log) // unconnected; only used to satisfy ingester.New
	ing := kline.New(cfg.Kline, ws, pool, rdb, log)

	to := time.Now().UTC()
	from := to.Add(-time.Duration(*days) * 24 * time.Hour)
	log.Info("backfill start",
		"scope", "intraday",
		"intervals", cfg.Kline.Intervals,
		"coins", cfg.Kline.Coins,
		"from", from, "to", to)
	if err := ing.Backfill(ctx, rest, from, to); err != nil {
		log.Error("backfill", "err", err)
		os.Exit(1)
	}
	log.Info("backfill done")
}

// filterIntervals returns the items of cfg present in the allow set, preserving
// cfg order. Used so we can drop daily intervals if someone left them in cfg.
func filterIntervals(cfgIntervals, allow []string) []string {
	allowed := make(map[string]struct{}, len(allow))
	for _, a := range allow {
		allowed[a] = struct{}{}
	}
	out := make([]string, 0, len(cfgIntervals))
	for _, iv := range cfgIntervals {
		if _, ok := allowed[iv]; ok {
			out = append(out, iv)
		}
	}
	return out
}
