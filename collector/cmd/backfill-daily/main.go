// Command backfill-daily pulls REST history for day-and-above kline intervals
// (1d / 1mo) into their respective hypertables, then exits. Intended to be
// cron-scheduled daily — these intervals close infrequently and HL retains
// many years of history for them, so the cadence can be loose.
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

var dailyIntervals = []string{"1d", "1mo"}

func main() {
	configPath := flag.String("config", "configs/config.toml", "path to config")
	days := flag.Int("days", 1825, "backfill window in days (paginates back from now; default 5y)")
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(log)

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Error("load config", "err", err)
		os.Exit(1)
	}

	// 1d/1mo aren't always in the live cfg.Intervals list (1mo never is — HL
	// barely pushes monthly closes), so this command always seeds the full
	// dailyIntervals set regardless of cfg. It is the only writer of these
	// intervals; collisions with the live ingester are impossible.
	cfg.Kline.Intervals = dailyIntervals

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
	ws := hl.NewWSClient(cfg.HL.WSURL, log)
	ing := kline.New(cfg.Kline, ws, pool, rdb, log)

	to := time.Now().UTC()
	from := to.Add(-time.Duration(*days) * 24 * time.Hour)
	log.Info("backfill start",
		"scope", "daily",
		"intervals", cfg.Kline.Intervals,
		"coins", cfg.Kline.Coins,
		"from", from, "to", to)
	if err := ing.Backfill(ctx, rest, from, to); err != nil {
		log.Error("backfill", "err", err)
		os.Exit(1)
	}
	log.Info("backfill done")
}

