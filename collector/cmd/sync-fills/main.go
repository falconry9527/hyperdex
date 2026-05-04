// Command sync-fills brings the user_fills PG table up to date for every
// address listed in [userfills].addrs, then exits. Intended to be run on
// demand (or via cron) — there's no long-running ingester for fills.
//
// Usage:
//
//	go run ./cmd/sync-fills --config configs/config.toml
//	go run ./cmd/sync-fills --config configs/config.toml --addr 0xabc... --days 30
//
// Flags override config: pass --addr to sync exactly one address regardless
// of [userfills].addrs; pass --days to override BackfillDays for this run.
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/seabond/collector/internal/config"
	"github.com/seabond/collector/internal/db"
	"github.com/seabond/collector/internal/hl"
	"github.com/seabond/collector/internal/ingest/userfills"
)

func main() {
	configPath := flag.String("config", "configs/config.toml", "path to config")
	addrOverride := flag.String("addr", "", "if set, sync only this address (overrides [userfills].addrs)")
	daysOverride := flag.Int("days", 0, "if >0, override [userfills].backfill_days for this run")
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(log)

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Error("load config", "err", err)
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

	rest := hl.NewRESTClient(cfg.HL.APIURL)

	addrs := cfg.UserFills.Addrs
	if *addrOverride != "" {
		addrs = []string{strings.ToLower(*addrOverride)}
	}
	if len(addrs) == 0 {
		log.Error("no addresses to sync — set [userfills].addrs in config or pass --addr")
		os.Exit(1)
	}
	days := cfg.UserFills.BackfillDays
	if *daysOverride > 0 {
		days = *daysOverride
	}

	syncer := userfills.New(rest, pool, log.With("cmd", "sync-fills"))
	log.Info("sync start", "addrs", addrs, "backfill_days", days)
	if err := syncer.SyncAll(ctx, addrs, days); err != nil {
		log.Error("sync", "err", err)
		os.Exit(1)
	}
	log.Info("sync done")
}
