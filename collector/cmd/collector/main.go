package main

import (
	"context"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/seabond/collector/internal/config"
	"github.com/seabond/collector/internal/db"
	"github.com/seabond/collector/internal/hl"
	"github.com/seabond/collector/internal/ingest/kline"
	"github.com/seabond/collector/internal/ingest/l2book"
	"github.com/seabond/collector/internal/ingest/ticker"
	"github.com/seabond/collector/internal/ingest/trades"
	"github.com/seabond/collector/internal/metrics"
	"github.com/seabond/collector/internal/retention"
)

func main() {
	configPath := flag.String("config", "configs/config.toml", "path to config")
	backfillDays := flag.Int("backfill-days", 0, "if >0, run REST backfill for this many days then exit")
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

	rdb := db.NewRedis(cfg.Redis)
	if err := rdb.Ping(ctx); err != nil {
		log.Error("ping redis", "err", err)
		os.Exit(1)
	}
	defer rdb.Close()

	rest := hl.NewRESTClient(cfg.HL.APIURL)

	// Backfill mode: pull history then exit. Useful for one-shot bootstrap.
	if *backfillDays > 0 {
		ws := hl.NewWSClient(cfg.HL.WSURL, log) // not connected; only need ingester construction
		ing := kline.New(cfg.Kline, ws, pool, rdb, log)
		to := time.Now().UTC()
		from := to.Add(-time.Duration(*backfillDays) * 24 * time.Hour)
		log.Info("backfill start", "from", from, "to", to, "coins", cfg.Kline.Coins)
		if err := ing.Backfill(ctx, rest, from, to); err != nil {
			log.Error("backfill", "err", err)
			os.Exit(1)
		}
		log.Info("backfill done")
		return
	}

	ws := hl.NewWSClient(cfg.HL.WSURL, log)
	if err := ws.Connect(ctx); err != nil {
		log.Error("ws connect", "err", err)
		os.Exit(1)
	}
	defer ws.Close()

	klineIng := kline.New(cfg.Kline, ws, pool, rdb, log)
	l2Ing := l2book.New(cfg.L2Book, ws, rdb, log.With("ingester", "l2book"))
	tickerIng := ticker.New(cfg.Ticker, ws, rdb, log.With("ingester", "ticker"))
	tradesIng := trades.New(cfg.Trades, ws, rdb, log.With("ingester", "trades"))
	janitor := retention.New(cfg.Retention, pool, log.With("ingester", "retention"))

	go func() {
		mux := http.NewServeMux()
		mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok"))
		})
		mux.Handle("/metrics", metrics.Handler())
		log.Info("admin listening", "addr", cfg.Admin.Addr)
		if err := http.ListenAndServe(cfg.Admin.Addr, mux); err != nil && err != http.ErrServerClosed {
			log.Error("admin server", "err", err)
		}
	}()

	log.Info("collector starting",
		"kline_coins", cfg.Kline.Coins, "kline_intervals", cfg.Kline.Intervals,
		"l2book_enabled", cfg.L2Book.Enabled, "l2book_coins", cfg.L2Book.Coins,
		"ticker_enabled", cfg.Ticker.Enabled, "ticker_coins", cfg.Ticker.Coins,
		"trades_enabled", cfg.Trades.Enabled, "trades_coins", cfg.Trades.Coins,
	)

	// All ingesters share the same WS client and run until ctx cancels.
	// Note: userfills is NOT in this list — it's a one-shot CLI tool now,
	// not a long-running ingester. Run `cmd/sync-fills` (or `make sync-fills`)
	// when you want to refresh user_fills from HL.
	ingesters := []func(context.Context) error{
		klineIng.Run,
		l2Ing.Run,
		tickerIng.Run,
		tradesIng.Run,
		janitor.Run,
	}
	errCh := make(chan error, len(ingesters))
	for _, run := range ingesters {
		run := run
		go func() { errCh <- run(ctx) }()
	}
	for range ingesters {
		if err := <-errCh; err != nil {
			log.Error("ingester", "err", err)
		}
	}
	log.Info("collector stopped")
}
