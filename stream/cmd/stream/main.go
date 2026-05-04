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

	"github.com/seabond/stream/internal/config"
	"github.com/seabond/stream/internal/db"
	"github.com/seabond/stream/internal/stream/routes"
)

func main() {
	configPath := flag.String("config", "configs/config.toml", "path to config")
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

	rdb := db.NewRedis(cfg.Redis)
	if err := rdb.Ping(ctx); err != nil {
		log.Error("ping redis", "err", err)
		os.Exit(1)
	}
	defer rdb.Close()

	router, cleanup := routes.InitRouter(rdb, log)
	defer cleanup()

	srv := &http.Server{
		Addr:              cfg.HTTP.Addr,
		Handler:           router,
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		log.Info("stream listening", "addr", cfg.HTTP.Addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("stream serve", "err", err)
		}
	}()

	<-ctx.Done()
	log.Info("stream shutting down")
	shutdownCtx, sc := context.WithTimeout(context.Background(), 10*time.Second)
	defer sc()
	_ = srv.Shutdown(shutdownCtx)
}
