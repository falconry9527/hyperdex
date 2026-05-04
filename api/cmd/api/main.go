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

	"github.com/seabond/api/internal/api/routes"
	"github.com/seabond/api/internal/config"
	"github.com/seabond/api/internal/db"
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

	pool, err := db.NewPostgres(ctx, cfg.Postgres)
	if err != nil {
		log.Error("open pg", "err", err)
		os.Exit(1)
	}
	defer pool.Close()

	router, cleanup := routes.InitRouter(pool, cfg)
	defer cleanup()

	srv := &http.Server{
		Addr:              cfg.HTTP.Addr,
		Handler:           router,
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		log.Info("api listening", "addr", cfg.HTTP.Addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("api serve", "err", err)
		}
	}()

	<-ctx.Done()
	log.Info("api shutting down")
	shutdownCtx, sc := context.WithTimeout(context.Background(), 10*time.Second)
	defer sc()
	_ = srv.Shutdown(shutdownCtx)
}
