// Package trades ingests Hyperliquid public trade prints into Redis. Each HL
// callback delivers an array of new trades for a single coin; we cache the
// rolling-window of recent prints (LPUSH+LTRIM) and publish the new batch.
//
// Live-only — trades are not persisted. The cache lets new ws subscribers see
// recent history rather than waiting for the next print.
package trades

import (
	"context"
	"encoding/json"
	"log/slog"

	hyperliquid "github.com/sonirico/go-hyperliquid"

	"github.com/seabond/collector/internal/config"
	"github.com/seabond/collector/internal/db"
	"github.com/seabond/collector/internal/hl"
)

const domainName = "trades"

type Ingester struct {
	cfg   config.TradesConfig
	ws    *hl.WSClient
	cache *db.Redis
	log   *slog.Logger
}

func New(cfg config.TradesConfig, ws *hl.WSClient, c *db.Redis, log *slog.Logger) *Ingester {
	return &Ingester{cfg: cfg, ws: ws, cache: c, log: log}
}

func (i *Ingester) Name() string { return domainName }

func (i *Ingester) Run(ctx context.Context) error {
	if !i.cfg.Enabled {
		i.log.Info("trades ingester disabled")
		<-ctx.Done()
		return nil
	}
	for _, coin := range i.cfg.Coins {
		coin := coin
		err := i.ws.SubscribeTrades(coin, func(ts []hyperliquid.Trade) {
			i.handle(ctx, coin, ts)
		})
		if err != nil {
			i.log.Error("subscribe failed", "coin", coin, "err", err)
			continue
		}
		i.log.Info("subscribed", "coin", coin)
	}
	<-ctx.Done()
	return nil
}

func (i *Ingester) handle(ctx context.Context, coin string, ts []hyperliquid.Trade) {
	// Marshal the batch (for pubsub) and each trade individually (for LPUSH).
	// Per-trade is needed because the cache is a Redis LIST keyed by trade —
	// LRANGE on the read side then yields the list of recent prints.
	batch, err := json.Marshal(ts)
	if err != nil {
		i.log.Warn("marshal trades batch failed", "coin", coin, "err", err)
		return
	}
	perTrade := make([][]byte, 0, len(ts))
	for _, t := range ts {
		b, err := json.Marshal(t)
		if err != nil {
			i.log.Warn("marshal trade failed", "coin", coin, "err", err)
			continue
		}
		perTrade = append(perTrade, b)
	}
	if err := i.cache.PublishLiveTrades(ctx, coin, batch, perTrade); err != nil {
		i.log.Warn("publish trades failed", "coin", coin, "err", err)
	}
}
