// Package ticker ingests Hyperliquid `activeAssetCtx` snapshots (mark/mid
// price, 24h volume, funding rate) into Redis. Live-only; no DB. Powers the
// top-bar ticker on the trading UI.
package ticker

import (
	"context"
	"encoding/json"
	"log/slog"

	hyperliquid "github.com/sonirico/go-hyperliquid"

	"github.com/seabond/collector/internal/config"
	"github.com/seabond/collector/internal/db"
	"github.com/seabond/collector/internal/hl"
)

const domainName = "ticker"

type Ingester struct {
	cfg   config.TickerConfig
	ws    *hl.WSClient
	cache *db.Redis
	log   *slog.Logger
}

func New(cfg config.TickerConfig, ws *hl.WSClient, c *db.Redis, log *slog.Logger) *Ingester {
	return &Ingester{cfg: cfg, ws: ws, cache: c, log: log}
}

func (i *Ingester) Name() string { return domainName }

func (i *Ingester) Run(ctx context.Context) error {
	if !i.cfg.Enabled {
		i.log.Info("ticker ingester disabled")
		<-ctx.Done()
		return nil
	}
	for _, coin := range i.cfg.Coins {
		coin := coin
		err := i.ws.SubscribeActiveAssetCtx(coin, func(c hyperliquid.ActiveAssetCtx) {
			i.handle(ctx, c)
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

func (i *Ingester) handle(ctx context.Context, c hyperliquid.ActiveAssetCtx) {
	payload, err := json.Marshal(c)
	if err != nil {
		i.log.Warn("marshal ticker failed", "coin", c.Coin, "err", err)
		return
	}
	if err := i.cache.PublishLiveTicker(ctx, c.Coin, payload); err != nil {
		i.log.Warn("publish ticker failed", "coin", c.Coin, "err", err)
	}
}
