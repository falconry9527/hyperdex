// Package l2book ingests Hyperliquid L2 order book snapshots into Redis.
//
// HL pushes a full top-N snapshot per update (~5–10 Hz), not diffs, so the
// flow is one-way:
//
//	HL WS callback -> json.Marshal(L2Book) -> Redis SET + PUBLISH
//
// L2 is live-only — no DB persistence. The Redis key TTL (30s) is short on
// purpose: a stale snapshot is worse than none for this use case, since a
// frozen book misleads traders.
package l2book

import (
	"context"
	"encoding/json"
	"log/slog"

	hyperliquid "github.com/sonirico/go-hyperliquid"

	"github.com/seabond/collector/internal/config"
	"github.com/seabond/collector/internal/db"
	"github.com/seabond/collector/internal/hl"
)

const domainName = "l2book"

type Ingester struct {
	cfg   config.L2BookConfig
	ws    *hl.WSClient
	cache *db.Redis
	log   *slog.Logger
}

func New(cfg config.L2BookConfig, ws *hl.WSClient, c *db.Redis, log *slog.Logger) *Ingester {
	return &Ingester{
		cfg:   cfg,
		ws:    ws,
		cache: c,
		log:   log,
	}
}

func (i *Ingester) Name() string { return domainName }

// Run subscribes to all configured coins and blocks until ctx is cancelled.
func (i *Ingester) Run(ctx context.Context) error {
	if !i.cfg.Enabled {
		i.log.Info("l2book ingester disabled")
		<-ctx.Done()
		return nil
	}

	for _, coin := range i.cfg.Coins {
		coin := coin
		err := i.ws.SubscribeL2Book(coin, func(b hyperliquid.L2Book) {
			i.handle(ctx, b)
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

// handle marshals the snapshot back to HL-native JSON and publishes it. We
// re-marshal (rather than capturing the raw frame from the SDK) because the
// SDK doesn't expose the original bytes; the Level json tags `,string` keep
// numeric fields as strings on the wire so the shape stays HL-compatible.
func (i *Ingester) handle(ctx context.Context, b hyperliquid.L2Book) {
	payload, err := json.Marshal(b)
	if err != nil {
		i.log.Warn("marshal l2book failed", "coin", b.Coin, "err", err)
		return
	}
	if err := i.cache.PublishLiveL2Book(ctx, b.Coin, payload); err != nil {
		i.log.Warn("publish l2book failed", "coin", b.Coin, "err", err)
	}
}
