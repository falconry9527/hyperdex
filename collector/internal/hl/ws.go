package hl

import (
	"context"
	"fmt"
	"log/slog"

	hyperliquid "github.com/sonirico/go-hyperliquid"
)

// WSClient wraps the HL websocket client and exposes typed candle subscriptions.
type WSClient struct {
	url string
	cli *hyperliquid.WebsocketClient
	log *slog.Logger
}

func NewWSClient(url string, log *slog.Logger) *WSClient {
	return &WSClient{
		url: url,
		cli: hyperliquid.NewWebsocketClient(url),
		log: log,
	}
}

// Connect establishes the websocket. Subscriptions made after Connect will be
// auto re-subscribed on reconnect by the underlying SDK.
func (w *WSClient) Connect(ctx context.Context) error {
	if err := w.cli.Connect(ctx); err != nil {
		return fmt.Errorf("hl ws connect: %w", err)
	}
	return nil
}

// SubscribeCandles subscribes to (coin, interval) and forwards events to cb.
// The SDK invokes cb for every update, including in-progress candles.
//
// Interval is translated at the boundary: outbound param goes via
// toWireInterval (so "1mo" → HL's "1M"), and the SDK's candle.Interval is
// normalized back via fromWireInterval before reaching cb.
func (w *WSClient) SubscribeCandles(coin, interval string, cb func(hyperliquid.Candle)) error {
	_, err := w.cli.Candles(
		hyperliquid.CandlesSubscriptionParams{Coin: coin, Interval: toWireInterval(interval)},
		func(c hyperliquid.Candle, err error) {
			if err != nil {
				w.log.Error("candle callback error", "coin", coin, "err", err)
				return
			}
			c.Interval = fromWireInterval(c.Interval)
			cb(c)
		},
	)
	if err != nil {
		return fmt.Errorf("subscribe candles %s/%s: %w", coin, interval, err)
	}
	return nil
}

// SubscribeActiveAssetCtx subscribes to mark/mid/funding/24h stats for `coin`.
// HL pushes ~1 Hz; this powers the top-bar ticker.
func (w *WSClient) SubscribeActiveAssetCtx(coin string, cb func(hyperliquid.ActiveAssetCtx)) error {
	_, err := w.cli.ActiveAssetCtx(
		hyperliquid.ActiveAssetCtxSubscriptionParams{Coin: coin},
		func(c hyperliquid.ActiveAssetCtx, err error) {
			if err != nil {
				w.log.Error("activeAssetCtx callback error", "coin", coin, "err", err)
				return
			}
			cb(c)
		},
	)
	if err != nil {
		return fmt.Errorf("subscribe activeAssetCtx %s: %w", coin, err)
	}
	return nil
}

// SubscribeTrades subscribes to public tape (per-coin print stream). HL pushes
// arrays of new trades — each callback may carry 1..N entries.
func (w *WSClient) SubscribeTrades(coin string, cb func([]hyperliquid.Trade)) error {
	_, err := w.cli.Trades(
		hyperliquid.TradesSubscriptionParams{Coin: coin},
		func(ts []hyperliquid.Trade, err error) {
			if err != nil {
				w.log.Error("trades callback error", "coin", coin, "err", err)
				return
			}
			if len(ts) == 0 {
				return
			}
			cb(ts)
		},
	)
	if err != nil {
		return fmt.Errorf("subscribe trades %s: %w", coin, err)
	}
	return nil
}

// SubscribeL2Book subscribes to the L2 order book for `coin`. HL publishes a
// full top-N snapshot on each update (~5–10 Hz), not diffs, so the consumer
// can simply overwrite previous state.
func (w *WSClient) SubscribeL2Book(coin string, cb func(hyperliquid.L2Book)) error {
	_, err := w.cli.L2Book(
		hyperliquid.L2BookSubscriptionParams{Coin: coin},
		func(b hyperliquid.L2Book, err error) {
			if err != nil {
				w.log.Error("l2book callback error", "coin", coin, "err", err)
				return
			}
			cb(b)
		},
	)
	if err != nil {
		return fmt.Errorf("subscribe l2book %s: %w", coin, err)
	}
	return nil
}

func (w *WSClient) Close() error {
	return w.cli.Close()
}
