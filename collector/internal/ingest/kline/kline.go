// Package kline ingests Hyperliquid candle data into Postgres + Redis.
//
// Flow: HL WS callback -> in-memory channel -> batcher
//   - finalized bars (every configured interval) -> batched COPY into
//     klines_<interval> hypertables
//   - every bar (incl. in-progress) -> Redis SET + PUBLISH for live fan-out
//
// All intervals listed in cfg.Kline.Intervals are persisted to their own
// hypertable; there are no continuous aggregates anymore.
package kline

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	hyperliquid "github.com/sonirico/go-hyperliquid"

	"github.com/seabond/collector/internal/config"
	"github.com/seabond/collector/internal/db"
	"github.com/seabond/collector/internal/hl"

	"github.com/jackc/pgx/v5/pgxpool"
)

const domainName = "kline"

type Ingester struct {
	cfg   config.KlineConfig
	ws    *hl.WSClient
	pool  *pgxpool.Pool
	cache *db.Redis
	log   *slog.Logger

	in chan db.Kline
}

func New(cfg config.KlineConfig, ws *hl.WSClient, pool *pgxpool.Pool, c *db.Redis, log *slog.Logger) *Ingester {
	return &Ingester{
		cfg:   cfg,
		ws:    ws,
		pool:  pool,
		cache: c,
		log:   log,
		in:    make(chan db.Kline, cfg.Batch.Size*4),
	}
}

func (i *Ingester) Name() string { return domainName }

// coverageDomain scopes (earliest, latest) tracking per interval, e.g.
// "kline_1m", "kline_1h". Per-interval is needed now that each interval is
// independently backfilled and live-ingested.
func coverageDomain(interval string) string { return domainName + "_" + interval }

// Run subscribes to all configured coins and starts the batcher.
// Returns when ctx is cancelled.
func (i *Ingester) Run(ctx context.Context) error {
	if !i.cfg.Enabled {
		i.log.Info("kline ingester disabled")
		<-ctx.Done()
		return nil
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		i.runBatcher(ctx)
	}()

	for _, coin := range i.cfg.Coins {
		for _, interval := range i.cfg.Intervals {
			if db.KlineTable(interval) == "" {
				i.log.Warn("skipping unsupported interval", "coin", coin, "interval", interval)
				continue
			}
			coin, interval := coin, interval
			err := i.ws.SubscribeCandles(coin, interval, func(c hyperliquid.Candle) {
				i.handle(ctx, c)
			})
			if err != nil {
				i.log.Error("subscribe failed", "coin", coin, "interval", interval, "err", err)
				continue
			}
			i.log.Info("subscribed", "coin", coin, "interval", interval)
		}
	}

	<-ctx.Done()
	wg.Wait()
	return nil
}

// handle is invoked for every candle update from HL. Each (coin, ts) may fire
// many times before close; we cache + publish every one, but only enqueue
// finalized bars for DB insert.
func (i *Ingester) handle(ctx context.Context, c hyperliquid.Candle) {
	openTs := time.UnixMilli(c.TimeOpen).UTC()
	closeTs := time.UnixMilli(c.TimeClose).UTC()

	// "Closed" heuristic: HL stamps T = open + interval. Treat the bar as
	// finalized once wall-clock has passed close time. Duplicate (coin, ts)
	// entries can land in one batch; UpsertKlines dedupes (last-write-wins).
	closed := time.Now().UTC().After(closeTs)

	// Cache + publish live update for every tick.
	if err := i.cache.PublishLiveKline(ctx, db.LiveKline{
		TimeOpen:  c.TimeOpen,
		TimeClose: c.TimeClose,
		Symbol:    c.Symbol,
		Interval:  c.Interval,
		Open:      c.Open,
		High:      c.High,
		Low:       c.Low,
		Close:     c.Close,
		Volume:    c.Volume,
		Trades:    c.TradesCount,
		Closed:    closed,
	}); err != nil {
		i.log.Warn("publish live kline failed", "coin", c.Symbol, "err", err)
	}

	if !closed {
		return
	}
	if db.KlineTable(c.Interval) == "" {
		return
	}

	select {
	case i.in <- db.Kline{
		Interval: c.Interval,
		Coin:     c.Symbol,
		TS:       openTs,
		Open:     c.Open,
		High:     c.High,
		Low:      c.Low,
		Close:    c.Close,
		Volume:   c.Volume,
		Trades:   c.TradesCount,
	}:
	case <-ctx.Done():
	default:
		// Backpressure: batcher is behind. Drop and rely on backfill to fill the gap.
		i.log.Warn("kline channel full, dropping", "coin", c.Symbol, "interval", c.Interval, "ts", openTs)
	}
}

// runBatcher flushes when the batch fills or the timer fires.
func (i *Ingester) runBatcher(ctx context.Context) {
	flushEvery := time.Duration(i.cfg.Batch.FlushMs) * time.Millisecond
	ticker := time.NewTicker(flushEvery)
	defer ticker.Stop()

	buf := make([]db.Kline, 0, i.cfg.Batch.Size)
	flush := func() {
		if len(buf) == 0 {
			return
		}
		fctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := db.UpsertKlines(fctx, i.pool, buf); err != nil {
			i.log.Error("flush klines failed", "n", len(buf), "err", err)
			// Keep the buffer for the next attempt rather than losing data.
			return
		}
		i.log.Debug("flushed klines", "n", len(buf))
		// Track per-(interval, coin) (earliest, latest) so coverage widens monotonically.
		type key struct{ interval, coin string }
		type span struct{ first, last time.Time }
		spans := map[key]*span{}
		for _, k := range buf {
			kk := key{k.Interval, k.Coin}
			s, ok := spans[kk]
			if !ok {
				spans[kk] = &span{first: k.TS, last: k.TS}
				continue
			}
			if k.TS.Before(s.first) {
				s.first = k.TS
			}
			if k.TS.After(s.last) {
				s.last = k.TS
			}
		}
		for kk, s := range spans {
			if err := db.UpdateCoverageRange(fctx, i.pool, coverageDomain(kk.interval), kk.coin, s.first, s.last); err != nil {
				i.log.Warn("update coverage failed", "coin", kk.coin, "interval", kk.interval, "err", err)
			}
		}
		buf = buf[:0]
	}

	for {
		select {
		case <-ctx.Done():
			flush()
			return
		case <-ticker.C:
			flush()
		case k := <-i.in:
			buf = append(buf, k)
			if len(buf) >= i.cfg.Batch.Size {
				flush()
			}
		}
	}
}

// backfillRequestDelay throttles between candleSnapshot calls to stay under
// HL's REST rate limit. The /info endpoint shares a global weight budget per
// IP, so even modest fan-out (3 coins × 7 intervals) needs spacing.
const backfillRequestDelay = 250 * time.Millisecond

// Backfill pulls history via REST candleSnapshot for every configured (coin,
// interval). Pages backwards from `to` in 5000-bar windows until `from` is
// reached or the API returns an empty page (HL caps history per interval).
func (i *Ingester) Backfill(ctx context.Context, rest *hl.RESTClient, from, to time.Time) error {
	for _, coin := range i.cfg.Coins {
		for _, interval := range i.cfg.Intervals {
			if db.KlineTable(interval) == "" {
				i.log.Warn("backfill skip unsupported interval", "interval", interval)
				continue
			}
			if err := i.backfillCoinInterval(ctx, rest, coin, interval, from, to); err != nil {
				i.log.Error("backfill failed", "coin", coin, "interval", interval, "err", err)
			}
		}
	}
	return nil
}

func (i *Ingester) backfillCoinInterval(ctx context.Context, rest *hl.RESTClient, coin, interval string, from, to time.Time) error {
	const maxBars = 5000
	intervalMs, err := intervalToMs(interval)
	if err != nil {
		return err
	}
	windowMs := int64(maxBars) * intervalMs
	endMs := to.UnixMilli()
	startFloorMs := from.UnixMilli()

	for endMs > startFloorMs {
		startMs := endMs - windowMs
		if startMs < startFloorMs {
			startMs = startFloorMs
		}
		raw, err := rest.CandlesSnapshot(ctx, coin, interval, startMs, endMs)
		if err != nil {
			return fmt.Errorf("snapshot %s %s [%d,%d]: %w", coin, interval, startMs, endMs, err)
		}
		if len(raw) == 0 {
			break
		}
		rows := make([]db.Kline, 0, len(raw))
		var pageFirst, pageLast time.Time
		for idx, r := range raw {
			ts := time.UnixMilli(r.TimeOpen).UTC()
			if idx == 0 {
				pageFirst, pageLast = ts, ts
			} else {
				if ts.Before(pageFirst) {
					pageFirst = ts
				}
				if ts.After(pageLast) {
					pageLast = ts
				}
			}
			rows = append(rows, db.Kline{
				Interval: interval,
				Coin:     r.Symbol,
				TS:       ts,
				Open:     r.Open, High: r.High, Low: r.Low, Close: r.Close,
				Volume: r.Volume,
				Trades: r.TradesCount,
			})
		}
		if err := db.UpsertKlines(ctx, i.pool, rows); err != nil {
			return fmt.Errorf("upsert backfill: %w", err)
		}
		if err := db.UpdateCoverageRange(ctx, i.pool, coverageDomain(interval), coin, pageFirst, pageLast); err != nil {
			i.log.Warn("update coverage failed", "coin", coin, "interval", interval, "err", err)
		}
		i.log.Info("backfill page", "coin", coin, "interval", interval, "n", len(rows), "endMs", endMs)
		endMs = startMs

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backfillRequestDelay):
		}
	}
	return nil
}

// intervalToMs converts an HL interval string to milliseconds. Covers the set
// supported by db.KlineTable.
func intervalToMs(s string) (int64, error) {
	switch s {
	case "1d":
		return int64(24 * time.Hour / time.Millisecond), nil
	case "1mo":
		// 30 days is a reasonable approximation for sizing the snapshot window;
		// HL's actual month boundaries are calendar-aligned.
		return int64(30 * 24 * time.Hour / time.Millisecond), nil
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, fmt.Errorf("unsupported interval %q", s)
	}
	return int64(d / time.Millisecond), nil
}
