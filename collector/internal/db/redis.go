package db

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/seabond/collector/internal/config"
)

// LiveKline mirrors HL's wire candle shape (t/T/s/i/o/h/l/c/v/n) so that REST
// and WS responses are identical for the same bar. The extra `closed` flag is
// our own addition — HL doesn't expose it; we derive it from now > TimeClose
// in the ingester so the frontend can distinguish in-progress vs finalized
// without doing the comparison itself.

// Redis wraps a *redis.Client with the write-side helpers the collector needs:
// caching the most recent bar and publishing it to per-channel pub/sub streams.
type Redis struct {
	rdb *redis.Client
}

// NewRedis constructs a Redis client from cfg.
func NewRedis(cfg config.RedisConfig) *Redis {
	opts := &redis.Options{
		Addr:     cfg.Host,
		Username: cfg.Username,
		Password: cfg.Password,
		DB:       cfg.DB,
	}
	if cfg.MaxActive > 0 {
		opts.PoolSize = cfg.MaxActive
	}
	if cfg.MaxIdle > 0 {
		opts.MinIdleConns = cfg.MaxIdle
	}
	if cfg.IdleTimeout > 0 {
		opts.ConnMaxIdleTime = time.Duration(cfg.IdleTimeout) * time.Second
	}
	if cfg.TLS {
		opts.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	return &Redis{rdb: redis.NewClient(opts)}
}

func (c *Redis) Ping(ctx context.Context) error { return c.rdb.Ping(ctx).Err() }
func (c *Redis) Close() error                   { return c.rdb.Close() }

// LiveKline is the in-progress bar shape we cache + publish.
type LiveKline struct {
	TimeOpen  int64  `json:"t"`
	TimeClose int64  `json:"T"`
	Symbol    string `json:"s"`
	Interval  string `json:"i"`
	Open      string `json:"o"`
	High      string `json:"h"`
	Low       string `json:"l"`
	Close     string `json:"c"`
	Volume    string `json:"v"`
	Trades    int    `json:"n"`
	Closed    bool   `json:"closed"` // our extension; not in HL's wire shape
}

// LiveChannel is the Redis pub/sub channel name the collector publishes to for
// live (in-progress + finalized) bars of (coin, interval).
func LiveChannel(coin, interval string) string {
	return fmt.Sprintf("kline.%s.%s", coin, interval)
}

// LiveKey is the Redis key holding the most recent bar JSON for (coin, interval).
// TTL is 5 minutes; writes happen on every WS tick from the collector.
func LiveKey(coin, interval string) string {
	return fmt.Sprintf("kline:last:%s:%s", coin, interval)
}

func (c *Redis) PublishLiveKline(ctx context.Context, k LiveKline) error {
	b, err := json.Marshal(k)
	if err != nil {
		return err
	}
	pipe := c.rdb.Pipeline()
	pipe.Set(ctx, LiveKey(k.Symbol, k.Interval), b, 5*time.Minute)
	pipe.Publish(ctx, LiveChannel(k.Symbol, k.Interval), b)
	_, err = pipe.Exec(ctx)
	return err
}

// L2BookLiveChannel is the Redis pub/sub channel for live L2 book snapshots.
func L2BookLiveChannel(coin string) string {
	return fmt.Sprintf("l2book.%s", coin)
}

// L2BookLiveKey is the Redis key holding the most recent L2 book JSON for coin.
// TTL is 30s — L2 is high frequency, anything older than that is stale enough
// that a connecting client should see "no data" rather than a frozen book.
func L2BookLiveKey(coin string) string {
	return fmt.Sprintf("l2book:last:%s", coin)
}

// PublishLiveL2Book caches the latest snapshot and fans it out to subscribers.
// `payload` is the already-marshalled HL-native frame; passing pre-encoded
// bytes lets the caller marshal once even when N subscribers share the channel.
func (c *Redis) PublishLiveL2Book(ctx context.Context, coin string, payload []byte) error {
	pipe := c.rdb.Pipeline()
	pipe.Set(ctx, L2BookLiveKey(coin), payload, 30*time.Second)
	pipe.Publish(ctx, L2BookLiveChannel(coin), payload)
	_, err := pipe.Exec(ctx)
	return err
}

// ─── ticker (activeAssetCtx) ────────────────────────────────────────────────

func TickerLiveChannel(coin string) string { return fmt.Sprintf("ticker.%s", coin) }
func TickerLiveKey(coin string) string     { return fmt.Sprintf("ticker:last:%s", coin) }

// PublishLiveTicker caches the latest ticker frame and fans it out. TTL is
// 60s — ticker updates ~1 Hz, anything older than a minute is stale.
func (c *Redis) PublishLiveTicker(ctx context.Context, coin string, payload []byte) error {
	pipe := c.rdb.Pipeline()
	pipe.Set(ctx, TickerLiveKey(coin), payload, time.Minute)
	pipe.Publish(ctx, TickerLiveChannel(coin), payload)
	_, err := pipe.Exec(ctx)
	return err
}

// ─── trades (public tape) ───────────────────────────────────────────────────

// TradesRecentWindow is the number of recent trades cached per coin. New ws
// subscribers receive this window as their first frame so they don't see an
// empty tape until the next print.
const TradesRecentWindow = 50

func TradesLiveChannel(coin string) string  { return fmt.Sprintf("trades.%s", coin) }
func TradesRecentKey(coin string) string    { return fmt.Sprintf("trades:recent:%s", coin) }

// PublishLiveTrades appends the new batch to the rolling recent-trades list and
// publishes the batch to subscribers. Both snapshot (cache) and increment
// (channel) frames are JSON arrays of HL-native Trade objects, so subscribers
// can append-and-trim regardless of which they received.
//
// The caller passes batch as the marshalled HL-native array; the function
// reads + writes the cache as Redis LIST ops (LPUSH+LTRIM) keyed off the
// individual trade JSON for O(1) writes.
func (c *Redis) PublishLiveTrades(ctx context.Context, coin string, batch []byte, perTrade [][]byte) error {
	key := TradesRecentKey(coin)
	pipe := c.rdb.Pipeline()
	// Newest first via LPUSH; LTRIM keeps the list bounded.
	if len(perTrade) > 0 {
		args := make([]any, 0, len(perTrade))
		for _, b := range perTrade {
			args = append(args, b)
		}
		pipe.LPush(ctx, key, args...)
		pipe.LTrim(ctx, key, 0, TradesRecentWindow-1)
		pipe.Expire(ctx, key, 5*time.Minute)
	}
	pipe.Publish(ctx, TradesLiveChannel(coin), batch)
	_, err := pipe.Exec(ctx)
	return err
}
