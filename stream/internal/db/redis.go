package db

import (
	"context"
	"crypto/tls"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/seabond/stream/internal/config"
)

// Redis wraps a *redis.Client with the read-side helpers stream needs:
// fetching the cached "last live bar" and subscribing to per-channel pub/sub
// streams. Writes are owned by the collector.
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

// LastLive returns the cached most recent bar payload, or nil if absent.
func (c *Redis) LastLive(ctx context.Context, coin, interval string) ([]byte, error) {
	v, err := c.rdb.Get(ctx, LiveKey(coin, interval)).Bytes()
	if err == redis.Nil {
		return nil, nil
	}
	return v, err
}

// L2BookLiveChannel mirrors collector's pub channel for live L2 snapshots.
func L2BookLiveChannel(coin string) string {
	return fmt.Sprintf("l2book.%s", coin)
}

// L2BookLiveKey mirrors collector's cache key for the most recent L2 snapshot.
// TTL on the write side is 30s — anything older is treated as stale.
func L2BookLiveKey(coin string) string {
	return fmt.Sprintf("l2book:last:%s", coin)
}

// LastL2Book returns the cached most recent L2 snapshot, or nil if absent/stale.
func (c *Redis) LastL2Book(ctx context.Context, coin string) ([]byte, error) {
	v, err := c.rdb.Get(ctx, L2BookLiveKey(coin)).Bytes()
	if err == redis.Nil {
		return nil, nil
	}
	return v, err
}

// ─── ticker ─────────────────────────────────────────────────────────────────

func TickerLiveChannel(coin string) string { return fmt.Sprintf("ticker.%s", coin) }
func TickerLiveKey(coin string) string     { return fmt.Sprintf("ticker:last:%s", coin) }

// LastTicker returns the cached most recent ticker frame, or nil if absent.
func (c *Redis) LastTicker(ctx context.Context, coin string) ([]byte, error) {
	v, err := c.rdb.Get(ctx, TickerLiveKey(coin)).Bytes()
	if err == redis.Nil {
		return nil, nil
	}
	return v, err
}

// ─── trades (public tape) ───────────────────────────────────────────────────

func TradesLiveChannel(coin string) string { return fmt.Sprintf("trades.%s", coin) }
func TradesRecentKey(coin string) string   { return fmt.Sprintf("trades:recent:%s", coin) }

// LastTrades returns the cached recent-trades window for `coin` as a single
// JSON array (newest first matches LIST order). Returns nil if no cache yet.
//
// The list is stored newest-first via collector's LPUSH; we do the same here
// to keep the wire shape consistent with live increments (HL also pushes
// newest-first within each batch).
func (c *Redis) LastTrades(ctx context.Context, coin string) ([]byte, error) {
	items, err := c.rdb.LRange(ctx, TradesRecentKey(coin), 0, -1).Result()
	if err == redis.Nil || len(items) == 0 {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	// items are individual JSON-encoded Trade objects; concatenate into an array.
	buf := make([]byte, 0, 64*len(items))
	buf = append(buf, '[')
	for i, s := range items {
		if i > 0 {
			buf = append(buf, ',')
		}
		buf = append(buf, s...)
	}
	buf = append(buf, ']')
	return buf, nil
}

// Subscribe returns a Redis pub/sub handle for the given channels.
func (c *Redis) Subscribe(ctx context.Context, channels ...string) *redis.PubSub {
	return c.rdb.Subscribe(ctx, channels...)
}
