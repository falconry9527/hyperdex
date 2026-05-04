package config

import (
	"fmt"
	"net/url"
	"os"

	"github.com/BurntSushi/toml"
)

type Config struct {
	HL       HLConfig       `toml:"hl"`
	Postgres PostgresConfig `toml:"postgres"`
	Redis    RedisConfig    `toml:"redis"`
	Admin    AdminConfig    `toml:"admin"`
	Kline    KlineConfig    `toml:"kline"`
	L2Book   L2BookConfig   `toml:"l2book"`
	Ticker    TickerConfig    `toml:"ticker"`
	Trades    TradesConfig    `toml:"trades"`
	UserFills UserFillsConfig `toml:"userfills"`
	Retention RetentionConfig `toml:"retention"`
}

type HLConfig struct {
	APIURL string `toml:"api_url"`
	WSURL  string `toml:"ws_url"`
}

type PostgresConfig struct {
	MaxOpenConns int               `toml:"max-open-conns"`
	MaxIdleConns int               `toml:"max-idle-conns"`
	Host         string            `toml:"host"` // "127.0.0.1:5432"
	User         string            `toml:"user"`
	Password     string            `toml:"password"`
	Database     string            `toml:"database"`
	TablePrefix  string            `toml:"table-prefix"`
	Params       map[string]string `toml:"params"`
	DSN          string            `toml:"dsn"` // optional: full override
}

// BuildDSN composes a libpq URL from explicit fields, or returns the override.
func (p PostgresConfig) BuildDSN() string {
	if p.DSN != "" {
		return p.DSN
	}
	u := &url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(p.User, p.Password),
		Host:   p.Host,
		Path:   "/" + p.Database,
	}
	q := u.Query()
	for k, v := range p.Params {
		q.Set(k, v)
	}
	if q.Get("sslmode") == "" {
		q.Set("sslmode", "disable")
	}
	u.RawQuery = q.Encode()
	return u.String()
}

type RedisConfig struct {
	Host        string `toml:"host"`
	Username    string `toml:"username"`
	Password    string `toml:"password"`
	DB          int    `toml:"db"`
	MaxIdle     int    `toml:"max-idle"`
	MaxActive   int    `toml:"max-active"`
	IdleTimeout int    `toml:"idle-timeout"` // seconds
	TLS         bool   `toml:"tls"`
}

type AdminConfig struct {
	Addr string `toml:"addr"`
}

type KlineConfig struct {
	Enabled bool `toml:"enabled"`
	// Intervals to subscribe on HL websocket. Each (coin, interval) pair gets
	// its own subscription and publishes to its own Redis channel. Only "1m"
	// bars are persisted to klines_1m; higher intervals are CAGG-derived in
	// the DB and live-only in Redis.
	Intervals []string    `toml:"intervals"`
	Coins     []string    `toml:"coins"`
	Batch     BatchConfig `toml:"batch"`
}

type BatchConfig struct {
	Size    int `toml:"size"`
	FlushMs int `toml:"flush_ms"`
}

// L2BookConfig configures the live-only L2 order book ingester. Snapshots are
// not persisted — they're only cached + published to Redis for ws fan-out.
type L2BookConfig struct {
	Enabled bool     `toml:"enabled"`
	Coins   []string `toml:"coins"`
}

// TickerConfig configures the live-only ticker (activeAssetCtx) ingester:
// last/mark/mid price, 24h volume, funding for the top bar.
type TickerConfig struct {
	Enabled bool     `toml:"enabled"`
	Coins   []string `toml:"coins"`
}

// TradesConfig configures the public tape ingester. A rolling window of recent
// trades is cached so new ws subscribers see history immediately.
type TradesConfig struct {
	Enabled bool     `toml:"enabled"`
	Coins   []string `toml:"coins"`
}

// UserFillsConfig configures the on-demand fill-history syncer. Consumed by
// `cmd/sync-fills` — there's no long-running ingester. Browser-side realtime
// UI subscribes to HL ws directly; this is the persistence layer behind it.
type UserFillsConfig struct {
	Addrs        []string `toml:"addrs"`         // 0x…  (case-insensitive; stored lowercase)
	BackfillDays int      `toml:"backfill_days"` // default 90 — only when DB has nothing newer
}

// RetentionConfig drives the in-process janitor that prunes old kline rows.
// Tables are whitelisted in the retention package — the toml field is just
// a per-deployment opt-in subset.
type RetentionConfig struct {
	Enabled    bool                     `toml:"enabled"`
	Tables     []TableRetentionConfig   `toml:"tables"`      // per-table policies
	KeepDays   int                      `toml:"keep_days"`   // default when a table entry has KeepDays=0
	BatchSize  int                      `toml:"batch_size"`  // rows per DELETE statement
	IntervalMs int                      `toml:"interval_ms"` // gap between sweeps
	MaxRounds  int                      `toml:"max_rounds"`  // safety cap on batches per sweep per table
}

// TableRetentionConfig binds a hypertable name to its retention window. Name
// must be in the retention package's allowlist; KeepDays defaults to the
// parent RetentionConfig.KeepDays when zero.
type TableRetentionConfig struct {
	Name     string `toml:"name"`
	KeepDays int    `toml:"keep_days"`
}

func Load(path string) (*Config, error) {
	var cfg Config
	if _, err := toml.DecodeFile(path, &cfg); err != nil {
		return nil, fmt.Errorf("decode config %s: %w", path, err)
	}
	cfg.applyEnvOverrides()
	if cfg.Postgres.MaxOpenConns == 0 {
		cfg.Postgres.MaxOpenConns = 10
	}
	if cfg.Kline.Batch.Size == 0 {
		cfg.Kline.Batch.Size = 500
	}
	if cfg.Kline.Batch.FlushMs == 0 {
		cfg.Kline.Batch.FlushMs = 500
	}
	if cfg.UserFills.BackfillDays == 0 {
		cfg.UserFills.BackfillDays = 90
	}
	if cfg.Retention.KeepDays == 0 {
		cfg.Retention.KeepDays = 30
	}
	if cfg.Retention.BatchSize == 0 {
		cfg.Retention.BatchSize = 1000
	}
	if cfg.Retention.IntervalMs == 0 {
		cfg.Retention.IntervalMs = 3600_000 // 1h
	}
	if cfg.Retention.MaxRounds == 0 {
		cfg.Retention.MaxRounds = 200 // batch_size 1000 × 200 = 200k rows/table/sweep cap
	}
	if len(cfg.Retention.Tables) == 0 {
		cfg.Retention.Tables = []TableRetentionConfig{
			{Name: "klines_1m", KeepDays: 30},
			{Name: "klines_5m", KeepDays: 30},
			{Name: "klines_15m", KeepDays: 30},
			{Name: "klines_1h", KeepDays: 90},
			{Name: "klines_4h", KeepDays: 90},
			{Name: "klines_1d", KeepDays: 730},   // ~24 months
			{Name: "klines_1mo", KeepDays: 1095}, // ~3 years
		}
	}
	for i := range cfg.Retention.Tables {
		if cfg.Retention.Tables[i].KeepDays == 0 {
			cfg.Retention.Tables[i].KeepDays = cfg.Retention.KeepDays
		}
	}
	return &cfg, nil
}

func (c *Config) applyEnvOverrides() {
	if v := os.Getenv("HL_API_URL"); v != "" {
		c.HL.APIURL = v
	}
	if v := os.Getenv("HL_WS_URL"); v != "" {
		c.HL.WSURL = v
	}
	if v := os.Getenv("POSTGRES_DSN"); v != "" {
		c.Postgres.DSN = v
	}
	if v := os.Getenv("POSTGRES_PASSWORD"); v != "" {
		c.Postgres.Password = v
	}
	if v := os.Getenv("REDIS_HOST"); v != "" {
		c.Redis.Host = v
	}
	if v := os.Getenv("REDIS_PASSWORD"); v != "" {
		c.Redis.Password = v
	}
}
