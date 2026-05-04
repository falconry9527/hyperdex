package config

import (
	"fmt"
	"net/url"
	"os"

	"github.com/BurntSushi/toml"
)

// Config is the api service config — HTTP listener + Postgres + a thin HL
// REST passthrough for /meta. Live (Redis-backed) endpoints live in the
// sibling stream service.
type Config struct {
	HTTP     HTTPConfig     `toml:"http"`
	Postgres PostgresConfig `toml:"postgres"`
	Kline    KlineConfig    `toml:"kline"`
	HL       HLConfig       `toml:"hl"`
	Meta     MetaConfig     `toml:"meta"`
	MyFills  MyFillsConfig  `toml:"myfills"`
}

// HLConfig points at Hyperliquid's REST endpoint. Only the api service uses
// this — collector has its own HL config.
type HLConfig struct {
	APIURL string `toml:"api_url"`
}

// MetaConfig tunes the /meta TTL cache. Asset universe rarely changes so a
// 5-minute TTL keeps load on HL near zero.
type MetaConfig struct {
	CacheTTLSec int `toml:"cache_ttl_sec"`
}

type HTTPConfig struct {
	Addr string `toml:"addr"`
}

type PostgresConfig struct {
	MaxOpenConns int               `toml:"max-open-conns"`
	MaxIdleConns int               `toml:"max-idle-conns"`
	Host         string            `toml:"host"`
	User         string            `toml:"user"`
	Password     string            `toml:"password"`
	Database     string            `toml:"database"`
	TablePrefix  string            `toml:"table-prefix"`
	Params       map[string]string `toml:"params"`
	DSN          string            `toml:"dsn"`
}

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

type KlineConfig struct {
	DefaultLimit int `toml:"default_limit"`
	MaxLimit     int `toml:"max_limit"`
}

// MyFillsConfig tunes the /myfills endpoint. Heavy traders can have thousands
// of fills, so we keep MaxLimit somewhat generous; the index on (user_addr,
// time DESC) keeps queries fast.
type MyFillsConfig struct {
	DefaultLimit int `toml:"default_limit"`
	MaxLimit     int `toml:"max_limit"`
}

func Load(path string) (*Config, error) {
	var cfg Config
	if _, err := toml.DecodeFile(path, &cfg); err != nil {
		return nil, fmt.Errorf("decode config %s: %w", path, err)
	}
	if v := os.Getenv("POSTGRES_DSN"); v != "" {
		cfg.Postgres.DSN = v
	}
	if v := os.Getenv("POSTGRES_PASSWORD"); v != "" {
		cfg.Postgres.Password = v
	}
	if cfg.Kline.DefaultLimit == 0 {
		cfg.Kline.DefaultLimit = 1500
	}
	if cfg.Kline.MaxLimit == 0 {
		cfg.Kline.MaxLimit = 5000
	}
	if cfg.Postgres.MaxOpenConns == 0 {
		cfg.Postgres.MaxOpenConns = 20
	}
	if cfg.HL.APIURL == "" {
		cfg.HL.APIURL = "https://api.hyperliquid.xyz"
	}
	if v := os.Getenv("HL_API_URL"); v != "" {
		cfg.HL.APIURL = v
	}
	if cfg.Meta.CacheTTLSec == 0 {
		cfg.Meta.CacheTTLSec = 300 // 5 min — universe rarely changes
	}
	if cfg.MyFills.DefaultLimit == 0 {
		cfg.MyFills.DefaultLimit = 500
	}
	if cfg.MyFills.MaxLimit == 0 {
		cfg.MyFills.MaxLimit = 5000
	}
	return &cfg, nil
}
