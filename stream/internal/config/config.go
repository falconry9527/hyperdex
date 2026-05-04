package config

import (
	"fmt"
	"os"

	"github.com/BurntSushi/toml"
)

// Config is the stream service config — only HTTP listener and Redis client.
// No Postgres dependency: stream reads exclusively from Redis pub/sub + cache.
type Config struct {
	HTTP  HTTPConfig  `toml:"http"`
	Redis RedisConfig `toml:"redis"`
}

type HTTPConfig struct {
	Addr string `toml:"addr"`
}

type RedisConfig struct {
	Host        string `toml:"host"`
	Username    string `toml:"username"`
	Password    string `toml:"password"`
	DB          int    `toml:"db"`
	MaxIdle     int    `toml:"max-idle"`
	MaxActive   int    `toml:"max-active"`
	IdleTimeout int    `toml:"idle-timeout"`
	TLS         bool   `toml:"tls"`
}

// Load reads cfg from path and applies env overrides. Recognised env vars:
// REDIS_HOST, REDIS_PASSWORD.
func Load(path string) (*Config, error) {
	var cfg Config
	if _, err := toml.DecodeFile(path, &cfg); err != nil {
		return nil, fmt.Errorf("decode config %s: %w", path, err)
	}
	if v := os.Getenv("REDIS_HOST"); v != "" {
		cfg.Redis.Host = v
	}
	if v := os.Getenv("REDIS_PASSWORD"); v != "" {
		cfg.Redis.Password = v
	}
	return &cfg, nil
}
