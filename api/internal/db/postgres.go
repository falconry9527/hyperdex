package db

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/seabond/api/internal/config"
)

// NewPostgres constructs a pgx connection pool from cfg, applying the user-
// supplied pool sizing and verifying the connection with a Ping.
func NewPostgres(ctx context.Context, pg config.PostgresConfig) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(pg.BuildDSN())
	if err != nil {
		return nil, fmt.Errorf("parse pg dsn: %w", err)
	}
	cfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeDescribeExec
	if pg.MaxOpenConns > 0 {
		cfg.MaxConns = int32(pg.MaxOpenConns)
	}
	if pg.MaxIdleConns > 0 {
		cfg.MinConns = int32(pg.MaxIdleConns)
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect pg: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping pg: %w", err)
	}
	return pool, nil
}
