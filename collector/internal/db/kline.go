package db

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Kline is a finalized bar ready to be persisted. Interval selects the target
// hypertable (klines_<interval>); see klineTables for the supported set.
type Kline struct {
	Interval string
	Coin     string
	TS       time.Time
	Open     string
	High     string
	Low      string
	Close    string
	Volume   string
	Trades   int
}

// klineTables whitelists the supported interval → table names. Used both to
// validate input and to interpolate the table name into upsert SQL safely.
var klineTables = map[string]string{
	"1m":  "klines_1m",
	"5m":  "klines_5m",
	"15m": "klines_15m",
	"1h":  "klines_1h",
	"4h":  "klines_4h",
	"1d":  "klines_1d",
	"1mo": "klines_1mo",
}

// KlineTable returns the hypertable name for an interval, or "" if unsupported.
func KlineTable(interval string) string { return klineTables[interval] }

// UpsertKlines groups rows by interval and upserts each group into its
// hypertable via CopyFrom + ON CONFLICT. Rows missing Interval are rejected.
func UpsertKlines(ctx context.Context, pool *pgxpool.Pool, rows []Kline) error {
	if len(rows) == 0 {
		return nil
	}
	byInterval := make(map[string][]Kline, 4)
	for _, r := range rows {
		if r.Interval == "" {
			return fmt.Errorf("kline missing interval: coin=%s ts=%s", r.Coin, r.TS)
		}
		byInterval[r.Interval] = append(byInterval[r.Interval], r)
	}
	for interval, sub := range byInterval {
		if err := upsertOne(ctx, pool, interval, sub); err != nil {
			return err
		}
	}
	return nil
}

func upsertOne(ctx context.Context, pool *pgxpool.Pool, interval string, rows []Kline) error {
	table, ok := klineTables[interval]
	if !ok {
		return fmt.Errorf("unsupported kline interval %q", interval)
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, fmt.Sprintf(`
		CREATE TEMP TABLE _klines_stage (LIKE %s INCLUDING DEFAULTS)
		ON COMMIT DROP
	`, table)); err != nil {
		return fmt.Errorf("create temp: %w", err)
	}

	_, err = tx.CopyFrom(ctx,
		pgx.Identifier{"_klines_stage"},
		[]string{"coin", "ts", "open", "high", "low", "close", "volume", "trades"},
		pgx.CopyFromSlice(len(rows), func(i int) ([]any, error) {
			r := rows[i]
			return []any{r.Coin, r.TS, r.Open, r.High, r.Low, r.Close, r.Volume, r.Trades}, nil
		}),
	)
	if err != nil {
		return fmt.Errorf("copyfrom stage: %w", err)
	}

	// DISTINCT ON collapses duplicate (coin, ts) rows in the batch — without
	// it, ON CONFLICT errors with "cannot affect row a second time". ctid DESC
	// keeps the last-copied row, which is the most recent observation.
	if _, err := tx.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s (coin, ts, open, high, low, close, volume, trades)
		SELECT DISTINCT ON (coin, ts) coin, ts, open, high, low, close, volume, trades
		FROM _klines_stage
		ORDER BY coin, ts, ctid DESC
		ON CONFLICT (coin, ts) DO UPDATE SET
			open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low,
			close=EXCLUDED.close, volume=EXCLUDED.volume, trades=EXCLUDED.trades
	`, table)); err != nil {
		return fmt.Errorf("merge stage: %w", err)
	}

	return tx.Commit(ctx)
}

// UpdateCoverageRange expands the (earliest, latest) range for (domain, coin).
// Monotonic: callers can pass any ts within their batch and the row keeps
// widening to cover everything observed so far.
func UpdateCoverageRange(ctx context.Context, pool *pgxpool.Pool, domain, coin string, earliest, latest time.Time) error {
	_, err := pool.Exec(ctx, `
		INSERT INTO coverage (domain, coin, earliest, latest)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (domain, coin) DO UPDATE SET
			earliest = LEAST(coverage.earliest, EXCLUDED.earliest),
			latest   = GREATEST(coverage.latest, EXCLUDED.latest),
			updated_at = now()
	`, domain, coin, earliest, latest)
	return err
}
