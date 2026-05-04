// Package retention prunes aged rows from the kline hypertables in batches.
//
// The janitor runs as a long-lived goroutine inside the collector process. On
// each tick it walks the configured tables and deletes rows older than
// KeepDays in BatchSize chunks until either no rows match or MaxRounds is
// reached, then sleeps until the next tick.
//
// Note on TimescaleDB compression: klines_1m / 5m / 15m have compression
// policies at 30 days, so when KeepDays is set to 30 the rows being deleted
// have just (or are about to be) moved to columnar storage. Modern Timescale
// (>= 2.11) supports DELETE on compressed chunks but it's much slower than
// chunk-level operations. For pure efficiency, prefer Timescale's
// `add_retention_policy` (which calls `drop_chunks`); the row-level batched
// DELETE here exists because it gives explicit, observable control over
// blast radius (BatchSize × MaxRounds caps the work per sweep).
package retention

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/seabond/collector/internal/config"
	"github.com/seabond/collector/internal/metrics"
)

// allowedTables is the SQL-injection guard. The table name is interpolated
// into the DELETE statement, so we must reject anything not in this set.
var allowedTables = map[string]struct{}{
	"klines_1m":  {},
	"klines_5m":  {},
	"klines_15m": {},
	"klines_1h":  {},
	"klines_4h":  {},
	"klines_1d":  {},
	"klines_1mo": {},
}

type Janitor struct {
	cfg  config.RetentionConfig
	pool *pgxpool.Pool
	log  *slog.Logger
}

func New(cfg config.RetentionConfig, pool *pgxpool.Pool, log *slog.Logger) *Janitor {
	return &Janitor{cfg: cfg, pool: pool, log: log}
}

// Run sweeps on a fixed interval until ctx cancels. Returns nil on shutdown
// — individual sweep errors are logged, not propagated, so a transient PG
// hiccup doesn't take down the collector.
func (j *Janitor) Run(ctx context.Context) error {
	if !j.cfg.Enabled {
		j.log.Info("retention disabled")
		<-ctx.Done()
		return nil
	}
	if err := j.validateTables(); err != nil {
		j.log.Error("retention disabled: invalid table list", "err", err)
		<-ctx.Done()
		return nil
	}

	interval := time.Duration(j.cfg.IntervalMs) * time.Millisecond
	policySummary := make([]string, 0, len(j.cfg.Tables))
	for _, t := range j.cfg.Tables {
		policySummary = append(policySummary, fmt.Sprintf("%s=%dd", t.Name, t.KeepDays))
	}
	j.log.Info("retention starting",
		"policies", policySummary,
		"batch_size", j.cfg.BatchSize,
		"interval", interval,
		"max_rounds", j.cfg.MaxRounds,
	)

	// Sweep once on startup so a fresh boot doesn't sit on stale data for
	// `interval` before doing any work.
	j.sweepAll(ctx)

	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			j.sweepAll(ctx)
		}
	}
}

func (j *Janitor) validateTables() error {
	for _, t := range j.cfg.Tables {
		if _, ok := allowedTables[t.Name]; !ok {
			return fmt.Errorf("table %q not in allowlist", t.Name)
		}
		if t.KeepDays <= 0 {
			return fmt.Errorf("table %q has non-positive keep_days=%d", t.Name, t.KeepDays)
		}
	}
	return nil
}

func (j *Janitor) sweepAll(ctx context.Context) {
	now := time.Now().UTC()
	for _, t := range j.cfg.Tables {
		if err := ctx.Err(); err != nil {
			return
		}
		cutoff := now.Add(-time.Duration(t.KeepDays) * 24 * time.Hour)
		j.sweep(ctx, t.Name, cutoff)
	}
}

func (j *Janitor) sweep(ctx context.Context, table string, cutoff time.Time) {
	start := time.Now()
	defer func() {
		metrics.RetentionSweepDuration.WithLabelValues(table).Observe(time.Since(start).Seconds())
	}()

	var total int64
	for round := 0; round < j.cfg.MaxRounds; round++ {
		if err := ctx.Err(); err != nil {
			return
		}
		n, err := j.deleteBatch(ctx, table, cutoff)
		if err != nil {
			j.log.Error("retention delete failed",
				"table", table, "round", round, "deleted_so_far", total, "err", err)
			return
		}
		if n == 0 {
			break
		}
		total += n
		metrics.RetentionDeleted.WithLabelValues(table).Add(float64(n))
	}
	if total > 0 {
		j.log.Info("retention swept",
			"table", table, "deleted", total, "cutoff", cutoff, "took", time.Since(start))
	}
}

// deleteBatch removes up to cfg.BatchSize oldest rows where ts < cutoff.
// (coin, ts) is the primary key of every klines_* hypertable, so the USING
// join is index-driven and stays within one chunk per round when possible.
func (j *Janitor) deleteBatch(ctx context.Context, table string, cutoff time.Time) (int64, error) {
	if _, ok := allowedTables[table]; !ok {
		return 0, fmt.Errorf("table %q not in allowlist", table)
	}
	sql := fmt.Sprintf(`
		WITH victims AS (
			SELECT coin, ts
			FROM %s
			WHERE ts < $1
			ORDER BY ts ASC
			LIMIT $2
		)
		DELETE FROM %s k
		USING victims
		WHERE k.coin = victims.coin AND k.ts = victims.ts
	`, table, table)
	tag, err := j.pool.Exec(ctx, sql, cutoff, j.cfg.BatchSize)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}
