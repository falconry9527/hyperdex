package db

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// UserFill is one row of HL trade history for a single address. Numeric fields
// arrive from HL as strings; we keep them as strings here to preserve precision
// when writing to numeric(38,18) columns.
type UserFill struct {
	UserAddr  string    // 0x… lowercase
	Tid       int64     // HL global trade id (unique)
	Time      time.Time // unix ms parsed
	Coin      string
	Side      string    // "B" or "A"
	Px        string
	Sz        string
	Fee       string
	ClosedPnl string
	StartPos  string
	Dir       string
	Oid       int64
	Hash      string
	Crossed   bool
	FeeToken  string
}

// UpsertUserFills inserts the batch into user_fills, ignoring conflicts on
// (user_addr, tid). Fills are immutable on HL, so we never want to overwrite
// existing rows — DO NOTHING is correct.
func UpsertUserFills(ctx context.Context, pool *pgxpool.Pool, rows []UserFill) error {
	if len(rows) == 0 {
		return nil
	}
	// Build a multi-VALUES insert. Batch sizes here are small (≤2000 from
	// one HL page), so a single statement is fine — no temp-table dance.
	const cols = 14
	args := make([]any, 0, cols*len(rows))
	placeholders := make([]string, 0, len(rows))
	for i, r := range rows {
		base := i * cols
		ph := make([]string, cols)
		for j := 0; j < cols; j++ {
			ph[j] = fmt.Sprintf("$%d", base+j+1)
		}
		placeholders = append(placeholders, "("+strings.Join(ph, ",")+")")
		args = append(args,
			strings.ToLower(r.UserAddr), r.Tid, r.Time, r.Coin, r.Side,
			r.Px, r.Sz, r.Fee, r.ClosedPnl, nullIfEmpty(r.StartPos),
			nullIfEmpty(r.Dir), r.Oid, nullIfEmpty(r.Hash), r.Crossed,
		)
		_ = r.FeeToken // not in the column list below — see note
	}
	// Note on fee_token: we accept it in UserFill but currently don't write it
	// since HL only ever returns "USDC" today. If multi-collateral lands we'll
	// expand the INSERT.

	sql := `
		INSERT INTO user_fills (
			user_addr, tid, time, coin, side,
			px, sz, fee, closed_pnl, start_pos,
			dir, oid, hash, crossed
		) VALUES ` + strings.Join(placeholders, ",") + `
		ON CONFLICT (user_addr, tid) DO NOTHING
	`
	if _, err := pool.Exec(ctx, sql, args...); err != nil {
		return fmt.Errorf("upsert user_fills: %w", err)
	}
	return nil
}

// LatestUserFillTime returns the most recent fill `time` we've stored for
// `addr`, or zero time if none. Used to scope incremental backfills.
func LatestUserFillTime(ctx context.Context, pool *pgxpool.Pool, addr string) (time.Time, error) {
	var ts *time.Time
	err := pool.QueryRow(ctx, `
		SELECT MAX(time) FROM user_fills WHERE user_addr = $1
	`, strings.ToLower(addr)).Scan(&ts)
	if err != nil {
		return time.Time{}, err
	}
	if ts == nil {
		return time.Time{}, nil
	}
	return *ts, nil
}

func nullIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}
