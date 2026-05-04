package repository

import (
	"context"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/seabond/api/internal/api/models"
)

// MyFillsRepository reads from the collector-populated user_fills table. The
// API never writes here — collector's `userfills` ingester owns ingestion.
type MyFillsRepository interface {
	Query(ctx context.Context, addr string, from, to time.Time, limit int) ([]models.UserFill, error)
}

type myFillsPgRepo struct{ pool *pgxpool.Pool }

func NewMyFillsRepository(pool *pgxpool.Pool) MyFillsRepository {
	return &myFillsPgRepo{pool: pool}
}

// Query returns fills in (from, to] for `addr`, newest first, capped at limit.
// Numeric columns are pulled as text to preserve precision; we then trim
// trailing zeros to match HL's wire format.
func (r *myFillsPgRepo) Query(ctx context.Context, addr string, from, to time.Time, limit int) ([]models.UserFill, error) {
	const sql = `
		SELECT tid, time, coin, side,
		       px::text, sz::text, fee::text, closed_pnl::text,
		       COALESCE(start_pos::text, ''),
		       COALESCE(dir, ''),
		       oid,
		       COALESCE(hash, ''),
		       crossed
		FROM user_fills
		WHERE user_addr = $1 AND time > $2 AND time <= $3
		ORDER BY time DESC, tid DESC
		LIMIT $4
	`
	rows, err := r.pool.Query(ctx, sql, strings.ToLower(addr), from, to, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]models.UserFill, 0, 64)
	for rows.Next() {
		var f models.UserFill
		var ts time.Time
		if err := rows.Scan(
			&f.Tid, &ts, &f.Coin, &f.Side,
			&f.Px, &f.Sz, &f.Fee, &f.ClosedPnl,
			&f.StartPosition, &f.Dir, &f.Oid, &f.Hash, &f.Crossed,
		); err != nil {
			return nil, err
		}
		f.Time = ts.UnixMilli()
		f.Px = trimZeros(f.Px)
		f.Sz = trimZeros(f.Sz)
		f.Fee = trimZeros(f.Fee)
		f.ClosedPnl = trimZeros(f.ClosedPnl)
		if f.StartPosition != "" {
			f.StartPosition = trimZeros(f.StartPosition)
		}
		out = append(out, f)
	}
	return out, rows.Err()
}
