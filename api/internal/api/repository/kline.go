package repository

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/seabond/api/internal/api/models"
)

// trimZeros normalizes numeric(38,18)-encoded text into HL's compact form:
// strips trailing zeros from the fractional part, but always preserves at
// least one decimal digit so integers come out "1.0" not "1" (matching HL).
func trimZeros(s string) string {
	if !strings.Contains(s, ".") {
		return s + ".0"
	}
	s = strings.TrimRight(s, "0")
	if strings.HasSuffix(s, ".") {
		return s + "0"
	}
	return s
}

// KlineRepository defines data-access for klines and their continuous
// aggregates. Implementations are expected to validate the interval against
// IsValidInterval before issuing a query.
type KlineRepository interface {
	IsValidInterval(interval string) bool
	QueryKlines(ctx context.Context, coin, interval string, from, to time.Time, limit int) ([]models.Kline, error)
}

type klineRepo struct {
	pool *pgxpool.Pool
}

// NewKlineRepository returns a KlineRepository backed by the given pgx pool.
func NewKlineRepository(pool *pgxpool.Pool) KlineRepository {
	return &klineRepo{pool: pool}
}

// tableFor maps an interval string to the matching hypertable.
// Returning an empty string signals "unsupported".
func tableFor(interval string) string {
	switch interval {
	case "1m":
		return "klines_1m"
	case "5m":
		return "klines_5m"
	case "15m":
		return "klines_15m"
	case "1h":
		return "klines_1h"
	case "4h":
		return "klines_4h"
	case "1d":
		return "klines_1d"
	case "1mo":
		return "klines_1mo"
	}
	return ""
}

// intervalMs returns the bar duration in milliseconds; used to compute
// TimeClose = TimeOpen + intervalMs in the wire response (HL convention).
// 1mo is approximated as 30 days here — DB bucket boundaries are exact, this
// is only the on-the-wire close time for the displayed bar.
func intervalMs(interval string) int64 {
	const (
		minute = int64(60_000)
		hour   = 60 * minute
		day    = 24 * hour
	)
	switch interval {
	case "1m":
		return minute
	case "5m":
		return 5 * minute
	case "15m":
		return 15 * minute
	case "1h":
		return hour
	case "4h":
		return 4 * hour
	case "1d":
		return day
	case "1mo":
		return 30 * day
	}
	return minute
}

func (r *klineRepo) IsValidInterval(interval string) bool {
	return tableFor(interval) != ""
}

// QueryKlines returns rows in ascending time order. Caller must clamp limit.
// The returned Klines mirror Hyperliquid's wire shape (t/T/s/i/o/h/l/c/v/n).
func (r *klineRepo) QueryKlines(ctx context.Context, coin, interval string, from, to time.Time, limit int) ([]models.Kline, error) {
	table := tableFor(interval)
	if table == "" {
		return nil, fmt.Errorf("unsupported interval %q", interval)
	}
	// Table name is whitelisted above so direct interpolation is safe.
	q := fmt.Sprintf(`
		SELECT ts, open::text, high::text, low::text, close::text, volume::text, trades
		FROM %s
		WHERE coin = $1 AND ts >= $2 AND ts < $3
		ORDER BY ts ASC
		LIMIT $4
	`, table)

	rows, err := r.pool.Query(ctx, q, coin, from, to, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	stepMs := intervalMs(interval)
	out := make([]models.Kline, 0, limit)
	for rows.Next() {
		var ts time.Time
		k := models.Kline{Symbol: coin, Interval: interval}
		if err := rows.Scan(&ts, &k.Open, &k.High, &k.Low, &k.Close, &k.Volume, &k.Trades); err != nil {
			return nil, err
		}
		k.TimeOpen = ts.UnixMilli()
		k.TimeClose = k.TimeOpen + stepMs
		k.Open = trimZeros(k.Open)
		k.High = trimZeros(k.High)
		k.Low = trimZeros(k.Low)
		k.Close = trimZeros(k.Close)
		k.Volume = trimZeros(k.Volume)
		out = append(out, k)
	}
	return out, rows.Err()
}
