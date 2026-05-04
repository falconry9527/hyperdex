// Package userfills syncs Hyperliquid user trade history into Postgres.
//
// HL only retains a few thousand recent fills server-side, so for long-term
// analytics (PnL curves, tax exports, win rate) we mirror them locally. Live
// realtime fills flow direct from browser to HL ws — this package is the
// persistence layer behind that, NOT a substitute.
//
// Designed as a one-shot operation: call Sync (or SyncAll) and it returns
// when caught up. Cron it, run it from a Makefile target, or trigger it ad-
// hoc — there's no long-running background loop, no ws subscription. This
// keeps the collector process focused on market data and lets fill history
// stay refresh-on-demand.
//
// Per address Sync does:
//
//  1. If no rows in DB for `addr` (or latest row is older than the cutoff),
//     paginate REST userFillsByTime by 7-day windows back to `BackfillDays`.
//     Pages that hit the 2000-row hard cap auto-bisect.
//  2. Always also pull REST userFills (most recent ~2000) — covers any gap
//     between the last backfill end and "now".
//
// Both paths UPSERT with ON CONFLICT (user_addr, tid) DO NOTHING so repeat
// runs are idempotent and cheap (PG short-circuits on the PK index).
package userfills

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/seabond/collector/internal/db"
	"github.com/seabond/collector/internal/hl"
)

// pageWindow is the time slice each userFillsByTime call asks for. HL caps
// each page at ~2000 fills; 7d is a safe default for typical retail volume.
// Pages that hit the cap are auto-bisected.
const pageWindow = 7 * 24 * time.Hour

// pageHardCap is HL's documented per-page row limit.
const pageHardCap = 2000

// Syncer brings PG fill history up to date for one or more addresses. It
// holds no state between calls — every method is safe to invoke
// concurrently for different addresses (single PG pool handles concurrency).
type Syncer struct {
	rest *hl.RESTClient
	pool *pgxpool.Pool
	log  *slog.Logger
}

func New(rest *hl.RESTClient, pool *pgxpool.Pool, log *slog.Logger) *Syncer {
	return &Syncer{rest: rest, pool: pool, log: log}
}

// SyncAll runs Sync for each addr sequentially. Sequential (not parallel)
// because the bottleneck is HL's IP-shared info-endpoint quota — paralleling
// only burns it faster without any wall-clock improvement.
func (s *Syncer) SyncAll(ctx context.Context, addrs []string, backfillDays int) error {
	for _, raw := range addrs {
		if err := s.Sync(ctx, strings.ToLower(raw), backfillDays); err != nil {
			s.log.Error("sync addr", "addr", raw, "err", err)
			// Don't abort — keep trying remaining addrs.
		}
	}
	return nil
}

// Sync brings PG up to date for one address. Always safe to re-run.
func (s *Syncer) Sync(ctx context.Context, addr string, backfillDays int) error {
	addr = strings.ToLower(addr)
	log := s.log.With("addr", addr)

	latest, err := db.LatestUserFillTime(ctx, s.pool, addr)
	if err != nil {
		return fmt.Errorf("query latest: %w", err)
	}
	cutoff := time.Now().UTC().Add(-time.Duration(backfillDays) * 24 * time.Hour)
	if latest.Before(cutoff) {
		from := cutoff
		if !latest.IsZero() && latest.After(from) {
			from = latest
		}
		log.Info("backfill start", "from", from)
		if err := s.backfill(ctx, addr, from, time.Now().UTC()); err != nil {
			return fmt.Errorf("backfill: %w", err)
		}
		log.Info("backfill done")
	}

	// Always also pull recent fills — the cheapest way to close any gap
	// between the backfill window and "now" without computing exact bounds.
	if err := s.pullRecent(ctx, addr); err != nil {
		return fmt.Errorf("pull recent: %w", err)
	}
	return nil
}

func (s *Syncer) pullRecent(ctx context.Context, addr string) error {
	raw, err := s.rest.UserFills(ctx, addr)
	if err != nil {
		return err
	}
	rows := convert(addr, raw)
	if err := db.UpsertUserFills(ctx, s.pool, rows); err != nil {
		return err
	}
	s.log.Info("pulled recent", "addr", addr, "n", len(rows))
	return nil
}

func (s *Syncer) backfill(ctx context.Context, addr string, from, to time.Time) error {
	winStart := from
	for winStart.Before(to) {
		winEnd := winStart.Add(pageWindow)
		if winEnd.After(to) {
			winEnd = to
		}
		if err := s.backfillWindow(ctx, addr, winStart, winEnd); err != nil {
			return err
		}
		winStart = winEnd

		// Soft pacing — HL info endpoints share a global IP weight budget.
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(250 * time.Millisecond):
		}
	}
	return nil
}

func (s *Syncer) backfillWindow(ctx context.Context, addr string, from, to time.Time) error {
	raw, err := s.rest.UserFillsByTime(ctx, addr, from.UnixMilli(), to.UnixMilli())
	if err != nil {
		return fmt.Errorf("userFillsByTime [%s,%s]: %w", from, to, err)
	}
	rows := convert(addr, raw)
	if err := db.UpsertUserFills(ctx, s.pool, rows); err != nil {
		return err
	}
	s.log.Info("backfill page", "addr", addr, "from", from, "to", to, "n", len(rows))

	// Page hit cap → may have truncated. Recurse on each half. UPSERT swallows
	// any boundary double-count.
	if len(raw) >= pageHardCap && to.Sub(from) > time.Minute {
		mid := from.Add(to.Sub(from) / 2)
		s.log.Warn("page hit cap, splitting", "addr", addr, "from", from, "to", to)
		if err := s.backfillWindow(ctx, addr, from, mid); err != nil {
			return err
		}
		if err := s.backfillWindow(ctx, addr, mid, to); err != nil {
			return err
		}
	}
	return nil
}

func convert(addr string, raw []hl.RawFill) []db.UserFill {
	out := make([]db.UserFill, 0, len(raw))
	for _, r := range raw {
		out = append(out, db.UserFill{
			UserAddr:  addr,
			Tid:       r.Tid,
			Time:      time.UnixMilli(r.Time).UTC(),
			Coin:      r.Coin,
			Side:      r.Side,
			Px:        r.Px,
			Sz:        r.Sz,
			Fee:       r.Fee,
			ClosedPnl: r.ClosedPnl,
			StartPos:  r.StartPosition,
			Dir:       r.Dir,
			Oid:       r.Oid,
			Hash:      r.Hash,
			Crossed:   r.Crossed,
			FeeToken:  r.FeeToken,
		})
	}
	return out
}
