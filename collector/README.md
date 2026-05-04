# Collector

WS subscription + REST backfill for Hyperliquid market data, writing to
TimescaleDB and publishing live updates through Redis.

K-line is the first domain — drop a new package under
`internal/ingest/<name>` plus a migration to add `trade`, `funding`,
`orderbook`, `liquidation`, etc.

## Quickstart

Prereqs: Go 1.23+, Docker, `golang-migrate` CLI
(`brew install golang-migrate`).

```bash
# 1. boot Postgres+TimescaleDB and Redis locally
make up

# 2. apply schema (creates klines_1m / 5m / 15m / 1h / 4h / 1d / 1mo hypertables)
make migrate

# 3. fetch dependencies
make tidy

# 4. (optional) backfill history before going live — see "Data sync" below
go run ./cmd/collector --config configs/config.toml --backfill-days 7

# 5. run the live collector (subscribes WS, batches into DB, publishes to Redis)
make run
```

## Data sync (backfill) commands

History is paginated via HL's REST `candleSnapshot` (5000-bar pages, 250 ms
between calls, 429 → exponential backoff). Three modes are provided so you can
schedule sub-day and day-up refreshes on different cadences:

| Command | Intervals | Default `--days` | Typical cadence |
|---|---|---:|---|
| `cmd/collector --backfill-days N` | every interval in `cfg.Kline.Intervals` | required | one-shot bootstrap |
| `cmd/backfill-intraday`           | 1m / 5m / 15m / 1h / 4h                | 30                       | hourly cron |
| `cmd/backfill-daily`              | 1d / 1mo                                | 1825 (5y)                | daily cron |

```bash
# one-shot: full backfill of everything currently in cfg.Intervals
go run ./cmd/collector --backfill-days 360

# sub-day intervals only (cfg.Intervals filtered to 1m/5m/15m/1h/4h)
go run ./cmd/backfill-intraday              # default 30 days
go run ./cmd/backfill-intraday --days 7

# day-and-above intervals (1d/1mo are seeded even if cfg omits them)
go run ./cmd/backfill-daily                 # default 5 years
go run ./cmd/backfill-daily --days 365
```

Cron example:

```cron
# refresh sub-day intervals every hour
0  *  *  *  * cd /opt/collector && ./bin/backfill-intraday >> /var/log/backfill-intraday.log 2>&1

# refresh day-up intervals once per day
30 0  *  *  * cd /opt/collector && ./bin/backfill-daily    >> /var/log/backfill-daily.log    2>&1
```

HL retention per interval (observed): 1m ≈ 3.5 d, 5m ≈ 17 d, 15m ≈ 52 d,
1h ≈ 7 mo, 4h ≈ 360 d+, 1d / 1mo multi-year. Backfill paginates backwards
until the API returns an empty page, so passing a window larger than HL's
retention is harmless — it just stops early.

## User fill sync (`sync-fills`)

Per-address trade history → `user_fills` PG table. **On-demand only** — the
collector binary doesn't run this in the background. Browser-side realtime UI
gets fills direct from HL ws independently; this command is the persistence
layer for analytics (PnL curves, tax exports, win rate).

```bash
# sync every addr in [userfills].addrs using cfg.BackfillDays
make sync-fills

# overrides (single addr, custom window)
make sync-fills ADDR=0xabc... DAYS=30

# direct invocation
go run ./cmd/sync-fills --config configs/config.toml
go run ./cmd/sync-fills --addr 0xabc... --days 7
```

Per addr the command does, in order:

1. If DB has nothing newer than `now - days`, paginate REST `userFillsByTime`
   in 7-day windows back to the cutoff (auto-bisects pages that hit HL's
   2000-row cap).
2. Always also call REST `userFills` (most recent ~2000) — closes any gap
   between the backfill window and "now" without computing exact bounds.

Both paths UPSERT on `(user_addr, tid)` with `ON CONFLICT DO NOTHING`, so
re-running is idempotent and cheap (PG short-circuits on the PK index).

Config:

```toml
[userfills]
addrs         = ["0x1b56..."]    # case-insensitive; lowercased on insert
backfill_days = 90               # only matters on first sync per addr
```

Cron-style scheduling (run every 30 min):

```cron
*/30 * * * * cd /opt/collector && /usr/bin/make sync-fills >> /var/log/sync-fills.log 2>&1
```

## Components

| Path | Purpose |
|---|---|
| `internal/hl/ws.go`            | HL websocket subscription wrapper (kline/l2book/ticker/trades) |
| `internal/hl/snapshot.go`      | HL `/info` REST client (candleSnapshot, userFills, userFillsByTime) |
| `internal/ingest/kline/`       | Receives candles, batches finalized bars, publishes live updates |
| `internal/ingest/l2book/`      | Live-only L2 order book → Redis pub |
| `internal/ingest/ticker/`      | Live-only `activeAssetCtx` → Redis pub |
| `internal/ingest/trades/`      | Live-only public tape → Redis pub + 50-trade rolling cache |
| `internal/ingest/userfills/`   | On-demand `Syncer` for `cmd/sync-fills` (no long-running goroutine) |
| `internal/db/`                 | `pgxpool` clients + bulk upsert + Redis SET/PUBLISH |
| `cmd/collector`                | Long-running ingester for everything except userfills |
| `cmd/sync-fills`               | One-shot fill-history sync; cron or run by hand |
| `cmd/backfill-intraday` / `-daily` | One-shot kline backfill (REST candleSnapshot) |

## Adding a new domain (e.g. trades)

1. Add `migrations/0020_trade.up.sql` (hypertable + aggregates if needed)
2. Create `internal/ingest/trade/` implementing `Run(ctx)` /
   `Backfill(ctx, ...)`
3. Wire it into `cmd/collector/main.go` next to the kline ingester

## Operational notes

- **Schema migrations** are owned by the collector. The API runs read-only.
- **Live bars** (in-progress) only live in Redis, never in Postgres. Finalized
  bars are upserted, so reconnect-induced duplicates are safe.
- **Backfill** is idempotent (`ON CONFLICT (coin, ts) DO UPDATE` for klines,
  `DO NOTHING` for user_fills). Re-run any backfill / sync command any time
  you suspect a gap.
- **`sync-fills` is on-demand**, not a daemon. The collector binary skips it
  entirely. Cron it (or just run after a trading session) to refresh PG.
- **Per-interval hypertables**: every interval has its own `klines_<interval>`
  table (no continuous aggregates). The collector persists finalized bars for
  each interval listed in `cfg.Kline.Intervals`; in-progress bars live only in
  Redis.
- **Coverage** is tracked per interval under `domain = 'kline_<interval>'` in
  the `coverage` table.
- **Compression** kicks in 30 days after a row's timestamp (90 d for 1h,
  180 d for 4h, 365 d for 1d). Tunable in the migrations.
- **Metrics**: `http://localhost:9090/metrics`. Wire to Prometheus + Grafana
  when you're ready.
