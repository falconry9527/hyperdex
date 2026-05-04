# API

Read-only REST service for Hyperliquid k-line history. Reads from
TimescaleDB — one `klines_<interval>` hypertable per interval, populated
by the `collector` service. Live updates live in the sibling `stream`
service (`:8090`).

> **Frontend integration**: see [docs/api.md](docs/api.md) for endpoint
> reference, request/response shapes, error codes, and TypeScript examples.

## Quickstart

Prereqs: Go 1.23+. The `collector` service must already have schema
migrations applied and be writing data — see `../collector/README.md`.

```bash
make tidy
make run

# Smoke test — REST returns {code, data, msg} envelope
curl 'http://localhost:8080/klines?coin=BTC&interval=1m&limit=5' | jq
```

## Layout

```
internal/api/
  routes/      InitRouter — middleware + per-domain handler chain (repo → service → handler)
  handlers/    gin adapters — *_handler.go per domain
  services/    business logic (KlineService, ...)
  repository/  data access — Postgres queries
  models/      domain entities exposed on the wire
  msg/         shared {code, data, msg} response envelope
internal/db/     pgxpool factory (infrastructure)
internal/config/ TOML loader
```

## Endpoints

| Method/path | Implementation |
|---|---|
| `GET /klines?coin=&interval=&from=&to=&limit=` | [handlers/kline_handler.go](internal/api/handlers/kline_handler.go) → [services/kline_service.go](internal/api/services/kline_service.go) → [repository/kline.go](internal/api/repository/kline.go) |
| `GET /healthz` | liveness |

Supported intervals: `1m / 5m / 15m / 1h / 4h / 1d / 1mo`.

## Adding a new domain (e.g. trades)

After the collector side is wired up:

1. Add `internal/api/repository/trade.go` (queries against the trade tables)
2. Add `internal/api/services/trade_service.go` (validation + defaults)
3. Add `internal/api/handlers/trade_handler.go` (parse params, call service)
4. Register the route in `internal/api/routes/routes.go` next to the
   kline registration: build `repo → svc → handler` and `r.GET("/trades", ...)`.
5. If the domain has live updates, extend `stream/internal/stream/ws/hub.go`'s
   `channelName` switch to map `{"channel":"trade",...}` to the matching
   Redis channel.

## Operational notes

- The API is **read-only** — schema migrations live in the collector
  repo. Don't add migrations here.
- Finalized history comes from per-interval Postgres hypertables
  (`klines_1m`, `klines_5m`, …, `klines_1mo`). The `tableFor()` switch in
  `internal/api/repository/kline.go` is the only place that needs updating
  if a new interval is added.
- Live bars come from the sibling `stream` service.
