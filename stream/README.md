# Stream

WebSocket fan-out service for Hyperliquid live market data. Subscribes to
Redis pub/sub channels populated by the collector and pushes raw JSON frames
to connected ws clients. No Postgres dependency — historical reads live in
the sibling `api` service.

> **Frontend integration**: see [docs/protocol.md](docs/protocol.md) for the
> wire protocol (sub/unsub commands, frame shapes, error frames) and a
> TypeScript reference client.

## Quickstart

Prereqs: Go 1.23+. The `collector` service must be publishing to Redis
(`kline.<coin>.<interval>` pub/sub channels and `kline:last:*` cache keys).

```bash
make tidy
make run

# Smoke test (Python websockets)
python3 - <<'EOF'
import asyncio, json, websockets
async def main():
    async with websockets.connect("ws://localhost:8090/ws") as ws:
        await ws.send(json.dumps({"action":"sub","channel":"kline","coin":"BTC","interval":"1m"}))
        for _ in range(5):
            print(await ws.recv())
asyncio.run(main())
EOF
```

## Layout

```
internal/
├── config/         TOML loader (HTTP listener + Redis only)
├── db/redis.go     Redis client + LiveChannel/LiveKey helpers
└── stream/
    ├── routes/     InitRouter — middleware, /healthz, hub→handler wiring
    ├── handlers/   gin adapter — Connect (upgrade) + Status (debug JSON)
    └── ws/         Hub state machine + in-band action router (sub/unsub registered in NewHub)
```

## Endpoints

| Method/path | Purpose |
|---|---|
| `GET /ws` | Upgrade to websocket; subscribe via `{"action":"sub","channel":"kline","coin":"BTC","interval":"1m"}`. Frames are raw JSON published by collector — no `{code,data,msg}` envelope. |
| `GET /ws/status` | Subscriber counts per active channel (debug). Returns raw JSON map. |
| `GET /healthz` | Liveness. Returns `ok`. |

## Operational notes

- Stream is **stateless across processes** — all state (subscriber sets, pubsub
  handles) is in-memory per instance. Multiple instances behind a load balancer
  works trivially as long as clients pick one and stay; sticky sessions are not
  required since each subscription is local to the instance.
- One Redis subscription per active `(coin, interval)` tuple, regardless of how
  many ws clients are subscribed — the Hub fans out in memory.
- New subscribers receive the cached `LastLive` bar immediately so the right-most
  candle paints without waiting for the next collector tick.
- Default port `:8090`. Set via `[http] addr` in `configs/config.toml`.
- Tighten `InsecureSkipVerify` on the websocket Accept call before exposing
  publicly — currently allows any origin for dev.
