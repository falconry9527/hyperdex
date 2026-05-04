# Web

Tiny single-file demo that paints Hyperliquid k-lines using the api + stream
services.
- REST `:8080/klines` (api) fills the historical bars.
- WebSocket `:8090/ws` (stream) streams live updates and replaces the
  right-most bar in place.

No build step. Just a static HTML file that loads
[Lightweight Charts](https://www.tradingview.com/lightweight-charts/) from a CDN.

## Run

1. Make sure `api` (and a backfilled DB) is up — see `../api/README.md`.
2. Serve this directory with any static server, then open it in a browser:

```bash
cd web
python3 -m http.server 5173
# open http://localhost:5173
```

Browsers refuse `fetch()` from `file://`, so a local server is required even
though there is no backend. The api side already enables permissive CORS for
dev (`*`), so the page can live on any port.

## Wire-level mapping

| UI control | What it does |
|---|---|
| coin / interval `<select>` | Triggers `unsub` for the previous tuple, fetches history for the new one, then `sub`s the new one. |
| The chart                  | Candlestick series, ascending time order, epoch seconds. |
| Right-most candle          | Updated on every WS frame (`closed:false` ticks). |

If the services are on non-default addresses, edit the two constants at the
top of `index.html`:

```js
const API_URL = 'http://localhost:8080';      // REST history (api service)
const WS_URL  = 'ws://localhost:8090/ws';     // live push (stream service)
```
