// Package ws fans out Redis pub/sub events to connected websocket clients.
//
// Wire protocol from client:
//
//	{"action":"sub","channel":"kline","coin":"BTC","interval":"1m"}
//	{"action":"unsub","channel":"kline","coin":"BTC","interval":"1m"}
//
// Server pushes raw JSON payloads from the corresponding Redis channel.
package ws

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/redis/go-redis/v9"

	"github.com/seabond/stream/internal/db"
)

// Hub maintains per-channel websocket subscribers and a single shared Redis
// subscription per channel. The hub is HTTP-framework agnostic — the gin
// adapter in routes.go invokes ServeWS to upgrade individual requests.
type Hub struct {
	cache  *db.Redis
	log    *slog.Logger
	router *router

	mu          sync.Mutex
	subscribers map[string]map[*conn]struct{} // channel -> set of conns
	pubsubs     map[string]*redis.PubSub      // channel -> shared redis subscription
}

type conn struct {
	c       *websocket.Conn
	writeMu sync.Mutex
	subs    map[string]struct{}
}

// NewHub returns a Hub ready to ServeWS — the in-band sub/unsub protocol is
// registered at construction time. To extend the protocol with new client
// commands (e.g. ping/auth), add an `r.Handle(...)` line below.
func NewHub(cli *db.Redis, log *slog.Logger) *Hub {
	h := &Hub{
		cache:       cli,
		log:         log,
		router:      newRouter(),
		subscribers: make(map[string]map[*conn]struct{}),
		pubsubs:     make(map[string]*redis.PubSub),
	}
	h.router.Handle("sub", h.subscribe)
	h.router.Handle("unsub", h.unsubscribe)
	return h
}

type clientMsg struct {
	Action   string `json:"action"`
	Channel  string `json:"channel"`
	Coin     string `json:"coin"`
	Interval string `json:"interval"`
}

// ─── action router ─────────────────────────────────────────────────────────
// Internal dispatch table for the in-band sub/unsub protocol carried over a
// single ws connection. Mirrors a tiny HTTP router but for client commands;
// kept package-private since only Hub registers and invokes it.

var (
	errInvalidJSON   = errors.New("invalid json")
	errUnknownAction = errors.New("unknown action")
)

// action handles one inbound websocket message for a connected client. ctx is
// the per-connection context; cancellation propagates from the read loop.
type action func(ctx context.Context, cn *conn, m clientMsg)

type router struct {
	actions map[string]action
}

func newRouter() *router {
	return &router{actions: make(map[string]action)}
}

// Handle registers fn under the given action name. Last call wins.
func (r *router) Handle(name string, fn action) {
	r.actions[name] = fn
}

// Dispatch parses raw, looks up the action handler, and invokes it.
func (r *router) Dispatch(ctx context.Context, cn *conn, raw []byte) error {
	var m clientMsg
	if err := json.Unmarshal(raw, &m); err != nil {
		return errInvalidJSON
	}
	fn, ok := r.actions[m.Action]
	if !ok {
		return errUnknownAction
	}
	fn(ctx, cn, m)
	return nil
}

// ─── connection lifecycle ──────────────────────────────────────────────────

// ServeWS upgrades the request to websocket and runs the read loop until the
// client disconnects.
func (h *Hub) ServeWS(w http.ResponseWriter, r *http.Request) {
	wsConn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		InsecureSkipVerify: true, // tighten in prod (CompressionMode + OriginPatterns)
	})
	if err != nil {
		h.log.Warn("ws accept", "err", err)
		return
	}

	cn := &conn{c: wsConn, subs: make(map[string]struct{})}
	defer h.closeConn(cn)

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	for {
		_, data, err := wsConn.Read(ctx)
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				h.log.Debug("ws read end", "err", err)
			}
			return
		}
		if err := h.router.Dispatch(ctx, cn, data); err != nil {
			h.writeJSON(cn, map[string]string{"error": err.Error()})
		}
	}
}

// ChannelCounts returns subscriber counts per active channel — used by the
// handler's status endpoint.
func (h *Hub) ChannelCounts() map[string]int {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make(map[string]int, len(h.subscribers))
	for ch, set := range h.subscribers {
		out[ch] = len(set)
	}
	return out
}

func channelName(m clientMsg) string {
	switch m.Channel {
	case "kline":
		if m.Coin == "" || m.Interval == "" {
			return ""
		}
		return db.LiveChannel(m.Coin, m.Interval)
	case "l2book":
		// L2 is per-coin; interval is ignored if the client sends it.
		if m.Coin == "" {
			return ""
		}
		return db.L2BookLiveChannel(m.Coin)
	case "ticker":
		if m.Coin == "" {
			return ""
		}
		return db.TickerLiveChannel(m.Coin)
	case "trades":
		if m.Coin == "" {
			return ""
		}
		return db.TradesLiveChannel(m.Coin)
	}
	return ""
}

func (h *Hub) subscribe(ctx context.Context, cn *conn, m clientMsg) {
	ch := channelName(m)
	if ch == "" {
		h.writeJSON(cn, map[string]string{"error": "unknown channel"})
		return
	}

	h.mu.Lock()
	if _, ok := cn.subs[ch]; ok {
		h.mu.Unlock()
		return
	}
	cn.subs[ch] = struct{}{}
	if _, ok := h.subscribers[ch]; !ok {
		h.subscribers[ch] = make(map[*conn]struct{})
	}
	h.subscribers[ch][cn] = struct{}{}
	// First subscriber to this channel: open the redis subscription.
	if _, ok := h.pubsubs[ch]; !ok {
		ps := h.cache.Subscribe(context.Background(), ch)
		h.pubsubs[ch] = ps
		go h.pump(ch, ps)
	}
	h.mu.Unlock()

	// Send the cached "last" frame immediately so the client doesn't wait for the next tick.
	switch m.Channel {
	case "kline":
		if last, err := h.cache.LastLive(ctx, m.Coin, m.Interval); err == nil && last != nil {
			h.writeRaw(cn, last)
		}
	case "l2book":
		if last, err := h.cache.LastL2Book(ctx, m.Coin); err == nil && last != nil {
			h.writeRaw(cn, last)
		}
	case "ticker":
		if last, err := h.cache.LastTicker(ctx, m.Coin); err == nil && last != nil {
			h.writeRaw(cn, last)
		}
	case "trades":
		// Trades catch-up is the rolling window (LRANGE), not a single frame.
		if last, err := h.cache.LastTrades(ctx, m.Coin); err == nil && last != nil {
			h.writeRaw(cn, last)
		}
	}
}

func (h *Hub) unsubscribe(_ context.Context, cn *conn, m clientMsg) {
	ch := channelName(m)
	if ch == "" {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(cn.subs, ch)
	if set, ok := h.subscribers[ch]; ok {
		delete(set, cn)
		if len(set) == 0 {
			delete(h.subscribers, ch)
			if ps, ok := h.pubsubs[ch]; ok {
				_ = ps.Close()
				delete(h.pubsubs, ch)
			}
		}
	}
}

func (h *Hub) pump(ch string, ps *redis.PubSub) {
	for msg := range ps.Channel() {
		h.mu.Lock()
		set := h.subscribers[ch]
		conns := make([]*conn, 0, len(set))
		for c := range set {
			conns = append(conns, c)
		}
		h.mu.Unlock()
		for _, cn := range conns {
			h.writeRaw(cn, []byte(msg.Payload))
		}
	}
}

func (h *Hub) writeRaw(cn *conn, payload []byte) {
	cn.writeMu.Lock()
	defer cn.writeMu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := cn.c.Write(ctx, websocket.MessageText, payload); err != nil {
		h.log.Debug("ws write", "err", err)
	}
}

func (h *Hub) writeJSON(cn *conn, v any) {
	b, err := json.Marshal(v)
	if err != nil {
		return
	}
	h.writeRaw(cn, b)
}

func (h *Hub) closeConn(cn *conn) {
	h.mu.Lock()
	for ch := range cn.subs {
		if set, ok := h.subscribers[ch]; ok {
			delete(set, cn)
			if len(set) == 0 {
				delete(h.subscribers, ch)
				if ps, ok := h.pubsubs[ch]; ok {
					_ = ps.Close()
					delete(h.pubsubs, ch)
				}
			}
		}
	}
	h.mu.Unlock()
	_ = cn.c.Close(websocket.StatusNormalClosure, "bye")
}
