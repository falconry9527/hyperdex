// Package hl is a tiny REST client for Hyperliquid's `/info` endpoint, used by
// the api service's /meta passthrough. It deliberately stays raw-bytes — we
// don't unmarshal the response, just forward HL's JSON verbatim under our
// envelope so the wire shape stays in lockstep with HL upstream.
package hl

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// Client is a single-purpose HL info-endpoint caller.
type Client struct {
	baseURL string
	http    *http.Client
}

// New constructs a Client targeting baseURL (e.g. "https://api.hyperliquid.xyz").
func New(baseURL string) *Client {
	return &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		http:    &http.Client{Timeout: 10 * time.Second},
	}
}

// Meta returns the raw JSON response of `POST /info {"type":"meta"}`. The
// response shape is `{"universe":[...], "marginTables":[...], "collateralToken":...}`
// — we forward bytes as-is so frontends see HL's native field names.
func (c *Client) Meta(ctx context.Context) ([]byte, error) {
	body := []byte(`{"type":"meta"}`)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/info", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("build /info request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("call /info: %w", err)
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read /info body: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("hl /info status %d: %s", resp.StatusCode, raw)
	}
	return raw, nil
}
