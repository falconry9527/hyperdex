package hl

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"
)

// RawCandle mirrors the HL candleSnapshot response shape.
type RawCandle struct {
	TimeOpen    int64  `json:"t"`
	TimeClose   int64  `json:"T"`
	Symbol      string `json:"s"`
	Interval    string `json:"i"`
	Open        string `json:"o"`
	Close       string `json:"c"`
	High        string `json:"h"`
	Low         string `json:"l"`
	Volume      string `json:"v"`
	TradesCount int    `json:"n"`
}

// RESTClient is a thin POST /info wrapper. We avoid the SDK's NewInfo here
// because we don't need its coin↔asset mapping for snapshot fetches.
type RESTClient struct {
	baseURL string
	http    *http.Client
}

func NewRESTClient(baseURL string) *RESTClient {
	return &RESTClient{
		baseURL: baseURL,
		http: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// CandlesSnapshot fetches up to 5000 candles in [startMs, endMs].
func (c *RESTClient) CandlesSnapshot(ctx context.Context, coin, interval string, startMs, endMs int64) ([]RawCandle, error) {
	body, _ := json.Marshal(map[string]any{
		"type": "candleSnapshot",
		"req": map[string]any{
			"coin":      coin,
			"interval":  toWireInterval(interval),
			"startTime": startMs,
			"endTime":   endMs,
		},
	})
	var out []RawCandle
	if err := c.doInfoPOST(ctx, body, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// RawFill mirrors HL's userFillsByTime / userFills response shape. All
// numeric fields are strings (HL's native wire); the indexer parses them at
// the DB-write boundary so the rest of the codebase stays string-typed.
type RawFill struct {
	Coin          string `json:"coin"`
	Px            string `json:"px"`
	Sz            string `json:"sz"`
	Side          string `json:"side"`        // "B" (buy) or "A" (sell)
	Time          int64  `json:"time"`        // unix ms
	StartPosition string `json:"startPosition"`
	Dir           string `json:"dir"`         // "Open Long" / "Close Short" / etc
	ClosedPnl     string `json:"closedPnl"`
	Hash          string `json:"hash"`
	Oid           int64  `json:"oid"`
	Crossed       bool   `json:"crossed"`
	Fee           string `json:"fee"`
	Tid           int64  `json:"tid"`
	FeeToken      string `json:"feeToken"`
}

// doInfoPOST handles the shared 429-retry boilerplate for /info calls and
// decodes the body into out. Used by every HL REST shape we consume.
func (c *RESTClient) doInfoPOST(ctx context.Context, body []byte, out any) error {
	const maxAttempts = 5
	backoff := time.Second
	for attempt := 1; ; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/info", bytes.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := c.http.Do(req)
		if err != nil {
			return fmt.Errorf("post /info: %w", err)
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			wait := backoff
			if ra := resp.Header.Get("Retry-After"); ra != "" {
				if secs, perr := strconv.Atoi(ra); perr == nil && secs > 0 {
					wait = time.Duration(secs) * time.Second
				}
			}
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if attempt >= maxAttempts {
				return fmt.Errorf("hl /info 429 after %d attempts", attempt)
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(wait):
			}
			backoff *= 2
			continue
		}
		if resp.StatusCode != http.StatusOK {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			return fmt.Errorf("hl /info status %d", resp.StatusCode)
		}
		err = json.NewDecoder(resp.Body).Decode(out)
		resp.Body.Close()
		if err != nil {
			return fmt.Errorf("decode /info: %w", err)
		}
		return nil
	}
}

// UserFills returns the most recent fills for `addr` (HL's `userFills` info
// type). Server caps at ~2000 entries — for older history use UserFillsByTime.
func (c *RESTClient) UserFills(ctx context.Context, addr string) ([]RawFill, error) {
	body, _ := json.Marshal(map[string]any{
		"type": "userFills",
		"user": addr,
	})
	var out []RawFill
	if err := c.doInfoPOST(ctx, body, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// UserFillsByTime fetches fills in [startMs, endMs]. `aggregateByTime=false`
// gives every individual fill (one row per maker-taker match). HL caps each
// page at 2000 fills; the caller must page by shrinking the window.
func (c *RESTClient) UserFillsByTime(ctx context.Context, addr string, startMs, endMs int64) ([]RawFill, error) {
	body, _ := json.Marshal(map[string]any{
		"type":            "userFillsByTime",
		"user":            addr,
		"startTime":       startMs,
		"endTime":         endMs,
		"aggregateByTime": false,
	})
	var out []RawFill
	if err := c.doInfoPOST(ctx, body, &out); err != nil {
		return nil, err
	}
	return out, nil
}
