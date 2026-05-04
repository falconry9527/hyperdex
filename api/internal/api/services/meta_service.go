package services

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/seabond/api/internal/config"
	"github.com/seabond/api/internal/hl"
)

// MetaService caches the HL universe metadata in memory with a TTL. The
// universe (asset names, szDecimals, maxLeverage) changes rarely, so a 5-min
// TTL keeps load on HL near zero while still picking up new listings.
type MetaService struct {
	hl  *hl.Client
	ttl time.Duration

	mu       sync.RWMutex
	cached   json.RawMessage
	cachedAt time.Time
}

func NewMetaService(c *hl.Client, cfg config.MetaConfig) *MetaService {
	return &MetaService{
		hl:  c,
		ttl: time.Duration(cfg.CacheTTLSec) * time.Second,
	}
}

// Get returns the cached payload if fresh, otherwise refreshes from HL. The
// returned value is HL's native /info{type:"meta"} JSON shape.
func (s *MetaService) Get(ctx context.Context) (json.RawMessage, error) {
	s.mu.RLock()
	if s.cached != nil && time.Since(s.cachedAt) < s.ttl {
		out := s.cached
		s.mu.RUnlock()
		return out, nil
	}
	s.mu.RUnlock()

	raw, err := s.hl.Meta(ctx)
	if err != nil {
		// On refresh failure, fall back to stale cache if we have one — better
		// to serve a 5-minute-stale universe than to break the trading UI.
		s.mu.RLock()
		stale := s.cached
		s.mu.RUnlock()
		if stale != nil {
			return stale, nil
		}
		return nil, err
	}

	s.mu.Lock()
	s.cached = raw
	s.cachedAt = time.Now()
	s.mu.Unlock()
	return raw, nil
}
