package services

import (
	"context"
	"errors"
	"time"

	"github.com/seabond/api/internal/api/models"
	"github.com/seabond/api/internal/api/repository"
	"github.com/seabond/api/internal/config"
)

var (
	ErrCoinRequired     = errors.New("coin is required")
	ErrIntervalRequired = errors.New("interval is required")
	ErrInvalidInterval  = errors.New("invalid interval")
	ErrInvalidLimit     = errors.New("invalid limit")
	ErrInvalidFrom      = errors.New("invalid from")
	ErrInvalidTo        = errors.New("invalid to")
)

// ListKlinesRequest carries already-parsed parameters from the handler. Empty
// strings for FromMs / ToMs / Limit (-1) mean "use defaults".
type ListKlinesRequest struct {
	Coin     string
	Interval string
	FromMs   int64 // 0 = unset
	ToMs     int64 // 0 = unset
	Limit    int   // 0 = unset
}

// KlineService applies request validation and default-window logic, then
// delegates the actual query to the repository.
type KlineService struct {
	repo repository.KlineRepository
	cfg  config.KlineConfig
}

// NewKlineService creates a KlineService backed by repo with the given config.
func NewKlineService(repo repository.KlineRepository, cfg config.KlineConfig) *KlineService {
	return &KlineService{repo: repo, cfg: cfg}
}

// ListKlines validates req, fills defaults, and returns the resulting bars in
// ascending ts order.
func (s *KlineService) ListKlines(ctx context.Context, req ListKlinesRequest) ([]models.Kline, error) {
	if req.Coin == "" {
		return nil, ErrCoinRequired
	}
	if req.Interval == "" {
		return nil, ErrIntervalRequired
	}
	if !s.repo.IsValidInterval(req.Interval) {
		return nil, ErrInvalidInterval
	}

	limit := s.cfg.DefaultLimit
	if req.Limit > 0 {
		limit = req.Limit
		if limit > s.cfg.MaxLimit {
			limit = s.cfg.MaxLimit
		}
	}

	to := time.Now().UTC()
	if req.ToMs > 0 {
		to = time.UnixMilli(req.ToMs).UTC()
	}
	from := to.Add(-time.Duration(limit) * intervalDuration(req.Interval))
	if req.FromMs > 0 {
		from = time.UnixMilli(req.FromMs).UTC()
	}

	return s.repo.QueryKlines(ctx, req.Coin, req.Interval, from, to, limit)
}

// intervalDuration is only used to compute a sensible default `from` when the
// caller passes a `limit` but no explicit window. Real bucket boundaries live
// in the database (time_bucket(...)).
func intervalDuration(s string) time.Duration {
	switch s {
	case "1m":
		return time.Minute
	case "5m":
		return 5 * time.Minute
	case "15m":
		return 15 * time.Minute
	case "1h":
		return time.Hour
	case "4h":
		return 4 * time.Hour
	case "1d":
		return 24 * time.Hour
	case "1mo":
		return 30 * 24 * time.Hour
	}
	return time.Minute
}
