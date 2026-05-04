package services

import (
	"context"
	"errors"
	"regexp"
	"time"

	"github.com/seabond/api/internal/api/models"
	"github.com/seabond/api/internal/api/repository"
	"github.com/seabond/api/internal/config"
)

var (
	ErrAddrRequired = errors.New("user is required")
	ErrAddrFormat   = errors.New("user must be a 0x-prefixed 40-hex address")
)

// reAddr loosely validates the user param. Strict on length + hex chars but
// case-insensitive — we lowercase before hitting the repo.
var reAddr = regexp.MustCompile(`^0x[0-9a-fA-F]{40}$`)

type ListMyFillsRequest struct {
	Addr   string
	FromMs int64 // 0 = unset → defaults to to - 30d
	ToMs   int64 // 0 = unset → defaults to now
	Limit  int   // 0 = unset → service default
}

type MyFillsService struct {
	repo repository.MyFillsRepository
	cfg  config.MyFillsConfig
}

func NewMyFillsService(repo repository.MyFillsRepository, cfg config.MyFillsConfig) *MyFillsService {
	return &MyFillsService{repo: repo, cfg: cfg}
}

func (s *MyFillsService) List(ctx context.Context, req ListMyFillsRequest) ([]models.UserFill, error) {
	if req.Addr == "" {
		return nil, ErrAddrRequired
	}
	if !reAddr.MatchString(req.Addr) {
		return nil, ErrAddrFormat
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
	from := to.Add(-30 * 24 * time.Hour)
	if req.FromMs > 0 {
		from = time.UnixMilli(req.FromMs).UTC()
	}

	return s.repo.Query(ctx, req.Addr, from, to, limit)
}
