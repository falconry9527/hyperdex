package handlers

import (
	"errors"
	"strconv"

	"github.com/gin-gonic/gin"

	"github.com/seabond/api/internal/api/msg"
	"github.com/seabond/api/internal/api/services"
)

// KlineHandler handles HTTP requests for k-line bars.
type KlineHandler struct {
	svc *services.KlineService
}

// NewKlineHandler creates a KlineHandler backed by svc.
func NewKlineHandler(svc *services.KlineService) *KlineHandler {
	return &KlineHandler{svc: svc}
}

// List godoc
// GET /klines?coin=BTC&interval=1m&from=<ms>&to=<ms>&limit=1500
//
// from/to are unix milliseconds. Defaults: to=now, from=to - limit*interval.
func (h *KlineHandler) List(c *gin.Context) {
	req := services.ListKlinesRequest{
		Coin:     c.Query("coin"),
		Interval: c.Query("interval"),
	}

	if v := c.Query("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			msg.Result(c, msg.ParamError, nil, services.ErrInvalidLimit.Error())
			return
		}
		req.Limit = n
	}
	if v := c.Query("from"); v != "" {
		ms, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			msg.Result(c, msg.ParamError, nil, services.ErrInvalidFrom.Error())
			return
		}
		req.FromMs = ms
	}
	if v := c.Query("to"); v != "" {
		ms, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			msg.Result(c, msg.ParamError, nil, services.ErrInvalidTo.Error())
			return
		}
		req.ToMs = ms
	}

	rows, err := h.svc.ListKlines(c.Request.Context(), req)
	if err != nil {
		switch {
		case errors.Is(err, services.ErrCoinRequired),
			errors.Is(err, services.ErrIntervalRequired),
			errors.Is(err, services.ErrInvalidInterval):
			msg.Result(c, msg.ParamError, nil, err.Error())
		default:
			msg.Error(c, err)
		}
		return
	}
	msg.Success(c, rows)
}
