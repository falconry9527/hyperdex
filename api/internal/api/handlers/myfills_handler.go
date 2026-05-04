package handlers

import (
	"errors"
	"strconv"

	"github.com/gin-gonic/gin"

	"github.com/seabond/api/internal/api/msg"
	"github.com/seabond/api/internal/api/services"
)

type MyFillsHandler struct {
	svc *services.MyFillsService
}

func NewMyFillsHandler(svc *services.MyFillsService) *MyFillsHandler {
	return &MyFillsHandler{svc: svc}
}

// Get godoc
// GET /myfills?user=0x...&from=<ms>&to=<ms>&limit=N
//
// Returns the requesting user's fill history from the collector-indexed table.
// Defaults: to=now, from=to-30d, limit=service default.
func (h *MyFillsHandler) Get(c *gin.Context) {
	req := services.ListMyFillsRequest{Addr: c.Query("user")}
	if v := c.Query("from"); v != "" {
		ms, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			msg.Result(c, msg.ParamError, nil, "invalid from"); return
		}
		req.FromMs = ms
	}
	if v := c.Query("to"); v != "" {
		ms, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			msg.Result(c, msg.ParamError, nil, "invalid to"); return
		}
		req.ToMs = ms
	}
	if v := c.Query("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			msg.Result(c, msg.ParamError, nil, "invalid limit"); return
		}
		req.Limit = n
	}

	rows, err := h.svc.List(c.Request.Context(), req)
	if err != nil {
		switch {
		case errors.Is(err, services.ErrAddrRequired),
			errors.Is(err, services.ErrAddrFormat):
			msg.Result(c, msg.ParamError, nil, err.Error())
		default:
			msg.Error(c, err)
		}
		return
	}
	msg.Success(c, rows)
}
