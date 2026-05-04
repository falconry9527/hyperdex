package handlers

import (
	"github.com/gin-gonic/gin"

	"github.com/seabond/api/internal/api/msg"
	"github.com/seabond/api/internal/api/services"
)

// MetaHandler exposes HL's asset universe metadata under our standard envelope.
type MetaHandler struct {
	svc *services.MetaService
}

func NewMetaHandler(svc *services.MetaService) *MetaHandler {
	return &MetaHandler{svc: svc}
}

// Get godoc
// GET /meta
//
// Returns HL's `/info {"type":"meta"}` payload verbatim under `data`. Frontends
// use this for szDecimals, maxLeverage, and the active asset list. Cached by
// the service for cfg.Meta.CacheTTLSec.
func (h *MetaHandler) Get(c *gin.Context) {
	raw, err := h.svc.Get(c.Request.Context())
	if err != nil {
		msg.Error(c, err)
		return
	}
	msg.Success(c, raw)
}
