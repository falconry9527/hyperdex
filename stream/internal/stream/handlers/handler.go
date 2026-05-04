package handlers

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/seabond/stream/internal/stream/ws"
)

// Handler bridges Gin to the websocket Hub. The hub itself is
// framework-agnostic; this layer adapts (*gin.Context) → (w, r).
type Handler struct {
	hub *ws.Hub
}

// NewHandler creates a Handler backed by hub.
func NewHandler(hub *ws.Hub) *Handler {
	return &Handler{hub: hub}
}

// Connect godoc
// GET /ws — upgrade to websocket and run the subscribe loop.
func (h *Handler) Connect(c *gin.Context) {
	h.hub.ServeWS(c.Writer, c.Request)
}

// Status godoc
// GET /ws/status — subscriber counts per active channel (debug).
// Returns raw JSON, no envelope: stream is a pure streaming service and skips
// the api's {code, data, msg} wrapper for its own tiny debug surface.
func (h *Handler) Status(c *gin.Context) {
	c.JSON(http.StatusOK, h.hub.ChannelCounts())
}
