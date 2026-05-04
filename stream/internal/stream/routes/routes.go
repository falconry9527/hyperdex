// Package routes builds the gin engine for the stream service: cross-cutting
// middleware (CORS), /healthz, and the /ws subsystem (gin handler wired to
// the in-memory Hub fan-out).
package routes

import (
	"log/slog"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/seabond/stream/internal/db"
	"github.com/seabond/stream/internal/stream/handlers"
	"github.com/seabond/stream/internal/stream/ws"
)

// InitRouter builds and returns the Gin engine with all routes registered.
func InitRouter(redis *db.Redis, log *slog.Logger) (*gin.Engine, func()) {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery())
	r.Use(corsDev())

	r.GET("/healthz", func(c *gin.Context) { c.String(http.StatusOK, "ok") })

	// ws fan-out: hub (state + dispatch) → handler (gin adapter) → routes.
	hub := ws.NewHub(redis, log)
	wsH := handlers.NewHandler(hub)
	r.GET("/ws", wsH.Connect)
	r.GET("/ws/status", wsH.Status)

	cleanup := func() {}
	return r, cleanup
}

// corsDev is a permissive CORS middleware for the dev frontend (web/index.html
// loaded from another origin). Lock down Allow-Origin in prod.
func corsDev() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Header("Access-Control-Allow-Origin", "*")
		c.Header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		c.Header("Access-Control-Allow-Headers", "Content-Type, Authorization")
		if c.Request.Method == http.MethodOptions {
			c.AbortWithStatus(http.StatusNoContent)
			return
		}
		c.Next()
	}
}
