// Package routes builds the gin engine: cross-cutting middleware (CORS),
// global endpoints (/healthz), and each domain's handler chain
// (repository → service → handler) bound to its route. WS live updates live
// in the sibling stream service.
package routes

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/seabond/api/internal/api/handlers"
	"github.com/seabond/api/internal/api/repository"
	"github.com/seabond/api/internal/api/services"
	"github.com/seabond/api/internal/config"
	"github.com/seabond/api/internal/hl"
)

// InitRouter builds and returns the Gin engine with REST routes registered.
func InitRouter(pool *pgxpool.Pool, cfg *config.Config) (*gin.Engine, func()) {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery())
	r.Use(corsDev())

	r.GET("/healthz", func(c *gin.Context) { c.String(http.StatusOK, "ok") })

	// kline domain: repo → service → handler.
	klineRepo := repository.NewKlineRepository(pool)
	klineSvc := services.NewKlineService(klineRepo, cfg.Kline)
	klineH := handlers.NewKlineHandler(klineSvc)
	r.GET("/klines", klineH.List)

	// meta domain: HL passthrough with TTL cache. No repo (no DB read).
	hlClient := hl.New(cfg.HL.APIURL)
	metaSvc := services.NewMetaService(hlClient, cfg.Meta)
	metaH := handlers.NewMetaHandler(metaSvc)
	r.GET("/meta", metaH.Get)

	// myfills domain: read-only view over collector's user_fills indexer.
	mfRepo := repository.NewMyFillsRepository(pool)
	mfSvc := services.NewMyFillsService(mfRepo, cfg.MyFills)
	mfH := handlers.NewMyFillsHandler(mfSvc)
	r.GET("/myfills", mfH.Get)

	cleanup := func() {}
	return r, cleanup
}

// corsDev is a permissive CORS middleware for the dev frontend (web/index.html
// loaded from another origin or file://). Lock down Allow-Origin in prod.
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
