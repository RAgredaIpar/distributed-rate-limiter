package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/distributed-rate-limiter/internal/circuitbreaker"
	"github.com/distributed-rate-limiter/internal/core"
	"github.com/distributed-rate-limiter/internal/limiter"
	serverhttp "github.com/distributed-rate-limiter/internal/server/http"
	redisstore "github.com/distributed-rate-limiter/internal/storage/redis"
	"github.com/distributed-rate-limiter/internal/telemetry"
	goredis "github.com/redis/go-redis/v9"
)

func main() {
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}
	httpPort := os.Getenv("HTTP_PORT")
	if httpPort == "" {
		httpPort = "8080"
	}

	redisClient := goredis.NewClient(&goredis.Options{Addr: redisAddr})
	store := redisstore.NewRedisStore(redisClient)

	clock := core.NewSystemClock()
	cache := limiter.NewShardedCache(64, clock)
	breaker := circuitbreaker.NewCircuitBreaker(circuitbreaker.BreakerConfig{
		FailureThreshold: 5,
		CooldownDuration: 5 * time.Second,
	}, clock)

	// Register Prometheus metrics collector with default registry.
	_ = telemetry.NewMetricsCollector()

	engine := limiter.NewEngine(cache, store, breaker, clock)
	handler := serverhttp.NewGatewayHandler(engine)

	server := &http.Server{
		Addr:              ":" + httpPort,
		Handler:           handler,
		ReadHeaderTimeout: 3 * time.Second,
		ReadTimeout:       5 * time.Second,
		WriteTimeout:      10 * time.Second,
	}

	go func() {
		log.Printf("Rate Limiter HTTP Service listening on port %s", httpPort)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server error: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		log.Printf("Server shutdown error: %v", err)
	}
	if err := redisClient.Close(); err != nil {
		log.Printf("Redis client close error: %v", err)
	}
	fmt.Println("Server gracefully stopped.")
}
