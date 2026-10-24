package redis

import (
	"context"
	_ "embed"
	"fmt"
	"time"

	"github.com/distributed-rate-limiter/internal/core"
	goredis "github.com/redis/go-redis/v9"
)

//go:embed script.lua
var luaScriptContent string

// Store implements core.RemoteStore using Redis and an embedded atomic Lua script.
type Store struct {
	client *goredis.Client
	script *goredis.Script
}

// NewRedisStore constructs a new Store backed by the provided Redis client.
func NewRedisStore(client *goredis.Client) *Store {
	return &Store{
		client: client,
		script: goredis.NewScript(luaScriptContent),
	}
}

// Ping verifies connectivity with the Redis server.
func (s *Store) Ping(ctx context.Context) error {
	return s.client.Ping(ctx).Err()
}

// AcquireLease atomically calculates token availability via continuous refill in Lua,
// persists state changes to Redis, and returns the granted batch.
func (s *Store) AcquireLease(ctx context.Context, key string, requestedBatch int64, capacity int64, refillRate float64, ttl time.Duration) (core.LeaseResult, error) {
	nowUnix := float64(time.Now().UnixNano()) / 1e9
	ttlSec := int(ttl.Seconds())
	if ttlSec <= 0 {
		ttlSec = 3600
	}

	redisKey := fmt.Sprintf("ratelimit:%s", key)
	res, err := s.script.Run(ctx, s.client, []string{redisKey}, requestedBatch, capacity, refillRate, nowUnix, ttlSec).Result()
	if err != nil {
		return core.LeaseResult{}, err
	}

	values, ok := res.([]interface{})
	if !ok || len(values) < 3 {
		return core.LeaseResult{}, fmt.Errorf("unexpected lua result format: %v", res)
	}

	granted, err := toInt64(values[0])
	if err != nil {
		return core.LeaseResult{}, fmt.Errorf("invalid granted tokens: %w", err)
	}
	remaining, err := toInt64(values[1])
	if err != nil {
		return core.LeaseResult{}, fmt.Errorf("invalid remaining tokens: %w", err)
	}
	resetAfterSec, err := toInt64(values[2])
	if err != nil {
		return core.LeaseResult{}, fmt.Errorf("invalid reset_after: %w", err)
	}

	return core.LeaseResult{
		GrantedTokens: granted,
		Remaining:     remaining,
		ResetAfter:    time.Duration(resetAfterSec) * time.Second,
	}, nil
}

func toInt64(val interface{}) (int64, error) {
	switch v := val.(type) {
	case int64:
		return v, nil
	case int:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case float64:
		return int64(v), nil
	case float32:
		return int64(v), nil
	default:
		return 0, fmt.Errorf("expected integer numeric value, got %T: %v", val, val)
	}
}
