package limiter

import (
	"context"
	"time"

	"github.com/distributed-rate-limiter/internal/core"
)

var _ core.Limiter = (*Engine)(nil)

// Engine orchestrates two-tier rate limiting across local RAM cache and remote store.
type Engine struct {
	cache   *ShardedCache
	store   core.RemoteStore
	breaker core.CircuitBreaker
	clock   core.Clock
}

// NewEngine constructs a new Engine coordinating Tier 1 and Tier 2 rate-limiting tiers.
// If clock is nil, core.NewSystemClock() is used as the default time provider.
func NewEngine(cache *ShardedCache, store core.RemoteStore, breaker core.CircuitBreaker, clock core.Clock) *Engine {
	if clock == nil {
		clock = core.NewSystemClock()
	}
	return &Engine{
		cache:   cache,
		store:   store,
		breaker: breaker,
		clock:   clock,
	}
}

// Allow evaluates an inbound rate-limiting request against Tier 1 (local RAM) and Tier 2 (remote store).
// It manages circuit breaking, background refills, and hybrid fallback policies.
func (e *Engine) Allow(ctx context.Context, req core.Request) (core.Decision, error) {
	if req.Cost <= 0 {
		req.Cost = 1
	}
	if req.BatchSize <= 0 {
		req.BatchSize = 20
	}
	if req.Criticality != core.CriticalityCritical {
		req.Criticality = core.CriticalityStandard
	}

	// Requests with Cost exceeding bucket Capacity can never be satisfied.
	if req.Capacity > 0 && req.Cost > req.Capacity {
		return core.Decision{
			Allowed:    false,
			Remaining:  0,
			ResetAfter: 0,
			TierUsed:   core.TierLocal,
			Reason:     "COST_EXCEEDS_CAPACITY",
		}, nil
	}

	// 1. Try Tier 1: Local RAM Cache
	allowed, remaining, needsRefill := e.cache.TryConsumeLocal(req.Key, req.Cost)
	if allowed {
		if needsRefill {
			go e.asyncRefill(context.Background(), req)
		}
		return core.Decision{
			Allowed:    true,
			Remaining:  remaining,
			ResetAfter: 0,
			TierUsed:   core.TierLocal,
			Reason:     "OK_LOCAL_TIER",
		}, nil
	}

	// 2. Cache miss or insufficient local tokens: verify Circuit Breaker
	if !e.breaker.Allow() {
		return e.handleFallback(req, "CIRCUIT_BREAKER_OPEN")
	}

	// Ensure batch size can fulfill at least the requested cost
	batchSize := req.BatchSize
	if batchSize < req.Cost {
		batchSize = req.Cost
	}
	if req.Capacity > 0 && batchSize > req.Capacity {
		batchSize = req.Capacity
	}

	// 3. Try Tier 2: Remote Store (Redis Lua lease)
	lease, err := e.store.AcquireLease(ctx, req.Key, batchSize, req.Capacity, req.RefillRate, time.Minute)
	if err != nil {
		e.breaker.RecordFailure()
		return e.handleFallback(req, err.Error())
	}

	e.breaker.RecordSuccess()

	if lease.GrantedTokens < req.Cost {
		e.cache.ClearRefilling(req.Key)
		return core.Decision{
			Allowed:    false,
			Remaining:  lease.Remaining,
			ResetAfter: lease.ResetAfter,
			TierUsed:   core.TierRemote,
			Reason:     "QUOTA_EXCEEDED",
		}, nil
	}

	// Deduct cost and store surplus batch tokens in local cache
	localGranted := lease.GrantedTokens - req.Cost
	if localGranted > 0 {
		e.cache.ReplenishBatch(req.Key, localGranted, time.Minute)
	} else {
		e.cache.ClearRefilling(req.Key)
	}

	return core.Decision{
		Allowed:    true,
		Remaining:  lease.Remaining,
		ResetAfter: 0,
		TierUsed:   core.TierRemote,
		Reason:     "OK_REMOTE_TIER",
	}, nil
}

func (e *Engine) asyncRefill(ctx context.Context, req core.Request) {
	if !e.breaker.Allow() {
		e.cache.ClearRefilling(req.Key)
		return
	}

	batchSize := req.BatchSize
	if batchSize <= 0 {
		batchSize = 20
	}
	if req.Capacity > 0 && batchSize > req.Capacity {
		batchSize = req.Capacity
	}

	lease, err := e.store.AcquireLease(ctx, req.Key, batchSize, req.Capacity, req.RefillRate, time.Minute)
	if err != nil {
		e.breaker.RecordFailure()
		e.cache.ClearRefilling(req.Key)
		return
	}

	e.breaker.RecordSuccess()
	if lease.GrantedTokens > 0 {
		e.cache.ReplenishBatch(req.Key, lease.GrantedTokens, time.Minute)
	} else {
		e.cache.ClearRefilling(req.Key)
	}
}

func (e *Engine) handleFallback(req core.Request, reason string) (core.Decision, error) {
	if req.Criticality == core.CriticalityStandard {
		return core.Decision{
			Allowed:    true,
			Remaining:  0,
			ResetAfter: 0,
			TierUsed:   core.TierFallback,
			Reason:     "FAIL_OPEN_DEGRADED: " + reason,
		}, nil
	}

	return core.Decision{
		Allowed:    false,
		Remaining:  0,
		ResetAfter: 5 * time.Second,
		TierUsed:   core.TierFallback,
		Reason:     "FAIL_CLOSED_CRITICAL_BLOCKED: " + reason,
	}, nil
}
