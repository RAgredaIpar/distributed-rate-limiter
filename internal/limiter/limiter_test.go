package limiter

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/distributed-rate-limiter/internal/circuitbreaker"
	"github.com/distributed-rate-limiter/internal/core"
)

var _ core.Limiter = (*Engine)(nil)

type mockRemoteStore struct {
	acquireFunc func(ctx context.Context, key string, requestedBatch int64, capacity int64, refillRate float64, ttl time.Duration) (core.LeaseResult, error)
	pingFunc    func(ctx context.Context) error
}

func (m *mockRemoteStore) AcquireLease(ctx context.Context, key string, requestedBatch int64, capacity int64, refillRate float64, ttl time.Duration) (core.LeaseResult, error) {
	if m.acquireFunc != nil {
		return m.acquireFunc(ctx, key, requestedBatch, capacity, refillRate, ttl)
	}
	return core.LeaseResult{}, nil
}

func (m *mockRemoteStore) Ping(ctx context.Context) error {
	if m.pingFunc != nil {
		return m.pingFunc(ctx)
	}
	return nil
}

func TestTwoTierEngineEndToEnd(t *testing.T) {
	ctx := context.Background()
	clock := core.NewSystemClock()
	cache := NewShardedCache(16, clock)
	breaker := circuitbreaker.NewCircuitBreaker(circuitbreaker.BreakerConfig{FailureThreshold: 2}, clock)

	remoteCalls := 0
	store := &mockRemoteStore{
		acquireFunc: func(ctx context.Context, key string, requestedBatch int64, capacity int64, refillRate float64, ttl time.Duration) (core.LeaseResult, error) {
			remoteCalls++
			return core.LeaseResult{
				GrantedTokens: requestedBatch,
				Remaining:     capacity - requestedBatch,
				ResetAfter:    0,
			}, nil
		},
	}

	engine := NewEngine(cache, store, breaker, clock)

	req := core.Request{
		Key:         "tenant:alpha",
		Cost:        1,
		Capacity:    100,
		RefillRate:  10.0,
		BatchSize:   10,
		Criticality: core.CriticalityStandard,
	}

	// 1st request: Cache miss -> triggers remote call
	dec, err := engine.Allow(ctx, req)
	if err != nil || !dec.Allowed || dec.TierUsed != core.TierRemote {
		t.Fatalf("expected TierRemote allowed, got %v tier=%s err=%v", dec.Allowed, dec.TierUsed, err)
	}

	// 2nd request: Hits local cache directly
	dec2, err := engine.Allow(ctx, req)
	if err != nil || !dec2.Allowed || dec2.TierUsed != core.TierLocal {
		t.Fatalf("expected TierLocal allowed, got %v tier=%s err=%v", dec2.Allowed, dec2.TierUsed, err)
	}

	if remoteCalls != 1 {
		t.Fatalf("expected exactly 1 remote call, got %d", remoteCalls)
	}
}

func TestTwoTierCircuitBreakerFallback(t *testing.T) {
	ctx := context.Background()
	clock := core.NewSystemClock()
	cache := NewShardedCache(16, clock)
	breaker := circuitbreaker.NewCircuitBreaker(circuitbreaker.BreakerConfig{FailureThreshold: 1, CooldownDuration: time.Minute}, clock)

	store := &mockRemoteStore{
		acquireFunc: func(ctx context.Context, key string, requestedBatch int64, capacity int64, refillRate float64, ttl time.Duration) (core.LeaseResult, error) {
			return core.LeaseResult{}, errors.New("redis connection refused")
		},
	}

	engine := NewEngine(cache, store, breaker, clock)

	// Standard criticality -> Fail-Open fallback
	decStandard, err := engine.Allow(ctx, core.Request{
		Key:         "tenant:std",
		Cost:        1,
		Criticality: core.CriticalityStandard,
		BatchSize:   10,
	})
	if err != nil || !decStandard.Allowed || decStandard.TierUsed != core.TierFallback {
		t.Fatalf("expected fail-open on standard traffic, got allowed=%v tier=%s err=%v", decStandard.Allowed, decStandard.TierUsed, err)
	}

	// Critical criticality -> Fail-Closed fallback
	decCritical, err := engine.Allow(ctx, core.Request{
		Key:         "tenant:crit",
		Cost:        1,
		Criticality: core.CriticalityCritical,
		BatchSize:   10,
	})
	if err != nil || decCritical.Allowed || decCritical.TierUsed != core.TierFallback {
		t.Fatalf("expected fail-closed on critical traffic, got allowed=%v tier=%s err=%v", decCritical.Allowed, decCritical.TierUsed, err)
	}
}

func TestTwoTierCircuitBreakerOpenDirectFallback(t *testing.T) {
	ctx := context.Background()
	clock := core.NewSystemClock()
	cache := NewShardedCache(16, clock)
	breaker := circuitbreaker.NewCircuitBreaker(circuitbreaker.BreakerConfig{FailureThreshold: 1, CooldownDuration: time.Hour}, clock)

	remoteCalls := 0
	store := &mockRemoteStore{
		acquireFunc: func(ctx context.Context, key string, requestedBatch int64, capacity int64, refillRate float64, ttl time.Duration) (core.LeaseResult, error) {
			remoteCalls++
			return core.LeaseResult{}, errors.New("redis timeout")
		},
	}

	engine := NewEngine(cache, store, breaker, clock)

	// First call trips breaker
	_, _ = engine.Allow(ctx, core.Request{
		Key:         "tenant:trip",
		Cost:        1,
		Criticality: core.CriticalityStandard,
	})

	if breaker.State() != core.StateOpen {
		t.Fatalf("expected breaker to be open, got %s", breaker.State())
	}

	// Second call should NOT call remote store at all; trips via breaker Allow() == false
	remoteCallsBefore := remoteCalls
	dec, err := engine.Allow(ctx, core.Request{
		Key:         "tenant:open",
		Cost:        1,
		Criticality: core.CriticalityCritical,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if dec.Allowed {
		t.Fatal("expected fail-closed when circuit breaker is open")
	}
	if dec.TierUsed != core.TierFallback {
		t.Fatalf("expected TierFallback, got %s", dec.TierUsed)
	}
	if remoteCalls != remoteCallsBefore {
		t.Fatalf("expected no remote calls while breaker is open, had %d, now %d", remoteCallsBefore, remoteCalls)
	}
}

func TestTwoTierCostExceedsCapacity(t *testing.T) {
	ctx := context.Background()
	clock := core.NewSystemClock()
	cache := NewShardedCache(16, clock)
	breaker := circuitbreaker.NewCircuitBreaker(circuitbreaker.BreakerConfig{}, clock)

	remoteCalls := 0
	store := &mockRemoteStore{
		acquireFunc: func(ctx context.Context, key string, requestedBatch int64, capacity int64, refillRate float64, ttl time.Duration) (core.LeaseResult, error) {
			remoteCalls++
			return core.LeaseResult{GrantedTokens: requestedBatch}, nil
		},
	}

	engine := NewEngine(cache, store, breaker, clock)

	// Cost 150 > Capacity 100
	dec, err := engine.Allow(ctx, core.Request{
		Key:         "tenant:impossible",
		Cost:        150,
		Capacity:    100,
		RefillRate:  10.0,
		BatchSize:   20,
		Criticality: core.CriticalityStandard,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if dec.Allowed {
		t.Fatal("expected request with Cost > Capacity to be rejected")
	}
	if dec.Reason != "COST_EXCEEDS_CAPACITY" {
		t.Fatalf("expected Reason COST_EXCEEDS_CAPACITY, got %s", dec.Reason)
	}
	if remoteCalls != 0 {
		t.Fatalf("expected remote store to not be called, got %d calls", remoteCalls)
	}
}

func TestTwoTierQuotaExceeded(t *testing.T) {
	ctx := context.Background()
	clock := core.NewSystemClock()
	cache := NewShardedCache(16, clock)
	breaker := circuitbreaker.NewCircuitBreaker(circuitbreaker.BreakerConfig{}, clock)

	store := &mockRemoteStore{
		acquireFunc: func(ctx context.Context, key string, requestedBatch int64, capacity int64, refillRate float64, ttl time.Duration) (core.LeaseResult, error) {
			// Remote store has only 2 tokens left, but cost is 5
			return core.LeaseResult{
				GrantedTokens: 2,
				Remaining:     0,
				ResetAfter:    3 * time.Second,
			}, nil
		},
	}

	engine := NewEngine(cache, store, breaker, clock)

	dec, err := engine.Allow(ctx, core.Request{
		Key:        "tenant:limited",
		Cost:       5,
		Capacity:   20,
		RefillRate: 1.0,
		BatchSize:  10,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if dec.Allowed {
		t.Fatal("expected quota exceeded to reject request")
	}
	if dec.Reason != "QUOTA_EXCEEDED" {
		t.Fatalf("expected reason QUOTA_EXCEEDED, got %s", dec.Reason)
	}
	if dec.ResetAfter != 3*time.Second {
		t.Fatalf("expected reset after 3s, got %v", dec.ResetAfter)
	}
}

func TestTwoTierLargeCostAdjustsBatch(t *testing.T) {
	ctx := context.Background()
	clock := core.NewSystemClock()
	cache := NewShardedCache(16, clock)
	breaker := circuitbreaker.NewCircuitBreaker(circuitbreaker.BreakerConfig{}, clock)

	var lastRequestedBatch int64
	store := &mockRemoteStore{
		acquireFunc: func(ctx context.Context, key string, requestedBatch int64, capacity int64, refillRate float64, ttl time.Duration) (core.LeaseResult, error) {
			lastRequestedBatch = requestedBatch
			return core.LeaseResult{
				GrantedTokens: requestedBatch,
				Remaining:     capacity - requestedBatch,
			}, nil
		},
	}

	engine := NewEngine(cache, store, breaker, clock)

	// Cost is 50, but configured BatchSize is 10
	dec, err := engine.Allow(ctx, core.Request{
		Key:        "tenant:bigcost",
		Cost:       50,
		Capacity:   100,
		RefillRate: 10.0,
		BatchSize:  10,
	})
	if err != nil || !dec.Allowed {
		t.Fatalf("expected allowed, got allowed=%v err=%v", dec.Allowed, err)
	}
	if lastRequestedBatch < 50 {
		t.Fatalf("expected requested batch to be at least 50, got %d", lastRequestedBatch)
	}
}

func TestTwoTierAsyncRefill(t *testing.T) {
	ctx := context.Background()
	clock := core.NewSystemClock()
	cache := NewShardedCache(16, clock)
	breaker := circuitbreaker.NewCircuitBreaker(circuitbreaker.BreakerConfig{}, clock)

	// Prime local cache with 20 tokens
	cache.ReplenishBatch("tenant:refill", 20, time.Minute)

	refillDone := make(chan struct{}, 1)
	store := &mockRemoteStore{
		acquireFunc: func(ctx context.Context, key string, requestedBatch int64, capacity int64, refillRate float64, ttl time.Duration) (core.LeaseResult, error) {
			defer func() {
				select {
				case refillDone <- struct{}{}:
				default:
				}
			}()
			return core.LeaseResult{
				GrantedTokens: requestedBatch,
				Remaining:     50,
			}, nil
		},
	}

	engine := NewEngine(cache, store, breaker, clock)

	// Consume 17 tokens from 20 -> remaining = 3 (<= 20% watermark = 4)
	dec, err := engine.Allow(ctx, core.Request{
		Key:         "tenant:refill",
		Cost:        17,
		Capacity:    100,
		RefillRate:  10.0,
		BatchSize:   20,
		Criticality: core.CriticalityStandard,
	})
	if err != nil || !dec.Allowed || dec.TierUsed != core.TierLocal {
		t.Fatalf("expected TierLocal allowed, got allowed=%v tier=%s err=%v", dec.Allowed, dec.TierUsed, err)
	}

	// Wait for async refill to complete
	select {
	case <-refillDone:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for async refill")
	}

	// Tokens should now be 3 + 20 = 23
	tokens, exists := cache.GetTokens("tenant:refill")
	if !exists || tokens < 20 {
		t.Fatalf("expected cache to have at least 20 tokens after refill, got %d (exists=%v)", tokens, exists)
	}
}

func TestTwoTierAsyncRefillErrorClearsRefilling(t *testing.T) {
	ctx := context.Background()
	clock := core.NewSystemClock()
	cache := NewShardedCache(16, clock)
	breaker := circuitbreaker.NewCircuitBreaker(circuitbreaker.BreakerConfig{FailureThreshold: 5}, clock)

	cache.ReplenishBatch("tenant:err", 20, time.Minute)

	refillDone := make(chan struct{}, 1)
	store := &mockRemoteStore{
		acquireFunc: func(ctx context.Context, key string, requestedBatch int64, capacity int64, refillRate float64, ttl time.Duration) (core.LeaseResult, error) {
			defer func() {
				select {
				case refillDone <- struct{}{}:
				default:
				}
			}()
			return core.LeaseResult{}, errors.New("transient remote failure")
		},
	}

	engine := NewEngine(cache, store, breaker, clock)

	// Consume 18 tokens -> remaining = 2 (<= 4 watermark)
	_, _ = engine.Allow(ctx, core.Request{
		Key:       "tenant:err",
		Cost:      18,
		Capacity:  100,
		BatchSize: 20,
	})

	select {
	case <-refillDone:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for async refill error attempt")
	}

	// A short delay to allow ClearRefilling
	time.Sleep(10 * time.Millisecond)

	// Check that another consumption that triggers watermark can attempt refill again (refilling flag is 0)
	// Consuming 1 token: remaining = 1 (<= 4).
	_, _, needsRefill := cache.TryConsumeLocal("tenant:err", 1)
	if !needsRefill {
		t.Fatal("expected needsRefill=true after ClearRefilling reset the flag")
	}
}

func TestTwoTierConcurrentAllow(t *testing.T) {
	ctx := context.Background()
	clock := core.NewSystemClock()
	cache := NewShardedCache(16, clock)
	breaker := circuitbreaker.NewCircuitBreaker(circuitbreaker.BreakerConfig{FailureThreshold: 100}, clock)

	var remoteCount int64
	store := &mockRemoteStore{
		acquireFunc: func(ctx context.Context, key string, requestedBatch int64, capacity int64, refillRate float64, ttl time.Duration) (core.LeaseResult, error) {
			atomic.AddInt64(&remoteCount, 1)
			return core.LeaseResult{
				GrantedTokens: requestedBatch,
				Remaining:     1000,
			}, nil
		},
	}

	engine := NewEngine(cache, store, breaker, clock)

	var wg sync.WaitGroup
	workers := 20
	iterations := 25

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				_, _ = engine.Allow(ctx, core.Request{
					Key:         "tenant:concurrent",
					Cost:        1,
					Capacity:    500,
					RefillRate:  50.0,
					BatchSize:   20,
					Criticality: core.CriticalityStandard,
				})
			}
		}(i)
	}

	wg.Wait()

	remotes := atomic.LoadInt64(&remoteCount)
	if remotes == 0 {
		t.Fatal("expected at least 1 remote call during concurrent load")
	}
	// Total requests = 20 * 25 = 500. With batch size 20, remote calls should be substantially less than 500.
	if remotes >= 500 {
		t.Fatalf("expected batching to reduce remote calls, but got %d", remotes)
	}
}
