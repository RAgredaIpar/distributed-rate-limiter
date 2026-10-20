package core

import (
	"context"
	"testing"
	"time"
)

type dummyLimiter struct{}

func (d *dummyLimiter) Allow(ctx context.Context, req Request) (Decision, error) {
	return Decision{
		Allowed:    true,
		Remaining:  req.Capacity - req.Cost,
		ResetAfter: time.Second,
		TierUsed:   TierLocal,
		Reason:     "OK",
	}, nil
}

type dummyStore struct{}

func (s *dummyStore) AcquireLease(ctx context.Context, key string, requestedBatch int64, capacity int64, refillRate float64, ttl time.Duration) (LeaseResult, error) {
	return LeaseResult{GrantedTokens: requestedBatch, Remaining: capacity - requestedBatch, ResetAfter: ttl}, nil
}

func (s *dummyStore) Ping(ctx context.Context) error {
	return nil
}

type dummyBreaker struct{}

func (b *dummyBreaker) Allow() bool         { return true }
func (b *dummyBreaker) RecordSuccess()      {}
func (b *dummyBreaker) RecordFailure()      {}
func (b *dummyBreaker) State() BreakerState { return StateClosed }

var (
	_ Limiter        = (*dummyLimiter)(nil)
	_ RemoteStore    = (*dummyStore)(nil)
	_ CircuitBreaker = (*dummyBreaker)(nil)
	_ Clock          = (*SystemClock)(nil)
)

func TestCoreTypesInstantiation(t *testing.T) {
	req := Request{
		Key:         "tenant:alpha:user:1",
		Cost:        1,
		Capacity:    100,
		RefillRate:  10.0,
		BatchSize:   20,
		Criticality: CriticalityStandard,
	}

	if req.Key != "tenant:alpha:user:1" {
		t.Fatalf("expected key tenant:alpha:user:1, got %s", req.Key)
	}

	decision := Decision{
		Allowed:    true,
		Remaining:  99,
		ResetAfter: 100 * time.Millisecond,
		TierUsed:   TierLocal,
		Reason:     "OK",
	}

	if !decision.Allowed || decision.TierUsed != TierLocal {
		t.Fatalf("expected allowed on local tier, got allowed=%v tier=%s", decision.Allowed, decision.TierUsed)
	}

	clock := NewSystemClock()
	now := clock.Now()
	if now.IsZero() {
		t.Fatal("expected non-zero time from SystemClock")
	}
}

func TestCoreConstantsAndInterfaces(t *testing.T) {
	if TierLocal != "local" || TierRemote != "remote" || TierFallback != "fallback" {
		t.Fatal("tier constants mismatch")
	}

	if CriticalityStandard != "standard" || CriticalityCritical != "critical" {
		t.Fatal("criticality constants mismatch")
	}

	if StateClosed != "closed" || StateOpen != "open" || StateHalfOpen != "half_open" {
		t.Fatal("circuit breaker state constants mismatch")
	}

	lim := &dummyLimiter{}
	dec, err := lim.Allow(context.Background(), Request{Capacity: 10, Cost: 2})
	if err != nil || !dec.Allowed || dec.Remaining != 8 {
		t.Fatalf("unexpected limiter response: dec=%+v err=%v", dec, err)
	}

	store := &dummyStore{}
	lease, err := store.AcquireLease(context.Background(), "k", 5, 10, 1.0, time.Second)
	if err != nil || lease.GrantedTokens != 5 {
		t.Fatalf("unexpected store response: lease=%+v err=%v", lease, err)
	}
	if err := store.Ping(context.Background()); err != nil {
		t.Fatalf("store ping failed: %v", err)
	}

	breaker := &dummyBreaker{}
	if !breaker.Allow() || breaker.State() != StateClosed {
		t.Fatalf("breaker failed")
	}
}
