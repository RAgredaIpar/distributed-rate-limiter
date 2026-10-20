package core

import (
	"context"
	"time"
)

// Tier represents the rate-limiting tier that processed the request.
type Tier string

const (
	// TierLocal indicates the request was evaluated using the local token bucket.
	TierLocal Tier = "local"
	// TierRemote indicates the request was evaluated using the remote store.
	TierRemote Tier = "remote"
	// TierFallback indicates the request was evaluated using the degraded local fallback.
	TierFallback Tier = "fallback"
)

// Criticality indicates the priority or criticality level of a request.
type Criticality string

const (
	// CriticalityStandard represents standard priority traffic subject to normal shedding.
	CriticalityStandard Criticality = "standard"
	// CriticalityCritical represents mission-critical traffic that bypasses conservative shedding.
	CriticalityCritical Criticality = "critical"
)

// Request encapsulates the parameters required to evaluate a rate-limiting decision.
type Request struct {
	Key         string
	Cost        int64
	Capacity    int64
	RefillRate  float64
	BatchSize   int64
	Criticality Criticality
}

// Decision represents the outcome of evaluating a rate-limiting request.
type Decision struct {
	Allowed    bool
	Remaining  int64
	ResetAfter time.Duration
	TierUsed   Tier
	Reason     string
}

// Limiter is the primary entrypoint contract for checking whether requests are allowed.
type Limiter interface {
	Allow(ctx context.Context, req Request) (Decision, error)
}

// LeaseResult contains the result of acquiring a token lease from a remote store.
type LeaseResult struct {
	GrantedTokens int64
	Remaining     int64
	ResetAfter    time.Duration
}

// RemoteStore defines operations for coordinating token leases with a shared backend.
type RemoteStore interface {
	AcquireLease(ctx context.Context, key string, requestedBatch int64, capacity int64, refillRate float64, ttl time.Duration) (LeaseResult, error)
	Ping(ctx context.Context) error
}

// BreakerState represents the current operating state of a circuit breaker.
type BreakerState string

const (
	// StateClosed allows traffic to flow normally through the remote store.
	StateClosed BreakerState = "closed"
	// StateOpen trips calls to the remote store and routes traffic to fallback.
	StateOpen BreakerState = "open"
	// StateHalfOpen allows limited probe traffic to test store recovery.
	StateHalfOpen BreakerState = "half_open"
)

// CircuitBreaker guards remote store calls and manages state transitions based on failures.
type CircuitBreaker interface {
	Allow() bool
	RecordSuccess()
	RecordFailure()
	State() BreakerState
}

// Clock provides an abstraction over time to enable deterministic testing.
type Clock interface {
	Now() time.Time
}

// SystemClock provides the standard wall-clock implementation using time.Now.
type SystemClock struct{}

// NewSystemClock constructs a new Clock backed by the system wall clock.
func NewSystemClock() Clock {
	return &SystemClock{}
}

// Now returns the current local time.
func (s *SystemClock) Now() time.Time {
	return time.Now()
}
