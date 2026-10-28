package circuitbreaker

import (
	"sync"
	"time"

	"github.com/distributed-rate-limiter/internal/core"
)

var _ core.CircuitBreaker = (*Breaker)(nil)

// BreakerConfig defines the configuration parameters for a CircuitBreaker.
type BreakerConfig struct {
	FailureThreshold int
	CooldownDuration time.Duration
}

// Breaker implements a thread-safe 3-state circuit breaker (Closed, Open, HalfOpen).
type Breaker struct {
	mu               sync.Mutex
	state            core.BreakerState
	consecutiveFails int
	lastStateChange  time.Time
	canaryInFlight   bool
	cfg              BreakerConfig
	clock            core.Clock
}

// NewCircuitBreaker constructs a Breaker instance with sane defaults and an injected Clock.
func NewCircuitBreaker(cfg BreakerConfig, clock core.Clock) *Breaker {
	if cfg.FailureThreshold <= 0 {
		cfg.FailureThreshold = 5
	}
	if cfg.CooldownDuration <= 0 {
		cfg.CooldownDuration = 5 * time.Second
	}
	if clock == nil {
		clock = core.NewSystemClock()
	}

	return &Breaker{
		state:           core.StateClosed,
		lastStateChange: clock.Now(),
		cfg:             cfg,
		clock:           clock,
	}
}

// Allow reports whether a request or probe is permitted according to current breaker state.
func (b *Breaker) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	now := b.clock.Now()
	if b.state == core.StateOpen {
		if now.Sub(b.lastStateChange) >= b.cfg.CooldownDuration {
			b.state = core.StateHalfOpen
			b.lastStateChange = now
			b.canaryInFlight = true
			return true
		}
		return false
	}

	if b.state == core.StateHalfOpen {
		if b.canaryInFlight {
			return false
		}
		b.canaryInFlight = true
		return true
	}

	return true
}

// RecordSuccess registers a successful operation, resetting consecutive failures
// and transitioning from HalfOpen back to Closed.
func (b *Breaker) RecordSuccess() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.canaryInFlight = false
	b.consecutiveFails = 0
	if b.state == core.StateHalfOpen {
		b.state = core.StateClosed
		b.lastStateChange = b.clock.Now()
	}
}

// RecordFailure registers a failed operation. If in HalfOpen or consecutive failures
// meet or exceed FailureThreshold, the breaker transitions to Open.
func (b *Breaker) RecordFailure() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.canaryInFlight = false
	b.consecutiveFails++
	now := b.clock.Now()

	if b.state == core.StateHalfOpen || b.consecutiveFails >= b.cfg.FailureThreshold {
		b.state = core.StateOpen
		b.lastStateChange = now
	}
}

// State returns the current BreakerState, accounting for cooldown elapsed in StateOpen.
func (b *Breaker) State() core.BreakerState {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.state == core.StateOpen && b.clock.Now().Sub(b.lastStateChange) >= b.cfg.CooldownDuration {
		return core.StateHalfOpen
	}
	return b.state
}
