package circuitbreaker

import (
	"sync"
	"testing"
	"time"

	"github.com/distributed-rate-limiter/internal/core"
)

type mockClock struct {
	mu  sync.Mutex
	now time.Time
}

func (m *mockClock) Now() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.now
}

func (m *mockClock) Advance(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.now = m.now.Add(d)
}

func TestCircuitBreakerInterfaceCompliance(t *testing.T) {
	var _ core.CircuitBreaker = (*Breaker)(nil)
}

func TestCircuitBreakerStateTransitions(t *testing.T) {
	clock := &mockClock{now: time.Now()}
	cfg := BreakerConfig{
		FailureThreshold: 3,
		CooldownDuration: 5 * time.Second,
	}

	cb := NewCircuitBreaker(cfg, clock)

	if cb.State() != core.StateClosed {
		t.Fatalf("expected closed, got %s", cb.State())
	}

	// Record 2 failures -> should remain Closed
	cb.RecordFailure()
	cb.RecordFailure()
	if cb.State() != core.StateClosed {
		t.Fatalf("expected closed after 2 failures, got %s", cb.State())
	}

	// 3rd failure trips the breaker to Open
	cb.RecordFailure()
	if cb.State() != core.StateOpen {
		t.Fatalf("expected open after 3 failures, got %s", cb.State())
	}

	// While Open, Allow() returns false
	if cb.Allow() {
		t.Fatal("expected Allow()=false while Open")
	}

	// Before cooldown expires, State() reports StateOpen
	clock.Advance(4 * time.Second)
	if cb.State() != core.StateOpen {
		t.Fatalf("expected open before cooldown expires, got %s", cb.State())
	}

	// Advance clock past cooldown -> state transitions to HalfOpen
	clock.Advance(2 * time.Second) // total 6 seconds > 5 seconds
	if cb.State() != core.StateHalfOpen {
		t.Fatalf("expected half-open when inspecting state after cooldown, got %s", cb.State())
	}

	if !cb.Allow() {
		t.Fatal("expected trial request to be allowed in HalfOpen")
	}
	if cb.State() != core.StateHalfOpen {
		t.Fatalf("expected half-open after cooldown, got %s", cb.State())
	}

	// Successful trial request recovers breaker to Closed
	cb.RecordSuccess()
	if cb.State() != core.StateClosed {
		t.Fatalf("expected closed after trial success, got %s", cb.State())
	}
}

func TestCircuitBreakerHalfOpenFailureTripsToOpen(t *testing.T) {
	clock := &mockClock{now: time.Now()}
	cfg := BreakerConfig{
		FailureThreshold: 2,
		CooldownDuration: 3 * time.Second,
	}

	cb := NewCircuitBreaker(cfg, clock)

	// Trip to Open
	cb.RecordFailure()
	cb.RecordFailure()
	if cb.State() != core.StateOpen {
		t.Fatalf("expected open, got %s", cb.State())
	}

	// Advance past cooldown
	clock.Advance(4 * time.Second)
	if cb.State() != core.StateHalfOpen {
		t.Fatalf("expected half-open, got %s", cb.State())
	}
	if !cb.Allow() {
		t.Fatal("expected trial request allowed")
	}

	// Failure in HalfOpen immediately trips back to Open
	cb.RecordFailure()
	if cb.State() != core.StateOpen {
		t.Fatalf("expected open immediately after failure in half-open, got %s", cb.State())
	}
	if cb.Allow() {
		t.Fatal("expected Allow()=false after tripping back to Open")
	}
}

func TestCircuitBreakerSuccessResetsFailures(t *testing.T) {
	clock := &mockClock{now: time.Now()}
	cfg := BreakerConfig{
		FailureThreshold: 3,
		CooldownDuration: 5 * time.Second,
	}

	cb := NewCircuitBreaker(cfg, clock)

	// Record 2 failures (< threshold)
	cb.RecordFailure()
	cb.RecordFailure()
	if cb.State() != core.StateClosed {
		t.Fatalf("expected closed, got %s", cb.State())
	}

	// Record success resets counter
	cb.RecordSuccess()

	// Record 2 more failures -> should still remain closed
	cb.RecordFailure()
	cb.RecordFailure()
	if cb.State() != core.StateClosed {
		t.Fatalf("expected closed after reset + 2 failures, got %s", cb.State())
	}

	// 3rd failure trips
	cb.RecordFailure()
	if cb.State() != core.StateOpen {
		t.Fatalf("expected open on 3rd failure after reset, got %s", cb.State())
	}
}

func TestCircuitBreakerDefaultsAndNilClock(t *testing.T) {
	// Zero/negative values and nil clock should receive safe defaults
	cb := NewCircuitBreaker(BreakerConfig{
		FailureThreshold: 0,
		CooldownDuration: 0,
	}, nil)

	if cb.State() != core.StateClosed {
		t.Fatalf("expected closed initially, got %s", cb.State())
	}
	if !cb.Allow() {
		t.Fatal("expected Allow()=true initially")
	}

	// Default failure threshold is 5
	for i := 0; i < 4; i++ {
		cb.RecordFailure()
		if cb.State() != core.StateClosed {
			t.Fatalf("expected closed at failure %d, got %s", i+1, cb.State())
		}
	}
	cb.RecordFailure() // 5th failure
	if cb.State() != core.StateOpen {
		t.Fatalf("expected open after 5 failures, got %s", cb.State())
	}
}

func TestCircuitBreakerConcurrentAccess(t *testing.T) {
	clock := &mockClock{now: time.Now()}
	cfg := BreakerConfig{
		FailureThreshold: 10,
		CooldownDuration: 100 * time.Millisecond,
	}

	cb := NewCircuitBreaker(cfg, clock)

	const goroutines = 20
	const iterations = 100
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				_ = cb.Allow()
				_ = cb.State()
				if j%3 == 0 {
					cb.RecordFailure()
				} else if j%5 == 0 {
					cb.RecordSuccess()
				}
				if j%10 == 0 {
					clock.Advance(20 * time.Millisecond)
				}
			}
		}(i)
	}

	wg.Wait()
}

func TestCircuitBreakerHalfOpenSingleCanaryGuard(t *testing.T) {
	clock := &mockClock{now: time.Now()}
	cfg := BreakerConfig{
		FailureThreshold: 3,
		CooldownDuration: 5 * time.Second,
	}

	cb := NewCircuitBreaker(cfg, clock)

	// Trip to StateOpen
	cb.RecordFailure()
	cb.RecordFailure()
	cb.RecordFailure()
	if cb.State() != core.StateOpen {
		t.Fatalf("expected state Open, got %s", cb.State())
	}

	// Advance past cooldown duration
	clock.Advance(6 * time.Second)
	if cb.State() != core.StateHalfOpen {
		t.Fatalf("expected state HalfOpen, got %s", cb.State())
	}

	// 1. First probe should be allowed as canary
	if !cb.Allow() {
		t.Fatal("expected first probe in HalfOpen to be allowed as canary")
	}

	// 2. Concurrent/subsequent probes while canary is in-flight must be rejected
	for i := 0; i < 5; i++ {
		if cb.Allow() {
			t.Fatalf("expected concurrent probe %d to be blocked while canary in-flight", i+1)
		}
	}

	// 3. Canary succeeds -> transitions to Closed and resets canary guard
	cb.RecordSuccess()
	if cb.State() != core.StateClosed {
		t.Fatalf("expected state Closed after canary success, got %s", cb.State())
	}
	if !cb.Allow() {
		t.Fatal("expected Allow()=true in Closed state")
	}

	// 4. Trip again to test failure path with canary
	cb.RecordFailure()
	cb.RecordFailure()
	cb.RecordFailure()
	if cb.State() != core.StateOpen {
		t.Fatalf("expected state Open, got %s", cb.State())
	}

	clock.Advance(6 * time.Second)
	if !cb.Allow() {
		t.Fatal("expected first canary probe allowed after second cooldown")
	}
	if cb.Allow() {
		t.Fatal("expected second probe blocked while canary in-flight")
	}

	// Canary fails -> trips immediately to Open and resets canary guard
	cb.RecordFailure()
	if cb.State() != core.StateOpen {
		t.Fatalf("expected state Open after canary failure, got %s", cb.State())
	}
	if cb.Allow() {
		t.Fatal("expected Allow()=false in Open state")
	}
}
