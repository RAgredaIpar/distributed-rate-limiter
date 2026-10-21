package tokenbucket

import (
	"sync"
	"testing"
	"time"
)

type mockClock struct {
	mu      sync.Mutex
	current time.Time
}

func (m *mockClock) Now() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.current
}

func (m *mockClock) Advance(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.current = m.current.Add(d)
}

func TestBucketRefillAndTake(t *testing.T) {
	start := time.Unix(1000, 0)
	clock := &mockClock{current: start}

	// 10 tokens capacity, 2 tokens per second refill
	b := NewBucket(10, 2.0, clock)

	// Consume all 10 tokens
	allowed, remaining, _ := b.Take(10)
	if !allowed || remaining != 0 {
		t.Fatalf("expected allowed=true with 0 remaining, got %v with %d", allowed, remaining)
	}

	// Immediate next request should be rejected
	allowed, _, _ = b.Take(1)
	if allowed {
		t.Fatal("expected request to be rejected when bucket is empty")
	}

	// Advance time by 2.5 seconds -> should recover 5 tokens
	clock.Advance(2500 * time.Millisecond)

	allowed, remaining, _ = b.Take(3)
	if !allowed || remaining != 2 {
		t.Fatalf("expected allowed=true with 2 remaining after refill, got %v with %d", allowed, remaining)
	}
}

func TestBucketCapacityCap(t *testing.T) {
	start := time.Unix(1000, 0)
	clock := &mockClock{current: start}

	// 10 tokens capacity, 5 tokens per second refill
	b := NewBucket(10, 5.0, clock)

	// Consume 5 tokens -> 5 remaining
	allowed, remaining, _ := b.Take(5)
	if !allowed || remaining != 5 {
		t.Fatalf("expected allowed=true with 5 remaining, got %v with %d", allowed, remaining)
	}

	// Advance time by 100 seconds (500 tokens theoretically generated)
	clock.Advance(100 * time.Second)

	// Capacity must cap at 10
	allowed, remaining, _ = b.Take(10)
	if !allowed || remaining != 0 {
		t.Fatalf("expected allowed=true with 0 remaining after capped refill, got %v with %d", allowed, remaining)
	}

	// Cannot take any more immediately
	allowed, _, _ = b.Take(1)
	if allowed {
		t.Fatal("expected request to be rejected when bucket is empty after consuming capped capacity")
	}
}

func TestBucketResetAfterCalculation(t *testing.T) {
	start := time.Unix(1000, 0)
	clock := &mockClock{current: start}

	// 10 tokens capacity, 2 tokens per second refill
	b := NewBucket(10, 2.0, clock)

	// Drain all 10 tokens
	allowed, remaining, _ := b.Take(10)
	if !allowed || remaining != 0 {
		t.Fatalf("expected allowed=true with 0 remaining, got %v with %d", allowed, remaining)
	}

	// Request 4 tokens: bucket has 0, needed = 4, refillRate = 2.0 -> resetAfter = 2s
	allowed, remaining, resetAfter := b.Take(4)
	if allowed {
		t.Fatal("expected request to be rejected when insufficient tokens")
	}
	if remaining != 0 {
		t.Fatalf("expected remaining=0, got %d", remaining)
	}
	expectedReset := 2 * time.Second
	if resetAfter != expectedReset {
		t.Fatalf("expected resetAfter=%v, got %v", expectedReset, resetAfter)
	}

	// Advance clock by resetAfter
	clock.Advance(resetAfter)

	// Now taking 4 tokens should succeed with 0 remaining
	allowed, remaining, resetAfter = b.Take(4)
	if !allowed || remaining != 0 || resetAfter != 0 {
		t.Fatalf("expected allowed=true, remaining=0, resetAfter=0, got allowed=%v, remaining=%d, resetAfter=%v", allowed, remaining, resetAfter)
	}
}

func TestBucketZeroRefillRate(t *testing.T) {
	start := time.Unix(1000, 0)
	clock := &mockClock{current: start}

	// 5 tokens capacity, 0 refill rate
	b := NewBucket(5, 0.0, clock)

	allowed, remaining, _ := b.Take(5)
	if !allowed || remaining != 0 {
		t.Fatalf("expected allowed=true with 0 remaining, got %v with %d", allowed, remaining)
	}

	// Advance time by 1 hour -> should not refill
	clock.Advance(1 * time.Hour)

	allowed, remaining, resetAfter := b.Take(1)
	if allowed {
		t.Fatal("expected request to be rejected with 0 refill rate")
	}
	if remaining != 0 {
		t.Fatalf("expected remaining=0, got %d", remaining)
	}
	if resetAfter != 0 {
		t.Fatalf("expected resetAfter=0 when refill rate is zero, got %v", resetAfter)
	}
}

func TestBucketDefaultClock(t *testing.T) {
	// Clock is nil -> defaults to SystemClock
	b := NewBucket(5, 10.0, nil)
	if b == nil {
		t.Fatal("expected non-nil Bucket")
	}

	allowed, remaining, _ := b.Take(2)
	if !allowed || remaining != 3 {
		t.Fatalf("expected allowed=true with 3 remaining, got %v with %d", allowed, remaining)
	}
}

func TestBucketConcurrentTake(t *testing.T) {
	start := time.Unix(1000, 0)
	clock := &mockClock{current: start}

	// 1000 tokens capacity, 0 refill rate to measure strict deduction
	b := NewBucket(1000, 0.0, clock)

	var wg sync.WaitGroup
	allowedCount := 0
	var countMu sync.Mutex

	workers := 20
	iterations := 50 // 20 * 50 = 1000 total attempts of cost 1

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				allowed, _, _ := b.Take(1)
				if allowed {
					countMu.Lock()
					allowedCount++
					countMu.Unlock()
				}
			}
		}()
	}

	wg.Wait()

	if allowedCount != 1000 {
		t.Fatalf("expected exactly 1000 allowed tokens across concurrent goroutines, got %d", allowedCount)
	}

	// Next take must fail
	allowed, remaining, _ := b.Take(1)
	if allowed || remaining != 0 {
		t.Fatalf("expected bucket empty after concurrent drain, allowed=%v, remaining=%d", allowed, remaining)
	}
}
