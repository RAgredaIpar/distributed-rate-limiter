package tokenbucket

import (
	"math"
	"sync"
	"time"

	"github.com/distributed-rate-limiter/internal/core"
)

// Bucket implements an in-memory token bucket rate limiter with lazy continuous refill.
type Bucket struct {
	mu         sync.Mutex
	capacity   float64
	tokens     float64
	refillRate float64
	lastRefill time.Time
	clock      core.Clock
}

// NewBucket initializes a new token bucket with the given capacity, continuous refill rate
// (tokens per second), and a Clock interface for deterministic time progression.
// If clock is nil, core.NewSystemClock() is used by default.
func NewBucket(capacity int64, refillRate float64, clock core.Clock) *Bucket {
	if clock == nil {
		clock = core.NewSystemClock()
	}
	return &Bucket{
		capacity:   float64(capacity),
		tokens:     float64(capacity),
		refillRate: refillRate,
		lastRefill: clock.Now(),
		clock:      clock,
	}
}

// Take attempts to consume cost tokens from the bucket.
// Returns:
// - allowed: true if the requested tokens were consumed, false otherwise.
// - remaining: current remaining tokens (floored to integer).
// - resetAfter: estimated duration until enough tokens will be refilled to satisfy this cost if rejected.
func (b *Bucket) Take(cost int64) (bool, int64, time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()

	now := b.clock.Now()
	elapsed := now.Sub(b.lastRefill).Seconds()
	if elapsed > 0 {
		b.tokens = math.Min(b.capacity, b.tokens+elapsed*b.refillRate)
		b.lastRefill = now
	}

	if cost < 0 {
		cost = 0
	}

	costF := float64(cost)
	if b.tokens >= costF {
		b.tokens -= costF
		remaining := int64(math.Floor(b.tokens))
		return true, remaining, 0
	}

	needed := costF - b.tokens
	var resetAfter time.Duration
	if b.refillRate > 0 {
		resetAfter = time.Duration((needed / b.refillRate) * float64(time.Second))
	}

	return false, int64(math.Floor(b.tokens)), resetAfter
}
