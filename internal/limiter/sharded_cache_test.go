package limiter

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/distributed-rate-limiter/internal/core"
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

func TestShardedCacheBatchConsumptionAndWatermark(t *testing.T) {
	cache := NewShardedCache(64, core.NewSystemClock())

	// Replenish 20 tokens for key with 5s expiration
	cache.ReplenishBatch("tenant:test", 20, 5*time.Second)

	// Consume 15 tokens -> remaining = 5 (> 20% of 20 = 4, watermark at 4)
	allowed, remaining, needsRefill := cache.TryConsumeLocal("tenant:test", 15)
	if !allowed || remaining != 5 {
		t.Fatalf("expected allowed=true with 5 remaining, got %v with %d", allowed, remaining)
	}
	if needsRefill {
		t.Fatal("expected needsRefill=false when remaining is 5 (> 4)")
	}

	// Consume 2 more -> remaining = 3 (low watermark triggered: 3 <= 4)
	allowed, remaining, needsRefill = cache.TryConsumeLocal("tenant:test", 2)
	if !allowed || remaining != 3 || !needsRefill {
		t.Fatalf("expected allowed=true, remaining=3, needsRefill=true; got %v, %d, %v", allowed, remaining, needsRefill)
	}
}

func TestShardedCacheConcurrencyRace(t *testing.T) {
	cache := NewShardedCache(64, core.NewSystemClock())
	cache.ReplenishBatch("tenant:concurrent", 1000, time.Minute)

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				cache.TryConsumeLocal("tenant:concurrent", 1)
			}
		}()
	}
	wg.Wait()

	tokens, exists := cache.GetTokens("tenant:concurrent")
	if !exists {
		t.Fatal("expected key to exist")
	}
	if tokens != 500 {
		t.Fatalf("expected exactly 500 tokens remaining, got %d", tokens)
	}
}

func TestShardedCacheThunderingHerdPrevention(t *testing.T) {
	cache := NewShardedCache(64, core.NewSystemClock())
	// Initial batch: 10 tokens. Watermark is 10/5 = 2.
	cache.ReplenishBatch("tenant:herd", 10, time.Minute)

	// Consume 8 tokens -> remaining = 2 (triggers watermark 2 <= 2)
	allowed, remaining, needsRefill := cache.TryConsumeLocal("tenant:herd", 8)
	if !allowed || remaining != 2 || !needsRefill {
		t.Fatalf("expected allowed=true, remaining=2, needsRefill=true; got %v, %d, %v", allowed, remaining, needsRefill)
	}

	// Second consume under watermark: remaining = 1 (1 <= 2).
	// Atomic CAS should prevent duplicate refill trigger!
	allowed, remaining, needsRefill = cache.TryConsumeLocal("tenant:herd", 1)
	if !allowed || remaining != 1 {
		t.Fatalf("expected allowed=true, remaining=1; got %v, %d", allowed, remaining)
	}
	if needsRefill {
		t.Fatal("expected needsRefill=false due to atomic CAS thundering-herd guard")
	}
}

func TestShardedCacheClearRefilling(t *testing.T) {
	cache := NewShardedCache(64, core.NewSystemClock())
	cache.ReplenishBatch("tenant:clear", 10, time.Minute)

	// Trigger refill at watermark
	_, _, needsRefill := cache.TryConsumeLocal("tenant:clear", 9)
	if !needsRefill {
		t.Fatal("expected needsRefill=true")
	}

	// Next consume without clear should have needsRefill=false
	_, _, needsRefill = cache.TryConsumeLocal("tenant:clear", 0)
	if needsRefill {
		t.Fatal("expected needsRefill=false before clearing")
	}

	// Clear refilling flag (e.g. after remote store failure or completion)
	cache.ClearRefilling("tenant:clear")

	// Now consuming under watermark should trigger needsRefill=true again
	_, _, needsRefill = cache.TryConsumeLocal("tenant:clear", 0)
	if !needsRefill {
		t.Fatal("expected needsRefill=true after ClearRefilling")
	}
}

func TestShardedCacheTTLExpiration(t *testing.T) {
	clock := &mockClock{current: time.Unix(1000, 0)}
	cache := NewShardedCache(64, clock)

	cache.ReplenishBatch("tenant:ttl", 50, 10*time.Second)

	// Within TTL: consumption succeeds
	clock.Advance(5 * time.Second)
	allowed, remaining, needsRefill := cache.TryConsumeLocal("tenant:ttl", 10)
	if !allowed || remaining != 40 || needsRefill {
		t.Fatalf("expected allowed=true, remaining=40, needsRefill=false; got %v, %d, %v", allowed, remaining, needsRefill)
	}

	// Advance past TTL (10s total elapsed from start)
	clock.Advance(6 * time.Second) // Now at 11s elapsed
	allowed, remaining, needsRefill = cache.TryConsumeLocal("tenant:ttl", 5)
	if allowed || remaining != 0 || !needsRefill {
		t.Fatalf("expected allowed=false, remaining=0, needsRefill=true for expired entry; got %v, %d, %v", allowed, remaining, needsRefill)
	}
}

func TestShardedCacheMissingAndInsufficientTokens(t *testing.T) {
	cache := NewShardedCache(64, core.NewSystemClock())

	// Non-existent key
	allowed, remaining, needsRefill := cache.TryConsumeLocal("tenant:nonexistent", 1)
	if allowed || remaining != 0 || !needsRefill {
		t.Fatalf("expected allowed=false, remaining=0, needsRefill=true for missing key; got %v, %d, %v", allowed, remaining, needsRefill)
	}

	// Key with insufficient tokens
	cache.ReplenishBatch("tenant:insufficient", 5, time.Minute)
	allowed, remaining, needsRefill = cache.TryConsumeLocal("tenant:insufficient", 10)
	if allowed || remaining != 5 || !needsRefill {
		t.Fatalf("expected allowed=false, remaining=5, needsRefill=true for insufficient tokens; got %v, %d, %v", allowed, remaining, needsRefill)
	}
}

func TestShardedCacheDefaultsAndDistribution(t *testing.T) {
	// Defaults when numShards <= 0 and clock == nil
	cache := NewShardedCache(0, nil)
	if cache.ShardCount() != 64 {
		t.Fatalf("expected 64 shards by default, got %d", cache.ShardCount())
	}

	// Verify distribution: hash multiple keys and confirm they map to more than 1 shard
	shardsSeen := make(map[int]bool)
	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("tenant:dist:%d", i)
		idx := cache.GetShardIndex(key)
		shardsSeen[idx] = true
	}
	if len(shardsSeen) < 30 {
		t.Fatalf("expected broad shard distribution across 64 shards, only saw %d shards", len(shardsSeen))
	}
}

func TestShardedCacheEdgeCases(t *testing.T) {
	cache := NewShardedCache(64, core.NewSystemClock())
	cache.ReplenishBatch("tenant:edge", 10, time.Minute)

	// Negative cost should be treated safely (cost clamped to 0)
	allowed, remaining, needsRefill := cache.TryConsumeLocal("tenant:edge", -5)
	if !allowed || remaining != 10 || needsRefill {
		t.Fatalf("expected allowed=true, remaining=10, needsRefill=false; got %v, %d, %v", allowed, remaining, needsRefill)
	}

	// Replenishing expired entry resets stale tokens
	clock := &mockClock{current: time.Unix(2000, 0)}
	timedCache := NewShardedCache(16, clock)
	timedCache.ReplenishBatch("tenant:reexpire", 20, 5*time.Second)

	clock.Advance(10 * time.Second) // Expired
	// Replenish new batch of 15
	timedCache.ReplenishBatch("tenant:reexpire", 15, 5*time.Second)
	tokens, exists := timedCache.GetTokens("tenant:reexpire")
	if !exists || tokens != 15 {
		t.Fatalf("expected expired tokens to be reset and new balance=15, got exists=%v tokens=%d", exists, tokens)
	}
}
