// Package limiter provides rate-limiting algorithms, caching tiers, and coordination engines.
package limiter

import (
	"hash/fnv"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/distributed-rate-limiter/internal/core"
)

type localEntry struct {
	mu          sync.Mutex
	tokens      int64
	initialSize int64
	expiresAt   time.Time
	refilling   int32
}

type cacheShard struct {
	mu      sync.RWMutex
	entries map[string]*localEntry
}

// ShardedCache provides a highly-concurrent, partitioned in-memory cache for Tier 1 local token balances.
type ShardedCache struct {
	numShards uint64
	shards    []*cacheShard
	clock     core.Clock
}

// NewShardedCache constructs a new ShardedCache partitioned into numShards independent buckets.
// If numShards <= 0, it defaults to 64 shards. If clock is nil, it defaults to core.NewSystemClock().
func NewShardedCache(numShards int, clock core.Clock) *ShardedCache {
	if numShards <= 0 {
		numShards = 64
	}
	if clock == nil {
		clock = core.NewSystemClock()
	}

	shards := make([]*cacheShard, numShards)
	for i := 0; i < numShards; i++ {
		shards[i] = &cacheShard{
			entries: make(map[string]*localEntry),
		}
	}

	return &ShardedCache{
		numShards: uint64(numShards),
		shards:    shards,
		clock:     clock,
	}
}

// GetShardIndex calculates the target shard index for key using 64-bit FNV-1a hashing.
func (c *ShardedCache) GetShardIndex(key string) int {
	h := fnv.New64a()
	h.Write([]byte(key))
	return int(h.Sum64() % c.numShards)
}

func (c *ShardedCache) getShard(key string) *cacheShard {
	return c.shards[c.GetShardIndex(key)]
}

// ShardCount returns the total number of shards configured for this cache.
func (c *ShardedCache) ShardCount() int {
	return int(c.numShards)
}

// TryConsumeLocal attempts to deduct cost tokens from the local cache entry for key.
// Returns:
//   - allowed: true if the local cache has sufficient unexpired tokens.
//   - remaining: remaining token balance after consumption (or current balance if rejected).
//   - needsRefill: true when the balance falls to/below the 20% low watermark (via atomic CAS)
//     or when the key is missing/expired/insufficient.
func (c *ShardedCache) TryConsumeLocal(key string, cost int64) (bool, int64, bool) {
	if cost < 0 {
		cost = 0
	}

	shard := c.getShard(key)

	shard.mu.RLock()
	entry, exists := shard.entries[key]
	shard.mu.RUnlock()

	if !exists {
		return false, 0, true
	}

	entry.mu.Lock()
	defer entry.mu.Unlock()

	now := c.clock.Now()
	if !entry.expiresAt.IsZero() && now.After(entry.expiresAt) {
		entry.tokens = 0
		return false, 0, true
	}

	if entry.tokens < cost {
		return false, entry.tokens, true
	}

	entry.tokens -= cost
	remaining := entry.tokens

	lowWatermark := entry.initialSize / 5 // 20%
	needsRefill := false
	if remaining <= lowWatermark && atomic.CompareAndSwapInt32(&entry.refilling, 0, 1) {
		needsRefill = true
	}

	return true, remaining, needsRefill
}

// ReplenishBatch adds tokens to the cache entry for key, sets its TTL, and resets the refilling flag.
func (c *ShardedCache) ReplenishBatch(key string, tokens int64, ttl time.Duration) {
	if tokens < 0 {
		tokens = 0
	}

	shard := c.getShard(key)

	shard.mu.Lock()
	entry, exists := shard.entries[key]
	if !exists {
		entry = &localEntry{}
		shard.entries[key] = entry
	}
	shard.mu.Unlock()

	entry.mu.Lock()
	defer entry.mu.Unlock()

	now := c.clock.Now()
	if !entry.expiresAt.IsZero() && now.After(entry.expiresAt) {
		entry.tokens = 0
	}

	if math.MaxInt64-entry.tokens < tokens {
		entry.tokens = math.MaxInt64
	} else {
		entry.tokens += tokens
	}

	entry.initialSize = entry.tokens
	if ttl > 0 {
		entry.expiresAt = now.Add(ttl)
	} else {
		entry.expiresAt = time.Time{}
	}
	atomic.StoreInt32(&entry.refilling, 0)
}

// ClearRefilling resets the refilling flag to 0 for key, allowing future watermark triggers.
func (c *ShardedCache) ClearRefilling(key string) {
	shard := c.getShard(key)

	shard.mu.RLock()
	entry, exists := shard.entries[key]
	shard.mu.RUnlock()

	if exists {
		atomic.StoreInt32(&entry.refilling, 0)
	}
}

// GetTokens returns the current token balance and existence state for key, clearing expired entries.
func (c *ShardedCache) GetTokens(key string) (int64, bool) {
	shard := c.getShard(key)

	shard.mu.RLock()
	entry, exists := shard.entries[key]
	shard.mu.RUnlock()

	if !exists {
		return 0, false
	}

	entry.mu.Lock()
	defer entry.mu.Unlock()

	now := c.clock.Now()
	if !entry.expiresAt.IsZero() && now.After(entry.expiresAt) {
		entry.tokens = 0
		return 0, false
	}

	return entry.tokens, true
}
