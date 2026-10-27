# ADR 0002: Sharded Mutex Local Cache

## Status
Accepted

## Date
2026-10-26

## Context
Under high-throughput server workloads (tens to hundreds of thousands of requests per second), multiple goroutines concurrently evaluate rate limits across independent client IDs, IP addresses, and API keys.

A naive centralized in-memory cache implementation uses a single `sync.RWMutex` or `sync.Mutex` protecting a single `map[string]*localBucket`. Under heavy concurrent load across multi-core systems:
1. **Lock Contention:** All goroutines serialize around the global lock, causing scheduler thrashing and thread parking in the Go runtime (`runtime.futex` / `runtime.semacquire`).
2. **Cache-Line Bouncing:** Frequent concurrent writes to the shared synchronization primitive invalidate CPU L1/L2 cache lines across processor cores, degrading throughput.
3. **`sync.Map` Inadequacies:** While `sync.Map` handles read-heavy workloads with stable keys well, rate-limiting caches perform frequent write/mutation cycles (decrementing balances, updating timestamps, toggling refill flags), where `sync.Map` degrades due to internal mutex fallback and dirty-map promotion overhead.

## Decision
We implement a **64-Shard Mutex-Partitioned Cache** (`ShardedCache`):
- **Partitioning Strategy:** The local cache divides the key namespace into $N = 64$ independent shards.
- **Hash Distribution:** Each string key is mapped to a shard index using Fowler-Noll-Vo 1a (FNV-1a) 64-bit hashing:
  $$\text{shardIndex} = \text{FNV-1a}(key) \pmod{64}$$
  Because 64 is a power of two, this index calculation executes with a bitwise mask `hash & 63`.
- **Independent Shard Mutexes:** Each shard encapsulates its own dedicated `sync.Mutex` and `map[string]*localBucket`.
- **Granular Locking:** Operations on different keys that hash to different shards execute concurrently in parallel without lock contention or thread serialization.

## Consequences

### Positive
- **Lock Contention Mitigation:** Statistical lock contention is reduced by a factor of 64 under uniform key distributions.
- **Multi-Core Scaling:** Independent goroutines across distinct CPU cores perform local batch deductions concurrently without cross-core synchronization overhead.
- **Predictable Constant-Time Overhead:** FNV-1a computes in $O(L)$ time proportional to key length with zero heap allocations, ensuring sub-microsecond local evaluations.
- **Zero External Dependencies:** Implemented purely in standard Go without third-party CGo or unmanaged memory dependencies.

### Negative / Trade-offs
- **Intra-Shard Collisions:** Keys that hash to the identical shard share a mutex. While hash collisions occur occasionally, 64 shards distribute uniform workloads effectively.
- **Fixed Shard Topology:** The shard count is determined at construction (defaulting to 64) and is fixed for the cache lifecycle.
