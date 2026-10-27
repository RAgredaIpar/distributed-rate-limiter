# ADR 0001: Two-Tier Token Batching Architecture

## Status
Accepted

## Date
2026-10-26

## Context
In modern distributed microservice architectures, rate limiting is essential to protect downstream systems, databases, and upstream third-party APIs from overload and denial-of-service conditions. Standard approaches face a trade-off:

1. **Purely Local Limiters:** Local in-memory token buckets (e.g., standard ring buffers or per-process rate limiters) provide ultra-low latency (< 1 microsecond) but lack global state synchronization. When incoming traffic is distributed across $N$ stateless replicas behind a round-robin or least-connections load balancer, a client can consume up to $N \times \text{limit}$ requests before any local limiter triggers, violating system-wide service level agreements (SLAs).
2. **Naive Centralized Limiters:** Direct remote querying (e.g., evaluating a Redis key or Lua script on every inbound HTTP/gRPC request) guarantees global quota enforcement but introduces 1–5 ms network round-trip time (RTT), TCP socket serialization overhead, and connection pool contention into the critical path of every request. Under peak loads of 100,000+ RPS, Redis becomes an operational bottleneck and single point of failure (SPOF).

## Decision
We adopt a **Two-Tier Token Batching Architecture**:
- **Tier 1 (Local In-Memory Cache):** Every application instance maintains an in-memory token bucket cache. Requests consume locally held tokens with sub-microsecond latency and zero remote network I/O.
- **Tier 2 (Distributed Centralized Store):** A centralized Redis instance maintains global token accounting using atomic Lua scripts.
- **Asynchronous Batch Lease Acquisition:** Instead of decrementing tokens one by one remotely, local instances acquire token leases in configurable batches ($S_{\text{batch}}$, default: 20 tokens or up to 20% of capacity).
- **Proactive Low-Watermark Prefetching:** When local remaining tokens drop below a low-watermark threshold ($\le 20\%$ of batch size), a single background goroutine asynchronously requests the next batch lease from Redis while foreground requests continue to be served without blocking.
- **Thundering Herd Suppression:** A per-key atomic synchronization flag (`refilling`) guarantees that only one background refill request is dispatched per key at any given time.

## Consequences

### Positive
- **Dramatic I/O Reduction:** Eliminates 95%+ of remote Redis network round-trips for high-volume endpoints, transforming linear remote load into batched sub-linear load.
- **Sub-Microsecond Latency:** Cached batch hits resolve in pure local RAM (p99 < 50 µs), isolating upstream callers from remote network jitter.
- **Horizontal Scalability:** Increases throughput by orders of magnitude per Redis cluster node, supporting higher cluster concurrency without connection starvation.
- **Controlled Contention:** Asynchronous background replenishment decouples downstream network latency from API client response time.

### Negative / Trade-offs
- **Lease Under-allocation on Crash:** If an application instance abruptly terminates or crashes, any unconsumed tokens remaining in its local lease are temporarily stranded until the remote lease TTL expires (default: 60 seconds).
- **Graceful Burst Relaxation:** Rapid distributed spikes across multiple nodes may temporarily consume multiple concurrent batches before global depletion is acknowledged, slightly relaxing strict global limits within the bounded batch size window.
