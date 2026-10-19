# System Architecture Specification: Two-Tier Distributed Rate Limiter

## 1. Overview & Problem Statement

Modern microservice architectures exposed to large-scale, bursty, or adversarial client traffic require robust rate-limiting mechanisms to preserve downstream database and service availability. 

Conventional single-tier approaches present a critical engineering dilemma:
1. **Local-only rate limiters** execute with sub-microsecond in-memory latency but lack global consistency across distributed replicas, allowing distributed clients behind round-robin load balancers to breach collective quotas.
2. **Centralized remote rate limiters (e.g., direct Redis queries per request)** enforce strict global quotas but introduce network serialization and round-trip latencies (1–5 ms) into the critical path of every incoming request, turning the shared data store into a performance bottleneck and single point of failure (SPOF).

This system implements a **Production-Grade Two-Tier Distributed Rate Limiter** in Go, combining local in-memory token batching with atomic Redis synchronization, resilient circuit breaking, and granular Prometheus observability.

---

## 2. High-Level Architecture

```
                          [ Client / Ingress Gateway ]
                                       │
                                       ▼ (gRPC HTTP/2 / REST HTTP)
                     ┌───────────────────────────────────┐
                     │     Ingress Protocol Adapters     │
                     │   (gRPC Server / HTTP Gateway)    │
                     └─────────────────┬─────────────────┘
                                       │
                                       ▼
                     ┌───────────────────────────────────┐
                     │        Limiter Engine Core        │
                     │                                   │
                     │   ┌───────────────────────────┐   │
                     │   │ Tier 1: Local In-Memory   │   │
                     │   │ Sharded Cache (RAM)       │   │
                     │   │ (64 Mutex-Sharded Buckets)│   │
                     │   └─────────────┬─────────────┘   │
                     │                 │ Low watermark / │
                     │                 │ Cache miss      │
                     │                 ▼                 │
                     │   ┌───────────────────────────┐   │
                     │   │      Circuit Breaker      │   │
                     │   │ (Closed / Open / HalfOpen)│   │
                     │   └─────────────┬─────────────┘   │
                     └─────────────────┼─────────────────┘
                                       │
                         Tripped (Open)│ Healthy (Closed)
                     ┌─────────────────┴─────────────────┐
                     ▼                                   ▼
        ┌─────────────────────────┐         ┌─────────────────────────┐
        │    Fallback Evaluator   │         │  Tier 2: Remote Engine  │
        │ - Standard: Fail-Open   │         │ (Redis Cluster/Master)  │
        │   (Degraded Local Cap)  │         │  Atomic Lua Evaluation  │
        │ - Critical: Fail-Closed │         └─────────────────────────┘
        └─────────────────────────┘                      │
                     │                                   │
                     └─────────────────┬─────────────────┘
                                       ▼
                     ┌───────────────────────────────────┐
                     │      Prometheus Telemetry         │
                     │   (p50/p95/p99 Latency, Errors,   │
                     │    Decisions, Breaker States)     │
                     └───────────────────────────────────┘
```

---

## 3. Mathematical Model: Token Bucket & Lazy Refill

The rate limiting algorithm follows the **Token Bucket** model with continuous lazy evaluation to avoid background tick threads per key.

Given:
* $B$: Maximum bucket capacity (maximum burst size).
* $r$: Continuous sustained refill rate (tokens per second).
* $T_p$: Stored token balance recorded at timestamp $t_p$.
* $t_n$: Current request evaluation timestamp.
* $\Delta t = \max(0, \, t_n - t_p)$: Elapsed duration since the last token update.

The instantaneously available token balance $T_a$ is computed as:
$$T_a = \min\left(B, \, T_p + \Delta t \times r\right)$$

### Batch Lease Pre-allocation
When a local node requires tokens from Tier 2, it requests a lease batch $S_{\text{batch}}$ (e.g., $\min(B \times 0.2, 50)$). 
If $T_a \ge S_{\text{batch}}$, the remote tier deducts $S_{\text{batch}}$ atomically, updates $t_p = t_n$, and returns the granted batch to the local node.

---

## 4. Component Design

### 4.1. Tier 1: Local In-Memory Sharded Engine
* **Contention Mitigation:** To eliminate global lock contention across thousands of concurrent goroutines, the local cache is partitioned into 64 shards using the FNV-1a non-cryptographic hashing algorithm:
  $$\text{shard} = \text{fnv1a}(\text{key}) \pmod{64}$$
* **Local Bucket Structure:**
  Each shard manages buckets with:
  * `tokens`: Current balance of locally granted tokens.
  * `capacity`: Local burst threshold.
  * `expiresAt`: Leased expiration time for the batch.
  * `refilling`: Atomic boolean flag ensuring only one goroutine triggers an asynchronous remote batch fetch when tokens fall below the low-watermark threshold ($< 20\%$ of batch), preventing the *thundering herd* problem.

### 4.2. Tier 2: Remote Redis Engine with Atomic Lua Script
* **Atomic Execution:** Synchronization is governed by an embedded Lua script executing atomically inside Redis, preventing race conditions between distributed nodes.
* **Storage Schema:** Redis Hash under `ratelimit:{key}`:
  * Field `tokens`: IEEE 754 float / integer token count.
  * Field `last_updated`: Unix timestamp in microseconds.
* **Auto-Purge (TTL):** Every write resets an expiration TTL:
  $$\text{TTL} = \left\lceil \frac{B}{r} \right\rceil + 60 \text{ seconds}$$
  ensuring stale or inactive tenant records are reclaimed automatically by Redis.

### 4.3. Fault Tolerance: Resilient Circuit Breaker
* **Finite State Machine:**
  * **Closed (Normal):** Remote calls proceed. Consecutive failures increment counter. If failures $\ge$ `FailureThreshold` (default: 5), transition to **Open**.
  * **Open (Isolated):** Remote calls are bypassed for `CooldownPeriod` (default: 5s). Traffic is routed to the Fallback Evaluator.
  * **Half-Open (Canary):** Allows a single trial request to Redis. On success, transitions to **Closed**; on failure, re-opens for another `CooldownPeriod`.
* **Hybrid Fallback Policy:**
  * **Standard Traffic (`CriticalityStandard`):** Fail-open mode. Requests are permitted but constrained by a local emergency in-memory limiter to avoid CPU exhaustion.
  * **Critical Traffic (`CriticalityCritical`):** Fail-closed mode. Requests are rejected with `CIRCUIT_OPEN_CRITICAL_BLOCKED` to protect financial transactions, billing APIs, or costly AI pipelines.

### 4.4. Telemetry & Observability
* **Prometheus Metrics:**
  * `ratelimit_requests_total{tenant, status="allowed|rejected", tier="local|remote|fallback"}`: Counter of all decisions.
  * `ratelimit_latency_seconds`: Histogram with exponential buckets to monitor $p50$, $p95$, and $p99$ latencies.
  * `ratelimit_circuit_breaker_state{state="closed|open|half_open"}`: Gauge reflecting health state.
  * `ratelimit_redis_errors_total{error_type}`: Counter capturing network timeouts, connection pool exhaustion, and Lua errors.

### 4.5. Network Interfaces
* **gRPC Service (`ratelimit.v1.RateLimiter`):**
  * High-performance unary RPC `Check(CheckRequest) returns (CheckResponse)` over HTTP/2.
* **HTTP Diagnostics & Gateway:**
  * `GET /metrics`: Prometheus scraper endpoint.
  * `GET /healthz`: Liveness and readiness probe for container orchestrators.
  * `POST /v1/check`: JSON REST proxy endpoint for easy CLI verification.

---

## 5. Domain Interfaces & Data Structures

```go
package core

import (
    "context"
    "time"
)

type Tier string

const (
    TierLocal    Tier = "local"
    TierRemote   Tier = "remote"
    TierFallback Tier = "fallback"
)

type Criticality string

const (
    CriticalityStandard Criticality = "standard"
    CriticalityCritical Criticality = "critical"
)

type Request struct {
    Key         string
    Cost        int64
    Capacity    int64
    RefillRate  float64
    BatchSize   int64
    Criticality Criticality
}

type Decision struct {
    Allowed    bool
    Remaining  int64
    ResetAfter time.Duration
    TierUsed   Tier
    Reason     string
}

type Limiter interface {
    Allow(ctx context.Context, req Request) (Decision, error)
}

type LeaseResult struct {
    GrantedTokens int64
    Remaining     int64
    ResetAfter    time.Duration
}

type RemoteStore interface {
    AcquireLease(ctx context.Context, key string, requestedBatch int64, capacity int64, refillRate float64, ttl time.Duration) (LeaseResult, error)
    Ping(ctx context.Context) error
}

type BreakerState string

const (
    StateClosed   BreakerState = "closed"
    StateOpen     BreakerState = "open"
    StateHalfOpen BreakerState = "half_open"
)

type CircuitBreaker interface {
    Allow() bool
    RecordSuccess()
    RecordFailure()
    State() BreakerState
}
```

---

## 6. Verification and Testing Strategy

Following strict **Test-Driven Development (TDD)** principles:
1. **Unit Testing:**
   * Pure deterministic unit tests for the token bucket arithmetic with synthetic clocks (`Clock` interface).
   * Sharded local cache concurrency tests using Go race detector (`go test -race`).
   * Circuit breaker state transitions verified without external dependencies.
2. **Integration Testing:**
   * Redis remote store validation running against a real Redis instance (via Docker or local Redis).
   * Script Lua atomic tests verifying concurrent acquisitions without over-allocation.
3. **End-to-End & Benchmarks:**
   * gRPC client benchmark measuring allocations per operation (`allocs/op`) and $p99$ response times under high concurrency.
