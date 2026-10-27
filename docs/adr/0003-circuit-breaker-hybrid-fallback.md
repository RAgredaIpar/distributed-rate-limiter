# ADR 0003: Circuit Breaker with Hybrid Fallback Policy

## Status
Accepted

## Date
2026-10-26

## Context
In distributed deployments, dependencies such as Redis are subject to transient network partitions, master failovers, CPU saturation, and connection pool exhaustion. 

When Tier 2 remote operations fail or time out, rate limiters face a fundamental availability vs. consistency dilemma:
1. **Universal Fail-Closed:** Rejecting all inbound requests when Redis is unavailable causes cascading service outages. A transient Redis failure turns an isolated rate-limiter incident into a complete system-wide denial of service for end users.
2. **Universal Fail-Open:** Permitting all inbound requests without restriction risks overwhelming downstream databases, payment processors, and resource-heavy APIs, risking database crashes, financial overages, and security exploitation.

## Decision
We implement a **3-State Circuit Breaker with a Hybrid Fallback Policy**:
- **Circuit Breaker State Machine:**
  - **Closed:** Normal operation. Remote requests flow to Redis. If consecutive failures reach `FailureThreshold` (default: 5), the breaker trips to **Open**.
  - **Open:** Redis calls are immediately short-circuited without network overhead. Inbound requests are redirected to the fallback evaluator for `CooldownDuration` (default: 5 seconds).
  - **Half-Open:** Following cooldown expiration, the breaker allows a single canary request to Redis. If the canary succeeds, the breaker resets to **Closed** and clears failure counters. If the canary fails, the breaker returns to **Open** for another cooldown period.
- **Hybrid Fallback Evaluation:**
  - **Standard Traffic (`CriticalityStandard`):** Evaluated as **Fail-Open**. Requests are permitted (`Allowed: true`, `TierUsed: TierFallback`, `Reason: "FAIL_OPEN_DEGRADED: ..."`) to prioritize service availability for typical user-facing features (read operations, browse flows, marketing landing pages).
  - **Critical Traffic (`CriticalityCritical`):** Evaluated as **Fail-Closed**. Requests are rejected (`Allowed: false`, `ResetAfter: 5s`, `TierUsed: TierFallback`, `Reason: "FAIL_CLOSED_CRITICAL_BLOCKED: ..."`) to strictly protect billing endpoints, credit card checkouts, authentication attempts, and expensive AI generation pipelines.

## Consequences

### Positive
- **Fault Isolation & Blast Radius Containment:** Redis degradation or downtime does not cascade to the entire ingress gateway.
- **Self-Healing:** Automatic transition from Open to Half-Open and Closed restores full two-tier operation without requiring operator intervention.
- **Differentiated SLA Enforcement:** Teams can designate specific sensitive endpoints as critical, guaranteeing security and financial safety while maximizing availability for general consumers.
- **Reduced Upstream Pressure:** Fast-failing in the Open state avoids stacking pending TCP connections against an already struggling Redis instance.

### Negative / Trade-offs
- **Potential Burst Overrun for Standard Endpoints:** During an extended Redis outage, standard traffic operates in degraded fail-open mode, which may allow higher request rates than the configured nominal quota.
- **Temporary Critical Request Shedding:** Critical endpoints will shed load while Redis recovers, prioritizing system protection over short-term availability.
