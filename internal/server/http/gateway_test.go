package http

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/distributed-rate-limiter/internal/circuitbreaker"
	"github.com/distributed-rate-limiter/internal/core"
	"github.com/distributed-rate-limiter/internal/limiter"
)

type dummyStore struct{}

func (d *dummyStore) AcquireLease(ctx context.Context, key string, requestedBatch int64, capacity int64, refillRate float64, ttl time.Duration) (core.LeaseResult, error) {
	return core.LeaseResult{GrantedTokens: requestedBatch, Remaining: 100}, nil
}

func (d *dummyStore) Ping(ctx context.Context) error {
	return nil
}

type mockLimiter struct {
	decision core.Decision
	err      error
	lastReq  core.Request
}

func (m *mockLimiter) Allow(ctx context.Context, req core.Request) (core.Decision, error) {
	m.lastReq = req
	return m.decision, m.err
}

func TestHTTPHealthAndCheckEndpoints(t *testing.T) {
	clock := core.NewSystemClock()
	cache := limiter.NewShardedCache(16, clock)
	breaker := circuitbreaker.NewCircuitBreaker(circuitbreaker.BreakerConfig{}, clock)
	engine := limiter.NewEngine(cache, &dummyStore{}, breaker, clock)

	handler := NewGatewayHandler(engine)

	// 1. Health check
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 OK from /healthz, got %d", rr.Code)
	}
	if body := rr.Body.String(); body != "OK" {
		t.Fatalf("expected body 'OK', got %q", body)
	}

	// 2. Metrics endpoint
	metricsReq := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	metricsRR := httptest.NewRecorder()
	handler.ServeHTTP(metricsRR, metricsReq)
	if metricsRR.Code != http.StatusOK {
		t.Fatalf("expected 200 OK from /metrics, got %d", metricsRR.Code)
	}
	if !strings.Contains(metricsRR.Body.String(), "# HELP") {
		t.Fatalf("expected prometheus metrics format from /metrics")
	}

	// 3. Check endpoint (Allowed)
	checkBody, _ := json.Marshal(map[string]interface{}{
		"key":        "client:web",
		"cost":       1,
		"capacity":   50,
		"refillRate": 5.0,
		"batchSize":  10,
	})
	req2 := httptest.NewRequest(http.MethodPost, "/v1/check", bytes.NewBuffer(checkBody))
	rr2 := httptest.NewRecorder()
	handler.ServeHTTP(rr2, req2)
	if rr2.Code != http.StatusOK {
		t.Fatalf("expected 200 OK from /v1/check, got %d", rr2.Code)
	}

	var resp core.Decision
	if err := json.NewDecoder(rr2.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode check response: %v", err)
	}
	if !resp.Allowed {
		t.Fatalf("expected Allowed=true in decision response")
	}
}

func TestHTTPCheckRateLimited(t *testing.T) {
	mock := &mockLimiter{
		decision: core.Decision{
			Allowed:   false,
			Remaining: 0,
			TierUsed:  core.TierLocal,
			Reason:    "rate limit exceeded",
		},
	}

	handler := NewGatewayHandler(mock)

	payload, _ := json.Marshal(CheckRequestPayload{
		Key:      "user:limited",
		Cost:     5,
		Capacity: 10,
	})

	req := httptest.NewRequest(http.MethodPost, "/v1/check", bytes.NewReader(payload))
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429 Too Many Requests, got %d", rr.Code)
	}

	var resp core.Decision
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode check response: %v", err)
	}
	if resp.Allowed {
		t.Fatalf("expected Allowed=false in 429 response")
	}
}

func TestHTTPCheckInvalidMethod(t *testing.T) {
	mock := &mockLimiter{}
	handler := NewGatewayHandler(mock)

	req := httptest.NewRequest(http.MethodGet, "/v1/check", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405 Method Not Allowed, got %d", rr.Code)
	}
}

func TestHTTPCheckInvalidJSON(t *testing.T) {
	mock := &mockLimiter{}
	handler := NewGatewayHandler(mock)

	req := httptest.NewRequest(http.MethodPost, "/v1/check", strings.NewReader("{invalid-json"))
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 Bad Request, got %d", rr.Code)
	}
}

func TestHTTPCheckLimiterInternalError(t *testing.T) {
	mock := &mockLimiter{
		err: errors.New("underlying store failure"),
	}
	handler := NewGatewayHandler(mock)

	payload, _ := json.Marshal(CheckRequestPayload{
		Key: "user:err",
	})
	req := httptest.NewRequest(http.MethodPost, "/v1/check", bytes.NewReader(payload))
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500 Internal Server Error, got %d", rr.Code)
	}
	body, _ := io.ReadAll(rr.Body)
	if !strings.Contains(string(body), "underlying store failure") {
		t.Fatalf("expected error message in body, got %q", string(body))
	}
}

func TestHTTPCheckCriticalityParsing(t *testing.T) {
	mock := &mockLimiter{
		decision: core.Decision{Allowed: true},
	}
	handler := NewGatewayHandler(mock)

	payload, _ := json.Marshal(map[string]interface{}{
		"key":         "srv:payment",
		"cost":        2,
		"capacity":    100,
		"refillRate":  10.0,
		"batchSize":   20,
		"criticality": "critical",
	})
	req := httptest.NewRequest(http.MethodPost, "/v1/check", bytes.NewReader(payload))
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rr.Code)
	}
	if mock.lastReq.Criticality != core.CriticalityCritical {
		t.Fatalf("expected CriticalityCritical, got %v", mock.lastReq.Criticality)
	}
}
