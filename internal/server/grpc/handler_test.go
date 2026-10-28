package grpc

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/distributed-rate-limiter/internal/core"
	ratelimitv1 "github.com/distributed-rate-limiter/proto/ratelimit/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type mockLimiter struct {
	allowFunc func(ctx context.Context, req core.Request) (core.Decision, error)
}

func (m *mockLimiter) Allow(ctx context.Context, req core.Request) (core.Decision, error) {
	if m.allowFunc != nil {
		return m.allowFunc(ctx, req)
	}
	return core.Decision{Allowed: true}, nil
}

func TestHandlerCheckAllowedLocal(t *testing.T) {
	var capturedReq core.Request
	mock := &mockLimiter{
		allowFunc: func(ctx context.Context, req core.Request) (core.Decision, error) {
			capturedReq = req
			return core.Decision{
				Allowed:    true,
				Remaining:  42,
				ResetAfter: 0,
				TierUsed:   core.TierLocal,
				Reason:     "OK_LOCAL_TIER",
			}, nil
		},
	}

	h := NewHandler(mock)
	resp, err := h.Check(context.Background(), &ratelimitv1.CheckRequest{
		Key:         "tenant:alpha",
		Cost:        2,
		Capacity:    100,
		RefillRate:  10.0,
		BatchSize:   20,
		Criticality: ratelimitv1.Criticality_CRITICALITY_STANDARD,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if capturedReq.Key != "tenant:alpha" || capturedReq.Cost != 2 || capturedReq.Capacity != 100 ||
		capturedReq.RefillRate != 10.0 || capturedReq.BatchSize != 20 || capturedReq.Criticality != core.CriticalityStandard {
		t.Fatalf("unexpected captured request: %+v", capturedReq)
	}

	if !resp.Allowed {
		t.Fatal("expected Allowed=true")
	}
	if resp.Remaining != 42 {
		t.Fatalf("expected Remaining=42, got %d", resp.Remaining)
	}
	if resp.ResetDurationMs != 0 {
		t.Fatalf("expected ResetDurationMs=0, got %d", resp.ResetDurationMs)
	}
	if resp.ServedBy != ratelimitv1.Tier_TIER_LOCAL {
		t.Fatalf("expected ServedBy=TIER_LOCAL, got %v", resp.ServedBy)
	}
	if resp.Reason != "OK_LOCAL_TIER" {
		t.Fatalf("expected Reason='OK_LOCAL_TIER', got %s", resp.Reason)
	}
}

func TestHandlerCheckAllowedRemote(t *testing.T) {
	mock := &mockLimiter{
		allowFunc: func(ctx context.Context, req core.Request) (core.Decision, error) {
			return core.Decision{
				Allowed:    true,
				Remaining:  100,
				ResetAfter: 0,
				TierUsed:   core.TierRemote,
				Reason:     "OK_REMOTE_TIER",
			}, nil
		},
	}

	h := NewHandler(mock)
	resp, err := h.Check(context.Background(), &ratelimitv1.CheckRequest{
		Key: "tenant:beta",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !resp.Allowed || resp.ServedBy != ratelimitv1.Tier_TIER_REDIS {
		t.Fatalf("expected allowed=true and Tier_TIER_REDIS, got allowed=%v tier=%v", resp.Allowed, resp.ServedBy)
	}
}

func TestHandlerCheckRateLimited(t *testing.T) {
	mock := &mockLimiter{
		allowFunc: func(ctx context.Context, req core.Request) (core.Decision, error) {
			return core.Decision{
				Allowed:    false,
				Remaining:  0,
				ResetAfter: 2500 * time.Millisecond,
				TierUsed:   core.TierRemote,
				Reason:     "QUOTA_EXCEEDED",
			}, nil
		},
	}

	h := NewHandler(mock)
	resp, err := h.Check(context.Background(), &ratelimitv1.CheckRequest{
		Key: "tenant:limited",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resp.Allowed {
		t.Fatal("expected Allowed=false")
	}
	if resp.Remaining != 0 {
		t.Fatalf("expected Remaining=0, got %d", resp.Remaining)
	}
	if resp.ResetDurationMs != 2500 {
		t.Fatalf("expected ResetDurationMs=2500, got %d", resp.ResetDurationMs)
	}
	if resp.Reason != "QUOTA_EXCEEDED" {
		t.Fatalf("expected Reason='QUOTA_EXCEEDED', got %s", resp.Reason)
	}
}

func TestHandlerCheckFallbackDegraded(t *testing.T) {
	mock := &mockLimiter{
		allowFunc: func(ctx context.Context, req core.Request) (core.Decision, error) {
			return core.Decision{
				Allowed:    true,
				Remaining:  0,
				ResetAfter: 0,
				TierUsed:   core.TierFallback,
				Reason:     "FAIL_OPEN_DEGRADED: CIRCUIT_BREAKER_OPEN",
			}, nil
		},
	}

	h := NewHandler(mock)
	resp, err := h.Check(context.Background(), &ratelimitv1.CheckRequest{
		Key: "tenant:fallback",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !resp.Allowed || resp.ServedBy != ratelimitv1.Tier_TIER_FALLBACK {
		t.Fatalf("expected Allowed=true and ServedBy=TIER_FALLBACK, got allowed=%v tier=%v", resp.Allowed, resp.ServedBy)
	}
}

func TestHandlerCheckCriticalityMapping(t *testing.T) {
	var capturedReq core.Request
	mock := &mockLimiter{
		allowFunc: func(ctx context.Context, req core.Request) (core.Decision, error) {
			capturedReq = req
			return core.Decision{Allowed: true}, nil
		},
	}

	h := NewHandler(mock)

	// Critical mapping
	_, _ = h.Check(context.Background(), &ratelimitv1.CheckRequest{
		Key:         "tenant:crit",
		Criticality: ratelimitv1.Criticality_CRITICALITY_CRITICAL,
	})
	if capturedReq.Criticality != core.CriticalityCritical {
		t.Fatalf("expected CriticalityCritical, got %s", capturedReq.Criticality)
	}

	// Standard mapping
	_, _ = h.Check(context.Background(), &ratelimitv1.CheckRequest{
		Key:         "tenant:std",
		Criticality: ratelimitv1.Criticality_CRITICALITY_STANDARD,
	})
	if capturedReq.Criticality != core.CriticalityStandard {
		t.Fatalf("expected CriticalityStandard, got %s", capturedReq.Criticality)
	}

	// Unspecified mapping (defaults to standard)
	_, _ = h.Check(context.Background(), &ratelimitv1.CheckRequest{
		Key:         "tenant:unspecified",
		Criticality: ratelimitv1.Criticality_CRITICALITY_UNSPECIFIED,
	})
	if capturedReq.Criticality != core.CriticalityStandard {
		t.Fatalf("expected CriticalityStandard for unspecified, got %s", capturedReq.Criticality)
	}
}

func TestHandlerCheckNilRequest(t *testing.T) {
	h := NewHandler(&mockLimiter{})
	resp, err := h.Check(context.Background(), nil)
	if resp != nil {
		t.Fatalf("expected nil response, got %+v", resp)
	}
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument error, got code=%s err=%v", status.Code(err), err)
	}
}

func TestHandlerCheckLimiterInternalError(t *testing.T) {
	mock := &mockLimiter{
		allowFunc: func(ctx context.Context, req core.Request) (core.Decision, error) {
			return core.Decision{}, errors.New("underlying failure")
		},
	}

	h := NewHandler(mock)
	resp, err := h.Check(context.Background(), &ratelimitv1.CheckRequest{Key: "fail"})
	if resp != nil {
		t.Fatalf("expected nil response, got %+v", resp)
	}
	if status.Code(err) != codes.Internal {
		t.Fatalf("expected Internal error, got code=%s err=%v", status.Code(err), err)
	}
}

func TestHandlerRegisterWithGRPCServer(t *testing.T) {
	server := grpc.NewServer()
	h := NewHandler(&mockLimiter{})
	ratelimitv1.RegisterRateLimiterServer(server, h)
}
