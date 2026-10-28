package grpc

import (
	"context"

	"github.com/distributed-rate-limiter/internal/core"
	ratelimitv1 "github.com/distributed-rate-limiter/proto/ratelimit/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var _ ratelimitv1.RateLimiterServer = (*Handler)(nil)

// Handler implements the protobuf RateLimiterServer interface.
type Handler struct {
	ratelimitv1.UnimplementedRateLimiterServer
	limiter core.Limiter
}

// NewHandler instantiates a gRPC handler that delegates rate evaluation to a core.Limiter.
func NewHandler(limiter core.Limiter) *Handler {
	return &Handler{
		limiter: limiter,
	}
}

// Check evaluates whether a request should be permitted or rate limited.
func (h *Handler) Check(ctx context.Context, in *ratelimitv1.CheckRequest) (*ratelimitv1.CheckResponse, error) {
	if in == nil {
		return nil, status.Error(codes.InvalidArgument, "request cannot be nil")
	}

	crit := core.CriticalityStandard
	if in.Criticality == ratelimitv1.Criticality_CRITICALITY_CRITICAL {
		crit = core.CriticalityCritical
	}

	req := core.Request{
		Key:         in.Key,
		Cost:        in.Cost,
		Capacity:    in.Capacity,
		RefillRate:  in.RefillRate,
		BatchSize:   in.BatchSize,
		Criticality: crit,
	}

	decision, err := h.limiter.Allow(ctx, req)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "rate limiting evaluation failed: %v", err)
	}

	return &ratelimitv1.CheckResponse{
		Allowed:         decision.Allowed,
		Remaining:       decision.Remaining,
		ResetDurationMs: decision.ResetAfter.Milliseconds(),
		ServedBy:        mapTierToProto(decision.TierUsed),
		Reason:          decision.Reason,
	}, nil
}

func mapTierToProto(tier core.Tier) ratelimitv1.Tier {
	switch tier {
	case core.TierLocal:
		return ratelimitv1.Tier_TIER_LOCAL
	case core.TierRemote:
		return ratelimitv1.Tier_TIER_REDIS
	case core.TierFallback:
		return ratelimitv1.Tier_TIER_FALLBACK
	default:
		return ratelimitv1.Tier_TIER_UNSPECIFIED
	}
}
