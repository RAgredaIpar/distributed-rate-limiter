package telemetry

import (
	"testing"
	"time"

	"github.com/distributed-rate-limiter/internal/core"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func TestMetricsCollection(t *testing.T) {
	reg := prometheus.NewRegistry()
	collector := NewMetricsCollectorWithRegistry(reg)

	collector.ObserveDecision(core.TierLocal, true, 200*time.Microsecond)
	collector.ObserveDecision(core.TierRemote, false, 2*time.Millisecond)
	collector.RecordCircuitState(core.StateOpen)

	metricFamilies, err := reg.Gather()
	if err != nil {
		t.Fatalf("failed to gather metrics: %v", err)
	}

	if len(metricFamilies) < 3 {
		t.Fatalf("expected at least 3 metric families registered, got %d", len(metricFamilies))
	}

	familyMap := make(map[string]*dto.MetricFamily)
	for _, mf := range metricFamilies {
		familyMap[mf.GetName()] = mf
	}

	// 1. Verify ratelimit_requests_total
	reqMF, ok := familyMap["ratelimit_requests_total"]
	if !ok {
		t.Fatalf("metric family ratelimit_requests_total not found")
	}
	if len(reqMF.GetMetric()) != 2 {
		t.Fatalf("expected 2 metric series for requests_total, got %d", len(reqMF.GetMetric()))
	}

	// 2. Verify ratelimit_latency_seconds
	latencyMF, ok := familyMap["ratelimit_latency_seconds"]
	if !ok {
		t.Fatalf("metric family ratelimit_latency_seconds not found")
	}
	if len(latencyMF.GetMetric()) != 2 {
		t.Fatalf("expected 2 metric series for latency_seconds, got %d", len(latencyMF.GetMetric()))
	}

	// 3. Verify ratelimit_circuit_breaker_state
	cbMF, ok := familyMap["ratelimit_circuit_breaker_state"]
	if !ok {
		t.Fatalf("metric family ratelimit_circuit_breaker_state not found")
	}
	if len(cbMF.GetMetric()) != 3 {
		t.Fatalf("expected 3 metric series for circuit breaker states (closed, open, half_open), got %d", len(cbMF.GetMetric()))
	}

	stateValues := make(map[string]float64)
	for _, m := range cbMF.GetMetric() {
		for _, label := range m.GetLabel() {
			if label.GetName() == "state" {
				stateValues[label.GetValue()] = m.GetGauge().GetValue()
			}
		}
	}

	if stateValues[string(core.StateOpen)] != 1.0 {
		t.Errorf("expected open state gauge to be 1.0, got %v", stateValues[string(core.StateOpen)])
	}
	if stateValues[string(core.StateClosed)] != 0.0 {
		t.Errorf("expected closed state gauge to be 0.0, got %v", stateValues[string(core.StateClosed)])
	}
	if stateValues[string(core.StateHalfOpen)] != 0.0 {
		t.Errorf("expected half_open state gauge to be 0.0, got %v", stateValues[string(core.StateHalfOpen)])
	}
}

func TestCircuitBreakerStateTransitions(t *testing.T) {
	reg := prometheus.NewRegistry()
	collector := NewMetricsCollectorWithRegistry(reg)

	// Set half-open
	collector.RecordCircuitState(core.StateHalfOpen)

	metricFamilies, err := reg.Gather()
	if err != nil {
		t.Fatalf("failed to gather metrics: %v", err)
	}

	var cbMF *dto.MetricFamily
	for _, mf := range metricFamilies {
		if mf.GetName() == "ratelimit_circuit_breaker_state" {
			cbMF = mf
			break
		}
	}

	if cbMF == nil {
		t.Fatalf("ratelimit_circuit_breaker_state metric not found")
	}

	stateValues := make(map[string]float64)
	for _, m := range cbMF.GetMetric() {
		for _, label := range m.GetLabel() {
			if label.GetName() == "state" {
				stateValues[label.GetValue()] = m.GetGauge().GetValue()
			}
		}
	}

	if stateValues[string(core.StateHalfOpen)] != 1.0 {
		t.Errorf("expected half_open state gauge to be 1.0, got %v", stateValues[string(core.StateHalfOpen)])
	}
	if stateValues[string(core.StateClosed)] != 0.0 {
		t.Errorf("expected closed state gauge to be 0.0, got %v", stateValues[string(core.StateClosed)])
	}
	if stateValues[string(core.StateOpen)] != 0.0 {
		t.Errorf("expected open state gauge to be 0.0, got %v", stateValues[string(core.StateOpen)])
	}
}

func TestDefaultConstructor(t *testing.T) {
	orig := prometheus.DefaultRegisterer
	prometheus.DefaultRegisterer = prometheus.NewRegistry()
	defer func() { prometheus.DefaultRegisterer = orig }()

	collector := NewMetricsCollector()
	if collector == nil {
		t.Fatalf("expected NewMetricsCollector to return non-nil instance")
	}
}
