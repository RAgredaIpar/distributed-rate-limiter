package telemetry

import (
	"strconv"
	"time"

	"github.com/distributed-rate-limiter/internal/core"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// MetricsCollector manages Prometheus metrics instrumentation for rate limiter evaluations
// and remote circuit breaker states.
type MetricsCollector struct {
	requestsTotal *prometheus.CounterVec
	latency       *prometheus.HistogramVec
	breakerState  *prometheus.GaugeVec
}

// NewMetricsCollector creates a MetricsCollector registered with the default Prometheus registerer.
func NewMetricsCollector() *MetricsCollector {
	return NewMetricsCollectorWithRegistry(prometheus.DefaultRegisterer)
}

// NewMetricsCollectorWithRegistry creates a MetricsCollector registered with the supplied registerer,
// facilitating isolated metric registries in testing environments.
func NewMetricsCollectorWithRegistry(reg prometheus.Registerer) *MetricsCollector {
	factory := promauto.With(reg)

	return &MetricsCollector{
		requestsTotal: factory.NewCounterVec(
			prometheus.CounterOpts{
				Name: "ratelimit_requests_total",
				Help: "Total number of rate limiter decisions classified by tier and result",
			},
			[]string{"tier", "allowed"},
		),
		latency: factory.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "ratelimit_latency_seconds",
				Help:    "Latency of rate limiter evaluation in seconds",
				Buckets: []float64{0.0001, 0.0005, 0.001, 0.002, 0.005, 0.01, 0.05, 0.1},
			},
			[]string{"tier"},
		),
		breakerState: factory.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "ratelimit_circuit_breaker_state",
				Help: "Current state of the Redis circuit breaker (1 if active, 0 otherwise)",
			},
			[]string{"state"},
		),
	}
}

// ObserveDecision records decision count and execution latency for a rate-limiting evaluation.
func (m *MetricsCollector) ObserveDecision(tier core.Tier, allowed bool, duration time.Duration) {
	m.requestsTotal.WithLabelValues(string(tier), strconv.FormatBool(allowed)).Inc()
	m.latency.WithLabelValues(string(tier)).Observe(duration.Seconds())
}

// RecordCircuitState records the current active state of the circuit breaker.
// The active state is set to 1 while all other states are reset to 0.
func (m *MetricsCollector) RecordCircuitState(state core.BreakerState) {
	states := []core.BreakerState{core.StateClosed, core.StateOpen, core.StateHalfOpen}
	for _, s := range states {
		if s == state {
			m.breakerState.WithLabelValues(string(s)).Set(1)
		} else {
			m.breakerState.WithLabelValues(string(s)).Set(0)
		}
	}
}
