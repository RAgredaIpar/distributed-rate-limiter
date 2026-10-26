package http

import (
	"encoding/json"
	"net/http"

	"github.com/distributed-rate-limiter/internal/core"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// CheckRequestPayload represents the JSON request body for the /v1/check endpoint.
type CheckRequestPayload struct {
	Key         string  `json:"key"`
	Cost        int64   `json:"cost"`
	Capacity    int64   `json:"capacity"`
	RefillRate  float64 `json:"refillRate"`
	BatchSize   int64   `json:"batchSize"`
	Criticality string  `json:"criticality"`
}

// NewGatewayHandler initializes and returns an HTTP handler configured with
// health checks, Prometheus metrics, and the rate limiter verification endpoint.
func NewGatewayHandler(limiter core.Limiter) http.Handler {
	mux := http.NewServeMux()

	mux.Handle("/metrics", promhttp.Handler())

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	})

	mux.HandleFunc("/v1/check", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
			return
		}

		var payload CheckRequestPayload
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		crit := core.CriticalityStandard
		if payload.Criticality == string(core.CriticalityCritical) {
			crit = core.CriticalityCritical
		}

		decision, err := limiter.Allow(r.Context(), core.Request{
			Key:         payload.Key,
			Cost:        payload.Cost,
			Capacity:    payload.Capacity,
			RefillRate:  payload.RefillRate,
			BatchSize:   payload.BatchSize,
			Criticality: crit,
		})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		if !decision.Allowed {
			w.WriteHeader(http.StatusTooManyRequests)
		} else {
			w.WriteHeader(http.StatusOK)
		}

		_ = json.NewEncoder(w).Encode(decision)
	})

	return mux
}
