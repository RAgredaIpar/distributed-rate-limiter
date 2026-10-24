package redis

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/distributed-rate-limiter/internal/core"
	goredis "github.com/redis/go-redis/v9"
)

var _ core.RemoteStore = (*Store)(nil)

func TestRedisStoreAcquireLease(t *testing.T) {
	mr := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	store := NewRedisStore(client)

	ctx := context.Background()
	// Acquire batch of 10 from capacity of 20 with refill rate 2.0
	res, err := store.AcquireLease(ctx, "tenant:redis_test", 10, 20, 2.0, time.Minute)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if res.GrantedTokens != 10 {
		t.Fatalf("expected 10 granted tokens, got %d", res.GrantedTokens)
	}

	if res.Remaining != 10 {
		t.Fatalf("expected 10 remaining tokens, got %d", res.Remaining)
	}

	// Acquire another batch of 10 -> remaining should reach 0
	res2, err := store.AcquireLease(ctx, "tenant:redis_test", 10, 20, 2.0, time.Minute)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res2.GrantedTokens != 10 || res2.Remaining != 0 {
		t.Fatalf("expected 10 granted, 0 remaining; got granted=%d remaining=%d", res2.GrantedTokens, res2.Remaining)
	}

	// 3rd batch should return 0 granted tokens
	res3, err := store.AcquireLease(ctx, "tenant:redis_test", 5, 20, 2.0, time.Minute)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res3.GrantedTokens != 0 {
		t.Fatalf("expected 0 granted tokens when exhausted, got %d", res3.GrantedTokens)
	}
}

func TestRedisStorePartialGrant(t *testing.T) {
	mr := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	store := NewRedisStore(client)

	ctx := context.Background()
	// Capacity is 15. First acquire 10 tokens.
	res1, err := store.AcquireLease(ctx, "tenant:partial", 10, 15, 0.0, time.Minute)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res1.GrantedTokens != 10 || res1.Remaining != 5 {
		t.Fatalf("expected 10 granted and 5 remaining; got granted=%d remaining=%d", res1.GrantedTokens, res1.Remaining)
	}

	// Now request 10 tokens, but only 5 remain.
	res2, err := store.AcquireLease(ctx, "tenant:partial", 10, 15, 0.0, time.Minute)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res2.GrantedTokens != 5 {
		t.Fatalf("expected 5 partial granted tokens, got %d", res2.GrantedTokens)
	}
	if res2.Remaining != 0 {
		t.Fatalf("expected 0 remaining tokens, got %d", res2.Remaining)
	}
}

func TestRedisStoreRefillOverTime(t *testing.T) {
	mr := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	store := NewRedisStore(client)

	ctx := context.Background()
	// Capacity 10, drain all 10 tokens with refill rate 100 tokens/sec
	res1, err := store.AcquireLease(ctx, "tenant:refill", 10, 10, 100.0, time.Minute)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res1.GrantedTokens != 10 || res1.Remaining != 0 {
		t.Fatalf("expected 10 granted, 0 remaining; got granted=%d remaining=%d", res1.GrantedTokens, res1.Remaining)
	}

	// Sleep 60ms -> should refill ~6 tokens (100 tokens/sec * 0.06s = 6 tokens)
	time.Sleep(60 * time.Millisecond)

	res2, err := store.AcquireLease(ctx, "tenant:refill", 5, 10, 100.0, time.Minute)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res2.GrantedTokens < 4 {
		t.Fatalf("expected at least 4 tokens refilled, got %d", res2.GrantedTokens)
	}
}

func TestRedisStoreTTL(t *testing.T) {
	mr := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	store := NewRedisStore(client)

	ctx := context.Background()
	// Test custom TTL
	_, err := store.AcquireLease(ctx, "tenant:custom_ttl", 5, 10, 1.0, 120*time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ttl := mr.TTL("ratelimit:tenant:custom_ttl")
	if ttl <= 0 || ttl > 120*time.Second {
		t.Fatalf("expected TTL ~120s, got %v", ttl)
	}

	// Test default fallback TTL (ttl <= 0)
	_, err = store.AcquireLease(ctx, "tenant:default_ttl", 5, 10, 1.0, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defaultTTL := mr.TTL("ratelimit:tenant:default_ttl")
	if defaultTTL <= 0 || defaultTTL > 3600*time.Second {
		t.Fatalf("expected default TTL ~3600s, got %v", defaultTTL)
	}
}

func TestRedisStoreResetAfter(t *testing.T) {
	mr := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	store := NewRedisStore(client)

	ctx := context.Background()
	// Drain bucket completely: capacity 10, refillRate 2.0 (2 tokens/sec)
	_, err := store.AcquireLease(ctx, "tenant:reset", 10, 10, 2.0, time.Minute)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Request 6 tokens -> needed = 6, refillRate = 2.0 -> reset_after = ceil(6 / 2) = 3s
	res, err := store.AcquireLease(ctx, "tenant:reset", 6, 10, 2.0, time.Minute)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.GrantedTokens != 0 {
		t.Fatalf("expected 0 granted tokens, got %d", res.GrantedTokens)
	}
	if res.ResetAfter < 2*time.Second || res.ResetAfter > 4*time.Second {
		t.Fatalf("expected ResetAfter ~3s, got %v", res.ResetAfter)
	}
}

func TestRedisStorePing(t *testing.T) {
	mr := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{
		Addr:       mr.Addr(),
		MaxRetries: -1,
	})
	store := NewRedisStore(client)

	ctx := context.Background()
	if err := store.Ping(ctx); err != nil {
		t.Fatalf("expected successful ping, got: %v", err)
	}

	// Close miniredis to trigger failure
	mr.Close()
	if err := store.Ping(ctx); err == nil {
		t.Fatal("expected error on closed redis connection, got nil")
	}
}

func TestRedisStoreContextCanceled(t *testing.T) {
	mr := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	store := NewRedisStore(client)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	_, err := store.AcquireLease(ctx, "tenant:canceled", 5, 10, 1.0, time.Minute)
	if err == nil {
		t.Fatal("expected error for canceled context, got nil")
	}
}

func TestToInt64(t *testing.T) {
	tests := []struct {
		name    string
		input   interface{}
		want    int64
		wantErr bool
	}{
		{"int64", int64(42), 42, false},
		{"int", int(42), 42, false},
		{"int32", int32(42), 42, false},
		{"float64", float64(42.0), 42, false},
		{"float32", float32(42.0), 42, false},
		{"invalid string", "42", 0, true},
		{"invalid nil", nil, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := toInt64(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("toInt64(%v) error = %v, wantErr %v", tt.input, err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("toInt64(%v) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestRedisStoreMalformedLuaResponses(t *testing.T) {
	mr := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	ctx := context.Background()

	// 1. Script returning a non-array
	store1 := NewRedisStore(client)
	store1.script = goredis.NewScript("return 'string-value'")
	if _, err := store1.AcquireLease(ctx, "bad1", 1, 10, 1, time.Minute); err == nil {
		t.Fatal("expected error on non-array lua response")
	}

	// 2. Script returning array with fewer than 3 elements
	store2 := NewRedisStore(client)
	store2.script = goredis.NewScript("return { 1, 2 }")
	if _, err := store2.AcquireLease(ctx, "bad2", 1, 10, 1, time.Minute); err == nil {
		t.Fatal("expected error on short array lua response")
	}

	// 3. Script returning non-numeric values
	store3 := NewRedisStore(client)
	store3.script = goredis.NewScript("return { 'invalid', 2, 3 }")
	if _, err := store3.AcquireLease(ctx, "bad3", 1, 10, 1, time.Minute); err == nil {
		t.Fatal("expected error on non-numeric granted value")
	}

	store4 := NewRedisStore(client)
	store4.script = goredis.NewScript("return { 1, 'invalid', 3 }")
	if _, err := store4.AcquireLease(ctx, "bad4", 1, 10, 1, time.Minute); err == nil {
		t.Fatal("expected error on non-numeric remaining value")
	}

	store5 := NewRedisStore(client)
	store5.script = goredis.NewScript("return { 1, 2, 'invalid' }")
	if _, err := store5.AcquireLease(ctx, "bad5", 1, 10, 1, time.Minute); err == nil {
		t.Fatal("expected error on non-numeric reset_after value")
	}
}
