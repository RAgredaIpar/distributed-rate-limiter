.PHONY: all test test-race build clean bench

all: test build

test:
	go test ./cmd/... ./internal/... -v

test-race:
	go test ./cmd/... ./internal/... -v -race

bench:
	go test -bench=. -benchmem ./cmd/... ./internal/...

build:
	go build -o bin/ratelimiter cmd/server/main.go

clean:
	rm -rf bin/
