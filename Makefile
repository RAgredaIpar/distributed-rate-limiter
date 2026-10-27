.PHONY: all test test-race build clean bench

all: test build

test:
	go test ./... -v

test-race:
	go test ./... -v -race

bench:
	go test -bench=. -benchmem ./...

build:
	go build -o bin/ratelimiter cmd/server/main.go

clean:
	rm -rf bin/
