 .PHONY: build test cover bench proto clean lint all provision share fuse install build-wourifs

GO ?= go
PROTOC ?= protoc
MODULE = github.com/MiltonJ23/WouriFS

# Build all binaries (Sprint 1 + Sprint 2 + unified CLI)
build: build-s1 build-s2 build-wourifs

# Unified CLI (wourifs init|serve|provision|mount|status|audit|share|bench)
build-wourifs:
	$(GO) build -o bin/wourifs ./cmd/wourifs

# Install the unified CLI on PATH (replaces any stale copy)
install: build-wourifs
	rm -f /usr/local/bin/wourifs
	cp bin/wourifs /usr/local/bin/wourifs
	chmod 755 /usr/local/bin/wourifs

build-s1:
	$(GO) build -o bin/namenode ./cmd/namenode
	$(GO) build -o bin/datanode ./cmd/datanode

build-s2:
	$(GO) build -o bin/wouri-provision ./cmd/provision
	$(GO) build -o bin/wouri-fuse ./cmd/fuse

# Run all tests
test:
	$(GO) test ./... -count=1 -timeout 120s

# Run tests with race detector
test-race:
	$(GO) test ./... -race -count=1 -timeout 120s

# Coverage report
cover:
	$(GO) test ./internal/... -coverprofile=coverage.out -count=1 -timeout 120s
	$(GO) tool cover -func=coverage.out | grep -E "^(github\.com/MiltonJ23|total:)"
	$(GO) tool cover -html=coverage.out -o coverage.html

# Run latency benchmarks
bench: build-wourifs
	$(GO) test ./... -bench=. -benchmem -count=3 -timeout 120s
	./bin/wourifs bench

# Generate protobuf code
proto:
	$(PROTOC) --go_out=. --go_opt=module=$(MODULE) \
		--go-grpc_out=. --go-grpc_opt=module=$(MODULE) \
		api/proto/v1/namenode.proto
	$(PROTOC) --go_out=. --go_opt=module=$(MODULE) \
		--go-grpc_out=. --go-grpc_opt=module=$(MODULE) \
		api/proto/v1/datanode.proto
	$(PROTOC) --go_out=. --go_opt=module=$(MODULE) \
		--go-grpc_out=. --go-grpc_opt=module=$(MODULE) \
		api/proto/v1/provision.proto

# Sprint 2 CLI shortcuts
provision: build-s2
	./bin/wouri-provision

share: build-wourifs
	./bin/wourifs share --dev-insecure --institutions configs/institutions.example.json

fuse: build-s2
	./bin/wouri-fuse --namenode 127.0.0.1:9000

# Clean build artifacts
clean:
	rm -rf bin/ coverage.out coverage.html data/

# Lint (requires golangci-lint)
lint:
	golangci-lint run ./...

# Run everything: test + build
all: test build
