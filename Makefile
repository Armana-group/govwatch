GO ?= GOTOOLCHAIN=local mise exec -- go

.PHONY: build test vet
build:
	$(GO) build -o bin/govwatch ./cmd/govwatch
test:
	$(GO) test ./...
vet:
	$(GO) vet ./...
