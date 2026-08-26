GO ?= go
VERSION ?= dev
COMMIT ?= unknown
BUILD_DATE ?= unknown
LDFLAGS = -s -w \
	-X github.com/ayeshLK/websubhub/internal/buildinfo.version=$(VERSION) \
	-X github.com/ayeshLK/websubhub/internal/buildinfo.commit=$(COMMIT) \
	-X github.com/ayeshLK/websubhub/internal/buildinfo.date=$(BUILD_DATE)

.PHONY: build check compose-smoke docs-check format-check source-header-check generate-check license-check test

build:
	mkdir -p bin
	$(GO) build -trimpath -ldflags "$(LDFLAGS)" -o bin/websubhub ./cmd/websubhub
	$(GO) build -trimpath -ldflags "$(LDFLAGS)" -o bin/websubhub-consolidator ./cmd/websubhub-consolidator

check: format-check generate-check source-header-check
	$(GO) vet ./...
	$(GO) test -shuffle=on ./...
	$(GO) test -race ./...
	$(MAKE) license-check
	$(MAKE) docs-check

format-check:
	@test -z "$$(gofmt -l .)" || (echo "Go files need formatting" >&2; gofmt -l . >&2; exit 1)

generate-check:
	$(GO) generate ./...
	@git diff --exit-code

source-header-check:
	$(GO) run ./internal/tools/sourceheaders

license-check:
	$(GO) run ./internal/tools/licensecheck

docs-check:
	$(GO) run ./internal/tools/doclinks

test:
	$(GO) test -shuffle=on ./...

compose-smoke:
	sh deploy/compose/smoke.sh
