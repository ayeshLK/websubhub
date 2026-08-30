GO ?= go
DOCKER ?= docker
GORELEASER ?= goreleaser
VERSION ?= dev
COMMIT ?= unknown
BUILD_DATE ?= unknown
LDFLAGS = -s -w \
	-X github.com/ayeshLK/websubhub/internal/buildinfo.version=$(VERSION) \
	-X github.com/ayeshLK/websubhub/internal/buildinfo.commit=$(COMMIT) \
	-X github.com/ayeshLK/websubhub/internal/buildinfo.date=$(BUILD_DATE)

.PHONY: build check compose-smoke container-check docs-check format-check source-header-check generate-check license-check release-check release-snapshot release-version-check test test-integration-kafka

build:
	mkdir -p bin
	$(GO) build -trimpath -ldflags "$(LDFLAGS)" -o bin/websubhub ./cmd/websubhub
	$(GO) build -trimpath -ldflags "$(LDFLAGS)" -o bin/websubhub-consolidator ./cmd/websubhub-consolidator

check: format-check generate-check source-header-check release-version-check
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

release-version-check:
	sh scripts/verify-release-version.sh

test:
	$(GO) test -shuffle=on ./...

test-integration-kafka:
	@test -n "$(WEBSUBHUB_TEST_KAFKA_BROKERS)" || (echo "WEBSUBHUB_TEST_KAFKA_BROKERS is required" >&2; exit 1)
	WEBSUBHUB_TEST_KAFKA_BROKERS="$(WEBSUBHUB_TEST_KAFKA_BROKERS)" \
		$(GO) test -v ./internal/persistence/messagestore/kafka

compose-smoke:
	sh deploy/compose/smoke.sh

release-check:
	@command -v $(GORELEASER) >/dev/null || (echo "goreleaser is required" >&2; exit 1)
	$(GORELEASER) check
	cmp configs/websubhub.example.toml packaging/websubhub/config/websubhub.toml
	cmp configs/websubhub-consolidator.example.toml packaging/websubhub-consolidator/config/websubhub-consolidator.toml

release-snapshot: release-check
	@command -v syft >/dev/null || (echo "syft is required" >&2; exit 1)
	$(GORELEASER) release --snapshot --clean --skip=publish,sign
	sh scripts/verify-release.sh

container-check:
	$(DOCKER) build --target websubhub \
		--build-arg VERSION=$(VERSION) --build-arg COMMIT=$(COMMIT) \
		--build-arg BUILD_DATE=$(BUILD_DATE) -t websubhub:local .
	$(DOCKER) run --rm websubhub:local --version
	$(DOCKER) build --target websubhub-consolidator \
		--build-arg VERSION=$(VERSION) --build-arg COMMIT=$(COMMIT) \
		--build-arg BUILD_DATE=$(BUILD_DATE) -t websubhub-consolidator:local .
	$(DOCKER) run --rm websubhub-consolidator:local --version
