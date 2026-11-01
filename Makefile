BINARY  ?= iceberg_exporter
IMAGE   ?= iceberg_exporter
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
REVISION?= $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
TAGS    ?=
LDFLAGS  = -s -w -X main.version=$(VERSION) -X main.revision=$(REVISION)
ALL_TAGS = glue sql hadoop gcs azure sqlpgx sqlsqlite
PROMETHEUS_IMAGE ?= prom/prometheus:v3.5.0
CONTAINER ?= docker
PROMTOOL ?= $(CONTAINER) run --rm -v $(CURDIR)/prometheus:/rules:ro,Z -w /rules --entrypoint promtool $(PROMETHEUS_IMAGE)

.PHONY: build build-full docker test test-race test-all-tags vet vet-all-tags rules-check tidy clean

build:
	go build -trimpath -ldflags "$(LDFLAGS)" -tags "$(TAGS)" -o $(BINARY) ./cmd/iceberg_exporter

build-full:
	go build -trimpath -ldflags "$(LDFLAGS)" -tags "$(ALL_TAGS)" -o $(BINARY) ./cmd/iceberg_exporter

docker:
	docker build \
		--build-arg TAGS="$(TAGS)" \
		--build-arg VERSION="$(VERSION)" \
		--build-arg REVISION="$(REVISION)" \
		-t $(IMAGE) .

test:
	go test ./...

test-race:
	go test -race ./...

test-all-tags:
	go test -tags "$(ALL_TAGS)" ./...

vet:
	go vet ./...

vet-all-tags:
	go vet -tags "$(ALL_TAGS)" ./...

# Validates and unit-tests the Prometheus rules. Runs promtool in a container;
# override with PROMTOOL=promtool to use a local binary.
rules-check:
	cd prometheus && $(PROMTOOL) check rules iceberg_exporter.rules.yml
	cd prometheus && $(PROMTOOL) test rules iceberg_exporter.rules_test.yml

tidy:
	go mod tidy

clean:
	rm -f $(BINARY)
