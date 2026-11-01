BINARY  ?= iceberg_exporter
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
REVISION?= $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
TAGS    ?=
LDFLAGS  = -s -w -X main.version=$(VERSION) -X main.revision=$(REVISION)
ALL_TAGS = glue sql hadoop gcs azure sqlpgx sqlsqlite

.PHONY: build build-full test test-race test-all-tags vet vet-all-tags tidy clean

build:
	go build -trimpath -ldflags "$(LDFLAGS)" -tags "$(TAGS)" -o $(BINARY) ./cmd/iceberg_exporter

build-full:
	go build -trimpath -ldflags "$(LDFLAGS)" -tags "$(ALL_TAGS)" -o $(BINARY) ./cmd/iceberg_exporter

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

tidy:
	go mod tidy

clean:
	rm -f $(BINARY)
