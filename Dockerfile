# syntax=docker/dockerfile:1

FROM --platform=$BUILDPLATFORM golang:1.27.3 AS build

ARG TAGS=""
ARG VERSION=dev
ARG REVISION=unknown
ARG TARGETOS
ARG TARGETARCH

WORKDIR /src

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY . .

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build \
        -trimpath \
        -tags "${TAGS}" \
        -ldflags "-s -w -X main.version=${VERSION} -X main.revision=${REVISION}" \
        -o /out/iceberg_exporter \
        ./cmd/iceberg_exporter

FROM gcr.io/distroless/static-debian13:nonroot

COPY --from=build /out/iceberg_exporter /iceberg_exporter

USER nonroot:nonroot
EXPOSE 8080

ENTRYPOINT ["/iceberg_exporter"]
CMD ["-c", "/etc/iceberg_exporter/config.yaml"]
