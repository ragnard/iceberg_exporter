# AGENTS.md

Prometheus exporter for Apache Iceberg table metadata, built on `apache/iceberg-go v0.7.0`. Single Go module (`go 1.27.1`), entrypoint `cmd/iceberg_exporter`.

## Commands

```bash
make build            # default: REST + Hive + S3 only
make build-full       # all optional backends
make test
go test ./internal/scrape -run TestName -v   # single test
make test-all-tags    # required when touching tagged files
make vet-all-tags
gofmt -w cmd internal
```

- There is no CI. `make test-all-tags` is the only check that compiles `//go:build` files.
- `make docker` forwards `TAGS`/`VERSION` to a multi-stage `golang:1.27.3` -> distroless Debian 13 image; it builds with `CGO_ENABLED=0` and runs as nonroot with config expected at `/etc/iceberg_exporter/config.yaml`.
- `go mod tidy` considers tagged files, so pgx/sqlite/cloud SDKs stay in `go.mod` even though they are not in the default binary.

## Build tags are load-bearing

Default binary: REST catalog, Hive catalog, S3 object store (`internal/registry/defaults.go`). Everything else lives in tagged files.

| Feature | Tag |
|---|---|
| Glue catalog | `glue` |
| SQL catalog | `sql` |
| Hadoop catalog | `hadoop` |
| GCS / Azure storage | `gcs` / `azure` |
| pgx / SQLite SQL drivers | `sql sqlpgx` / `sql sqlsqlite` |

When adding a backend: add a tagged blank-import file in `internal/registry/`, keep `registry.CatalogTypes()`/`ObjectStoreSchemes()` honest, and update `ALL_TAGS` in the `Makefile` plus the README table. Untagged imports change the default binary and break the lean-build guarantee.

## Architecture and invariants

- `internal/scrape`: discovery, refresh scheduling, table state. `Refresh` is serialized by `refreshMu`; `State` is immutable after `atomic.Pointer.Store`.
- Catalog clients live for one refresh only: `Refresh` calls `Catalogs.open` and closes every client when it returns. Do not cache clients across refreshes (the Hive Thrift client has no reconnect logic, so a dropped connection would stick). `NewCatalogs` at startup validates without connecting; a catalog load failure is a per-refresh discovery error.
- `internal/icebergstats`: metadata + manifest scanning into plain values. `Collect` reads only the current snapshot and uses `Entries(fs, true)` so deleted entries are excluded.
- Manifest-scan reuse: `Collect` takes `Options.Previous` and skips the manifest list and manifests when `fileStatsKey` (table UUID, snapshot ID, small/large thresholds, `max_tracked`) matches. Any new input that affects manifest-derived stats must go into `fileStatsKey`, and any new manifest-derived field into `copyFileStats`.
- `internal/metrics`: renders `scrape.State` on each Prometheus scrape. Do not call catalogs/object stores from `Collect`; `/metrics` must stay cache-only.
- Hive catalog table loads are serialized via `Scraper.loadLock` → `icebergstats.Options.LoadLock` because its Thrift client is not concurrency-safe. The lock covers only `LoadTable`; manifest reads must stay outside it. Keep that path if changing table workers.
- Table/manifest reads are concurrent; totals are guarded by a mutex in `accumulator`. Partition state is bounded by `scrape.partitions.max_tracked` (totals stay exact; distributions truncate).
- Failure semantics: a failed table keeps its previous `Stats` and sets `iceberg_table_scrape_success=0`; tables disappear only on successful discovery; `iceberg_exporter_scrape_success` reflects discovery only. `/healthz` and `/readyz` reflect only the process and must not depend on catalog or scrape success.

## Domain decisions

- Partition support is intentionally **Layer 1 only**: distribution histograms and max/avg, no `partition` label, no partition-age buckets, no top-N/debug endpoint. Discuss before adding.
- Unpartitioned tables report `iceberg_table_partitions=1` (implicit partition); `partitioned` counts only non-void spec fields.
- Do not expose snapshot IDs (as labels or gauges): a label creates a new series per commit and int64 IDs lose precision as floats.
- Keep Prometheus units `_seconds`; metric names/descriptors live only in `internal/metrics/metrics.go`.
- Histograms use cumulative buckets from `icebergstats.Histogram`; do not hand-build `le` values.
- Summary bytes are all file bytes (including delete files); `data_records` is records in live data files before deletes are applied.

## Gotchas

- Config is parsed to a `yaml.Node`, `${VAR}` placeholders are expanded inside scalars only (`$$` = literal `$`; unset variables are errors; comments are not expanded), then decoded. `yaml.Node.Decode` has no `KnownFields`, so `checkKnownFields` enforces strict keys: typos fail. Keep it working if you add custom-unmarshaled config types.
- Never log catalog `properties` or credentials.
- `scrape.timeout` cannot interrupt iceberg-go manifest/object-store reads (APIs are not context-aware); only context-aware work and per-entry checks are cancellable.
- Dotted namespace/table labels can collide for exotic namespace components. Internal keys are length-prefixed and collision-free; tables are sorted with the internal key as tie-breaker (`compareRefs`), so the collector deterministically keeps the same table, skips the other duplicate label sets and reports `iceberg_exporter_duplicate_table_labels` rather than failing the scrape.
- `internal/scrape/hadoop_integration_test.go` only compiles with `-tags hadoop`; the non-tagged tests use fake catalogs and in-memory manifests, so no external services are needed.
- `config.example.yaml` is parsed by `TestExampleConfigParses`, so keep it valid when config keys change; update the README config reference at the same time.
