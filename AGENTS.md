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
- `internal/icebergstats`: metadata + manifest scanning into plain values. `Collect` reads only the current snapshot and uses `Entries(fs, true)` so deleted entries are excluded.
- `internal/metrics`: renders `scrape.State` on each Prometheus scrape. Do not call catalogs/object stores from `Collect`; `/metrics` must stay cache-only.
- Hive catalog table loads are serialized via `Scraper.lockCatalog` because its Thrift client is not concurrency-safe. Keep that path if changing table workers.
- Table/manifest reads are concurrent; totals are guarded by a mutex in `accumulator`. Partition state is bounded by `scrape.partitions.max_tracked` (totals stay exact; distributions truncate).
- Failure semantics: a failed table keeps its previous `Stats` and sets `iceberg_table_scrape_success=0`; tables disappear only on successful discovery; `iceberg_exporter_scrape_success` reflects discovery only. `/readyz` requires a fully successful refresh within `2 × interval`.

## Domain decisions

- Partition support is intentionally **Layer 1 only**: distribution histograms and max/avg, no `partition` label, no partition-age buckets, no top-N/debug endpoint. Discuss before adding.
- Unpartitioned tables report `iceberg_table_partitions=1` (implicit partition); `partitioned` counts only non-void spec fields.
- Snapshot IDs are exposed as the `snapshot_id` label on `iceberg_table_current_snapshot_info`, not as a numeric gauge (int64 precision).
- Keep Prometheus units `_seconds`; metric names/descriptors live only in `internal/metrics/metrics.go`.
- Histograms use cumulative buckets from `icebergstats.Histogram`; do not hand-build `le` values.
- Summary bytes are all file bytes (including delete files); `data_records` is records in live data files before deletes are applied.

## Gotchas

- Config uses `yaml.Decoder.KnownFields(true)`: typos fail. Only `${VAR}` placeholders are expanded (`$$` = literal `$`); unset variables are errors. Expansion applies to the whole file, so escape placeholders in comments as `$${VAR}`.
- Never log catalog `properties` or credentials.
- `scrape.timeout` cannot interrupt iceberg-go manifest/object-store reads (APIs are not context-aware); only context-aware work and per-entry checks are cancellable.
- Dotted namespace/table labels can collide for exotic namespace components. Internal keys are length-prefixed and collision-free; the collector skips duplicate label sets and reports `iceberg_exporter_duplicate_table_labels` rather than failing the scrape.
- `internal/scrape/hadoop_integration_test.go` only compiles with `-tags hadoop`; the non-tagged tests use fake catalogs and in-memory manifests, so no external services are needed.
- `config.example.yaml` is parsed by `TestExampleConfigParses`, so keep it valid when config keys change; update the README config reference at the same time.
