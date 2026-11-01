# iceberg_exporter

A Prometheus exporter for Apache Iceberg table metadata, built on
[iceberg-go](https://github.com/apache/iceberg-go).

The exporter discovers tables through one or more Iceberg catalogs, reads their
metadata and current-snapshot manifests on a fixed interval, and serves the
results from memory. Prometheus scrapes never talk to the catalog or object
store, so a slow refresh cannot block `/metrics`.

## Status

This is the first implementation. It collects table metadata, snapshot
information, live file totals, file-size distributions, and **partition
distribution aggregates**. Per-partition labels, partition-age buckets, and
other partition-level detail are intentionally not implemented yet.

## Build

REST, Hive and S3 are compiled in by default. Everything else is opt-in via Go
build tags:

| Backend | Build tag | Default |
|---|---|---|
| REST catalog | — | yes |
| Hive catalog | — | yes |
| S3 / S3-compatible object store | — | yes |
| Local filesystem | — | yes (iceberg-go core) |
| Glue catalog | `glue` | no |
| SQL catalog | `sql` | no |
| Hadoop catalog | `hadoop` | no |
| GCS object store | `gcs` | no |
| Azure object store | `azure` | no |
| pgx SQL driver | `sql` + `sqlpgx` | no |
| SQLite SQL driver | `sql` + `sqlsqlite` | no |

```bash
go build -o iceberg_exporter ./cmd/iceberg_exporter

# With Glue and GCS:
go build -tags "glue gcs" -o iceberg_exporter ./cmd/iceberg_exporter

# With every optional backend:
go build -tags "glue sql hadoop gcs azure sqlpgx sqlsqlite" \
  -o iceberg_exporter ./cmd/iceberg_exporter
```

The binary reports what it was built with:

```bash
./iceberg_exporter -version
```

Catalogs that are requested in the config but not compiled in fail at startup
with a message naming the missing build tag.

MySQL needs no extra tag: the SQL catalog package registers the MySQL driver.

### Docker

```bash
docker build -t iceberg_exporter .
docker build --build-arg TAGS="glue gcs" -t iceberg_exporter .
make docker
```

The image is a multi-stage build: `golang:1.27.3` compiles a static binary and
`gcr.io/distroless/static-debian13:nonroot` runs it as a non-root user. The
default `TAGS` is empty, so the image contains only REST, Hive and S3.

The image expects the config at `/etc/iceberg_exporter/config.yaml`:

```bash
docker run --rm -p 8080:8080 \
  -v "$PWD/config.yaml:/etc/iceberg_exporter/config.yaml:ro" \
  iceberg_exporter
```

## Run

```bash
./iceberg_exporter -c config.yaml
```

Flags:

- `-c`, `-config` — path to the YAML configuration (required)
- `-version` — print version, compiled catalogs and object stores, then exit

Endpoints:

- `/metrics` — Prometheus metrics
- `/healthz` — liveness; always 200 while the process is up
- `/readyz` — readiness; always 200 while the HTTP server is serving

Neither probe depends on catalog or scrape health: an unreachable catalog is
reported through `iceberg_exporter_scrape_success` and
`iceberg_table_scrape_success`, not by taking the exporter out of service.

## Configuration

```yaml
listen: ":8080"
log:
  level: info        # debug | info | warn | error
  format: logfmt     # logfmt | json

catalogs:
  prod:
    type: rest       # rest | hive | glue | sql | hadoop
    uri: https://iceberg.example.com
    warehouse: s3://lake/warehouse
    credential: ${ICEBERG_CREDENTIAL}
    properties:      # passed verbatim to iceberg-go's catalog.Load
      s3.region: eu-west-1
      # sql.driver: pgx

  # staging:
  #   type: hive
  #   uri: thrift://metastore:9083

scrape:
  interval: 5m
  timeout: 4m
  concurrency: 4          # tables collected in parallel
  catalogs: [prod]        # optional subset; defaults to all configured
  namespaces:
    include: ["**"]
    exclude: ["tmp.**"]
  tables:
    include: ["**"]
    exclude: ["**_staging"]
  manifests:
    enabled: true
    concurrency: 8        # manifest files read in parallel per table
  partitions:
    max_tracked: 100000   # per-table partition aggregation limit
  small_file_bytes: 67108864   # 64 MiB; 0 disables small-file counting
  large_file_bytes: 0          # 0 = 2 × the table's target file size
  target_file_size_bytes: 536870912  # fallback when the table has no property
```

`${VAR}` placeholders inside YAML values are expanded from the environment
after parsing, so credentials do not need to be stored in the file and are
taken verbatim (characters such as ` #`, `:` or quotes cannot change the
document). Quote a placeholder (`"${VAR}"`) to force a string; unquoted ones
are typed from the expanded value. Comments are not expanded. `$$` produces a literal
`$`. Referencing an unset variable is a startup error. Catalog properties are
never logged.

Filter patterns match the full dotted namespace or table name. `**` spans
namespace levels, and a trailing `.**` also matches the subtree root:
`prod.**` matches `prod`, `prod.sales`, and `prod.sales.eu`. Both `*` and `**`
may span dots; use `**` for clarity. Excludes win over includes.

`scrape.timeout` applies to one complete refresh. Note that manifest and
object-store reads inside iceberg-go are not context-aware, so a blocked object
store read can outlast the timeout; configure read timeouts in the storage
client where possible. A table that times out or fails keeps its previously
collected values; only its `iceberg_table_scrape_success` drops to 0, and the
failure is logged at warn level. Tables that disappear from the catalog are
removed on the next successful discovery.

Hive catalog table loads are serialized internally because the Hive Thrift
client is not safe for concurrent use. Only the table load (metastore lookup
and metadata file read) is serialized; manifest reads for Hive tables still run up to
`scrape.concurrency` tables at a time, like every other catalog type.

## Metrics

Common labels: `catalog`, `namespace`, `table`.

### Table metadata

| Metric | Description |
|---|---|
| `iceberg_table_info` | Always 1 for a collected table |
| `iceberg_table_format_version` | Iceberg format version |
| `iceberg_table_partitioned` | 1 if the current spec has partition fields |
| `iceberg_table_schema_count` | Number of schemas |
| `iceberg_table_partition_spec_count` | Number of partition specs |
| `iceberg_table_current_snapshot_timestamp_seconds` | Current snapshot time |
| `iceberg_table_current_snapshot_age_seconds` | Age of the current snapshot |
| `iceberg_table_last_updated_timestamp_seconds` | Last metadata update |
| `iceberg_table_last_updated_age_seconds` | Age of the last update |
| `iceberg_table_snapshot_count` | Retained snapshots |
| `iceberg_table_oldest_snapshot_timestamp_seconds` | Oldest retained snapshot |
| `iceberg_table_oldest_snapshot_age_seconds` | Age of the oldest snapshot |
| `iceberg_table_ref_count` | Branches and tags |
| `iceberg_table_last_sequence_number` | Last sequence number |
| `iceberg_table_refs{type}` | Refs by `type` (`branch`, `tag`); branches include `main` |
| `iceberg_table_oldest_ref_snapshot_age_seconds{type}` | Age of the snapshot referenced by the oldest non-main branch/tag; absent if none |
| `iceberg_table_metadata_log_entries` | Previous metadata files tracked in the metadata log |
| `iceberg_table_last_operation_timestamp_seconds{operation}` | Newest retained snapshot per operation (`append`, `overwrite`, `delete`, `replace`); `replace` is typically compaction |
| `iceberg_table_statistics_files` | Table statistics (Puffin) files |
| `iceberg_table_partition_statistics_files` | Partition statistics files |
| `iceberg_table_current_snapshot_has_statistics` | 1 if a statistics file exists for the current snapshot |

### Effective table settings

The `iceberg_table_setting_*` series report the value in effect for each
table: the table property when set and valid, otherwise the default shown.
Only this allowlist is read; no other properties are exported. Snapshot
retention set on the `main` branch ref overrides the table properties.

| Metric | Source | Default |
|---|---|---|
| `iceberg_table_setting_snapshot_max_age_seconds` | `history.expire.max-snapshot-age-ms` | 5 days |
| `iceberg_table_setting_min_snapshots_to_keep` | `history.expire.min-snapshots-to-keep` | 1 |
| `iceberg_table_setting_metadata_previous_versions_max` | `write.metadata.previous-versions-max` | 100 |
| `iceberg_table_setting_metadata_delete_after_commit_enabled` | `write.metadata.delete-after-commit.enabled` | 0 |
| `iceberg_table_setting_gc_enabled` | `gc.enabled` | 1 |
| `iceberg_table_setting_manifest_merge_enabled` | `commit.manifest-merge.enabled` | 1 |
| `iceberg_table_setting_target_file_size_bytes` | `write.target-file-size-bytes` | `scrape.target_file_size_bytes` (512 MiB) |

### Current snapshot delta

| Metric | Description |
|---|---|
| `iceberg_table_last_snapshot_files_added` | Data files added by the snapshot |
| `iceberg_table_last_snapshot_files_removed` | Data files removed |
| `iceberg_table_last_snapshot_bytes_added` | File bytes added |
| `iceberg_table_last_snapshot_bytes_removed` | File bytes removed |
| `iceberg_table_last_snapshot_records_added` | Records added |
| `iceberg_table_last_snapshot_records_removed` | Records removed |

### Live file statistics

These require `scrape.manifests.enabled: true`. Only live entries of the current
snapshot are counted; deleted entries are skipped.

| Metric | Description |
|---|---|
| `iceberg_table_manifests` | Manifests in the current snapshot |
| `iceberg_table_data_files` | Live data files |
| `iceberg_table_data_bytes` | Total live data bytes |
| `iceberg_table_data_records` | Records in live data files, before delete files are applied |
| `iceberg_table_delete_files` | Live position/equality delete files |
| `iceberg_table_delete_records` | Records in live delete files |
| `iceberg_table_delete_files_by_type{type}` | Live delete files by `type` (`position`, `equality`, `deletion_vector`) |
| `iceberg_table_delete_records_by_type{type}` | Records in live delete files by `type` |
| `iceberg_table_small_files` | Files below `small_file_bytes` |
| `iceberg_table_large_files` | Files above the large-file threshold |
| `iceberg_table_file_size_bytes` | Histogram of data-file sizes |

### Partitions (aggregates only)

| Metric | Description |
|---|---|
| `iceberg_table_partitions` | Distinct live partitions; 1 for an unpartitioned table |
| `iceberg_table_partition_files` | Histogram of files per partition |
| `iceberg_table_partition_bytes` | Histogram of bytes per partition |
| `iceberg_table_partition_records` | Histogram of records per partition |
| `iceberg_table_partition_files_max` | Largest file count in one partition |
| `iceberg_table_partition_bytes_max` | Largest byte size of one partition |
| `iceberg_table_partition_tracking_truncated` | 1 if `max_tracked` was reached |

Partitions are keyed by `(spec_id, partition values)`. After partition-spec
evolution, the same logical partition can be counted more than once.

### Exporter health

| Metric | Description |
|---|---|
| `iceberg_exporter_build_info` | Version and compiled backends |
| `iceberg_exporter_scrape_duration_seconds` | Duration of the last refresh |
| `iceberg_exporter_last_scrape_timestamp_seconds` | When it finished |
| `iceberg_exporter_scrape_success` | 1 if discovery completed without errors |
| `iceberg_exporter_tables_discovered` | Tables found |
| `iceberg_exporter_tables_collected` | Tables collected successfully |
| `iceberg_exporter_tables_failing` | Tables whose last collection failed |
| `iceberg_exporter_table_errors_total` | Failed table collections, by catalog |
| `iceberg_exporter_duplicate_table_labels` | Tables skipped because their labels collided |
| `iceberg_table_scrape_success` | Per-table success of the latest refresh |
| `iceberg_table_last_successful_scrape_timestamp_seconds` | Last success per table |

## Example queries

```promql
# Tables with a high small-file ratio
iceberg_table_small_files
  / clamp_min(iceberg_table_data_files, 1) > 0.5

# Average data file size
iceberg_table_data_bytes / clamp_min(iceberg_table_data_files, 1)

# Partition size skew (largest partition vs. average)
iceberg_table_partition_bytes_max
  / clamp_min(iceberg_table_data_bytes / clamp_min(iceberg_table_partitions, 1), 1)

# Tables with delete files that likely need compaction
iceberg_table_delete_files > 0

# Stale tables (no new snapshot in 24h)
iceberg_table_current_snapshot_age_seconds > 86400

# Snapshot retention is not expiring old metadata
iceberg_table_oldest_snapshot_age_seconds > 30 * 86400

# 90th percentile data-file size, per table
histogram_quantile(0.9, sum by (catalog, namespace, table, le) (iceberg_table_file_size_bytes))
```

## Table health rules

[`prometheus/iceberg_exporter.rules.yml`](prometheus/iceberg_exporter.rules.yml)
turns the metrics above into one series per unhealthy table and problem:

```
iceberg_table_problem{catalog, namespace, table, problem="small_files", severity="warning"}
```

A series exists only while the problem is present; its value is the measured
quantity (an age, ratio or count), so problems sort by severity of the
condition. Load the file through `rule_files` in `prometheus.yml`.

```promql
# Every unhealthy table, with its problems
iceberg_table_problem

# Number of tables per problem
count by (problem, severity) (iceberg_table_problem)

# Tables with the most problems
sort_desc(count by (catalog, namespace, table) (iceberg_table_problem))

# Worst small-file offenders
topk(10, iceberg_table_problem{problem="small_files"})
```

| Problem | Severity | Condition |
|---|---|---|
| `scrape_failing` | warning | The exporter cannot read the table; other problems reflect its last good state |
| `snapshots_not_expired` | warning | Oldest snapshot older than 2 x the table's retention age, more snapshots than `min_snapshots_to_keep`, GC enabled |
| `too_many_snapshots` | warning | More than 1000 retained snapshots |
| `stale_refs` | warning | A non-main branch or tag references a snapshot older than 30 days |
| `metadata_files_not_cleaned` | warning | Metadata log full and `write.metadata.delete-after-commit.enabled=false` |
| `gc_disabled` | info | `gc.enabled=false` |
| `small_files` | warning | More than 30% of data files are small, and more than 100 data files |
| `too_many_manifests` | warning | More than 1000 manifests |
| `manifest_merge_disabled` | info | `commit.manifest-merge.enabled=false` |
| `delete_file_buildup` | warning | Delete records exceed 10% of data records, and more than 10 delete files |
| `equality_deletes` | warning | More than 100 equality delete files |
| `compaction_overdue` | warning | Delete files or small files present, and no retained `replace` snapshot from the last 7 days |
| `partition_skew` | warning | Largest partition more than 10 x the average, with more than 10 partitions |
| `over_partitioned` | warning | More than 1000 partitions averaging under 64 MiB |
| `hot_partition_files` | warning | More than 1000 files in one partition |
| `partition_tracking_truncated` | info | Partition distributions are approximate |
| `format_v1` | info | Format version 1 |
| `missing_statistics` | info | Over 100 GiB of data and no statistics file for the current snapshot |

Thresholds are starting points, marked `Tune:` in the file. Copy the file and
adjust them, or add label matchers (for example `{namespace!~"raw\\..*"}`) to
scope a check. The file also records `iceberg:tables_with_problem:count` and
defines one alert, `IcebergTableProblem`, for warning/critical problems held
for 1h. `make rules-check` validates the file and runs its promtool unit tests
(`prometheus/iceberg_exporter.rules_test.yml`) in a container.

Not covered, because table metadata does not record them: orphan files,
commit conflicts, and dangling delete files (which need per-partition state).

## Development

```bash
make build
make test
make test-race
make test-all-tags
make vet
make rules-check   # promtool via docker; CONTAINER=podman or PROMTOOL=promtool to override
```

`make test-all-tags` compiles and tests every optional backend so tagged files
do not rot.

The integration surface is deliberately small: `internal/scrape` owns the
refresh loop and table state, `internal/icebergstats` turns metadata and
manifests into plain values, and `internal/metrics` renders those values. Tests
use a fake catalog and in-memory data files; no object store is required.
