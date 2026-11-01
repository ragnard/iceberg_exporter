// Package metrics renders the latest scrape state as Prometheus metrics.
package metrics

import (
	"strings"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/ragnard/iceberg_exporter/internal/icebergstats"
	"github.com/ragnard/iceberg_exporter/internal/scrape"
)

var tableLabels = []string{"catalog", "namespace", "table"}

// BuildInfo describes what was compiled into this binary.
type BuildInfo struct {
	Version      string
	Revision     string
	Catalogs     []string
	ObjectStores []string
}

// StateProvider supplies the latest scrape state.
type StateProvider interface {
	State() *scrape.State
}

// Collector renders the latest scrape state.
type Collector struct {
	scraper     StateProvider
	build       BuildInfo
	tableErrors *prometheus.CounterVec
	descs       []*prometheus.Desc

	formatVersion           *prometheus.Desc
	partitioned             *prometheus.Desc
	schemaCount             *prometheus.Desc
	partitionSpecCount      *prometheus.Desc
	currentSnapshotTime     *prometheus.Desc
	lastUpdatedTime         *prometheus.Desc
	snapshotCount           *prometheus.Desc
	oldestSnapshotTime      *prometheus.Desc
	lastSequenceNumber      *prometheus.Desc
	targetFileSizeBytes     *prometheus.Desc
	filesAdded              *prometheus.Desc
	filesRemoved            *prometheus.Desc
	bytesAdded              *prometheus.Desc
	bytesRemoved            *prometheus.Desc
	recordsAdded            *prometheus.Desc
	recordsRemoved          *prometheus.Desc
	manifests               *prometheus.Desc
	dataFiles               *prometheus.Desc
	dataBytes               *prometheus.Desc
	dataRecords             *prometheus.Desc
	deleteFiles             *prometheus.Desc
	deleteRecords           *prometheus.Desc
	fileSizes               *prometheus.Desc
	partitions              *prometheus.Desc
	partitionFiles          *prometheus.Desc
	partitionBytes          *prometheus.Desc
	partitionRecords        *prometheus.Desc
	partitionFilesMax       *prometheus.Desc
	partitionBytesMax       *prometheus.Desc
	partitionTrackingFailed *prometheus.Desc
	tableScrapeSuccess      *prometheus.Desc
	tableLastSuccess        *prometheus.Desc
	duplicateTableLabels    *prometheus.Desc

	refs                       *prometheus.Desc
	oldestRefSnapshotTime      *prometheus.Desc
	metadataLogEntries         *prometheus.Desc
	lastOperationTime          *prometheus.Desc
	statisticsFiles            *prometheus.Desc
	partitionStatisticsFiles   *prometheus.Desc
	currentSnapshotHasStats    *prometheus.Desc
	snapshotMaxAge             *prometheus.Desc
	minSnapshotsToKeep         *prometheus.Desc
	metadataPreviousVersionMax *prometheus.Desc
	metadataDeleteAfterCommit  *prometheus.Desc
	gcEnabled                  *prometheus.Desc
	manifestMergeEnabled       *prometheus.Desc
	deleteFilesByType          *prometheus.Desc
	deleteRecordsByType        *prometheus.Desc
}

// NewCollector creates the exporter collector.
func NewCollector(scraper StateProvider, build BuildInfo) *Collector {
	d := func(name, help string) *prometheus.Desc {
		return prometheus.NewDesc("iceberg_table_"+name, help, tableLabels, nil)
	}
	dl := func(name, help, label string) *prometheus.Desc {
		return prometheus.NewDesc("iceberg_table_"+name, help, append(append([]string{}, tableLabels...), label), nil)
	}
	c := &Collector{
		scraper: scraper,
		build:   build,
		tableErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "iceberg_exporter_table_errors_total",
			Help: "Total number of failed table collections, by catalog.",
		}, []string{"catalog"}),

		formatVersion:              d("format_version", "Iceberg table format version."),
		partitioned:                d("partitioned", "1 if the table's current partition spec has fields, 0 otherwise."),
		schemaCount:                d("schema_count", "Number of schemas in the table metadata."),
		partitionSpecCount:         d("partition_spec_count", "Number of partition specs in the table metadata."),
		currentSnapshotTime:        d("current_snapshot_timestamp_seconds", "Timestamp of the current snapshot."),
		lastUpdatedTime:            d("last_updated_timestamp_seconds", "Time the table metadata was last updated."),
		snapshotCount:              d("snapshot_count", "Number of snapshots retained in the table metadata."),
		oldestSnapshotTime:         d("oldest_snapshot_timestamp_seconds", "Timestamp of the oldest retained snapshot."),
		lastSequenceNumber:         d("last_sequence_number", "Last sequence number in the table metadata."),
		targetFileSizeBytes:        d("setting_target_file_size_bytes", "Effective target data file size, from table properties or defaults."),
		filesAdded:                 d("last_snapshot_files_added", "Data files added by the current snapshot."),
		filesRemoved:               d("last_snapshot_files_removed", "Data files removed by the current snapshot."),
		bytesAdded:                 d("last_snapshot_bytes_added", "File bytes added by the current snapshot."),
		bytesRemoved:               d("last_snapshot_bytes_removed", "File bytes removed by the current snapshot."),
		recordsAdded:               d("last_snapshot_records_added", "Records added by the current snapshot."),
		recordsRemoved:             d("last_snapshot_records_removed", "Records removed by the current snapshot."),
		manifests:                  d("manifests", "Number of manifests in the current snapshot."),
		dataFiles:                  d("data_files", "Number of live data files in the current snapshot."),
		dataBytes:                  d("data_bytes", "Total size of live data files in the current snapshot."),
		dataRecords:                d("data_records", "Records in live data files in the current snapshot, before delete files are applied."),
		deleteFiles:                d("delete_files", "Number of live delete files in the current snapshot."),
		deleteRecords:              d("delete_records", "Total records in live delete files in the current snapshot."),
		fileSizes:                  d("file_size_bytes", "Distribution of live data file sizes."),
		partitions:                 d("partitions", "Number of distinct live partitions; 1 for unpartitioned tables."),
		partitionFiles:             d("partition_files", "Distribution of live data files per partition."),
		partitionBytes:             d("partition_bytes", "Distribution of live data bytes per partition."),
		partitionRecords:           d("partition_records", "Distribution of live records per partition."),
		partitionFilesMax:          d("partition_files_max", "Largest number of live data files in a single partition."),
		partitionBytesMax:          d("partition_bytes_max", "Largest number of live data bytes in a single partition."),
		partitionTrackingFailed:    d("partition_tracking_truncated", "1 if partition distribution tracking hit the configured limit."),
		tableScrapeSuccess:         d("scrape_success", "1 if the table was collected successfully in the latest refresh, 0 otherwise."),
		tableLastSuccess:           d("last_successful_scrape_timestamp_seconds", "Time the table was last collected successfully."),
		refs:                       dl("refs", "Number of snapshot references, by type (branch or tag). Branches include main.", "type"),
		oldestRefSnapshotTime:      dl("oldest_ref_snapshot_timestamp_seconds", "Timestamp of the snapshot referenced by the oldest non-main branch or tag, by type.", "type"),
		metadataLogEntries:         d("metadata_log_entries", "Number of previous metadata files tracked in the metadata log."),
		lastOperationTime:          dl("last_operation_timestamp_seconds", "Timestamp of the newest retained snapshot for each snapshot operation; replace is typically compaction.", "operation"),
		statisticsFiles:            d("statistics_files", "Number of table statistics (Puffin) files registered in the table metadata."),
		partitionStatisticsFiles:   d("partition_statistics_files", "Number of partition statistics files registered in the table metadata."),
		currentSnapshotHasStats:    d("current_snapshot_has_statistics", "1 if a table statistics file exists for the current snapshot, 0 otherwise."),
		snapshotMaxAge:             d("setting_snapshot_max_age_seconds", "Effective snapshot retention age (main branch override, history.expire.max-snapshot-age-ms, or the Iceberg default)."),
		minSnapshotsToKeep:         d("setting_min_snapshots_to_keep", "Effective minimum number of snapshots kept by expiration (main branch override, history.expire.min-snapshots-to-keep, or the Iceberg default)."),
		metadataPreviousVersionMax: d("setting_metadata_previous_versions_max", "Effective write.metadata.previous-versions-max."),
		metadataDeleteAfterCommit:  d("setting_metadata_delete_after_commit_enabled", "1 if write.metadata.delete-after-commit.enabled is effectively true, 0 otherwise."),
		gcEnabled:                  d("setting_gc_enabled", "1 if gc.enabled is effectively true, 0 otherwise."),
		manifestMergeEnabled:       d("setting_manifest_merge_enabled", "1 if commit.manifest-merge.enabled is effectively true, 0 otherwise."),
		deleteFilesByType:          dl("delete_files_by_type", "Number of live delete files in the current snapshot, by type (position, equality, deletion_vector).", "type"),
		deleteRecordsByType:        dl("delete_records_by_type", "Records in live delete files in the current snapshot, by type (position, equality, deletion_vector).", "type"),
		duplicateTableLabels: prometheus.NewDesc(
			"iceberg_exporter_duplicate_table_labels",
			"Number of tables skipped because their catalog/namespace/table labels collided with another table.",
			nil, nil,
		),
	}

	c.descs = []*prometheus.Desc{
		c.formatVersion, c.partitioned, c.schemaCount, c.partitionSpecCount,
		c.currentSnapshotTime,
		c.lastUpdatedTime, c.snapshotCount,
		c.oldestSnapshotTime,
		c.lastSequenceNumber, c.targetFileSizeBytes,
		c.filesAdded, c.filesRemoved, c.bytesAdded, c.bytesRemoved,
		c.recordsAdded, c.recordsRemoved,
		c.manifests, c.dataFiles, c.dataBytes, c.dataRecords,
		c.deleteFiles, c.deleteRecords,
		c.fileSizes, c.partitions, c.partitionFiles, c.partitionBytes,
		c.partitionRecords, c.partitionFilesMax, c.partitionBytesMax,
		c.partitionTrackingFailed, c.tableScrapeSuccess, c.tableLastSuccess,
		c.duplicateTableLabels,
		c.refs, c.oldestRefSnapshotTime, c.metadataLogEntries, c.lastOperationTime,
		c.statisticsFiles, c.partitionStatisticsFiles, c.currentSnapshotHasStats,
		c.snapshotMaxAge, c.minSnapshotsToKeep, c.metadataPreviousVersionMax,
		c.metadataDeleteAfterCommit, c.gcEnabled, c.manifestMergeEnabled,
		c.deleteFilesByType, c.deleteRecordsByType,
	}
	return c
}

// TableErrors returns the counter of failed table collections.
func (c *Collector) TableErrors() *prometheus.CounterVec { return c.tableErrors }

// ObserveTableError increments the per-catalog table error counter.
func (c *Collector) ObserveTableError(catalog string) { c.tableErrors.WithLabelValues(catalog).Inc() }

var (
	buildInfoDesc = prometheus.NewDesc(
		"iceberg_exporter_build_info",
		"Build information and the catalog and object-store backends compiled into this binary.",
		[]string{"version", "revision", "catalogs", "object_stores"}, nil,
	)
	scrapeDurationDesc = prometheus.NewDesc(
		"iceberg_exporter_scrape_duration_seconds",
		"Duration of the latest refresh.",
		nil, nil,
	)
	lastScrapeDesc = prometheus.NewDesc(
		"iceberg_exporter_last_scrape_timestamp_seconds",
		"Time the latest refresh finished.",
		nil, nil,
	)
	scrapeSuccessDesc = prometheus.NewDesc(
		"iceberg_exporter_scrape_success",
		"1 if the latest refresh completed discovery without errors, 0 otherwise.",
		nil, nil,
	)
	tablesDiscoveredDesc = prometheus.NewDesc(
		"iceberg_exporter_tables_discovered",
		"Number of tables discovered in the latest refresh.",
		nil, nil,
	)
	tablesCollectedDesc = prometheus.NewDesc(
		"iceberg_exporter_tables_collected",
		"Number of tables collected successfully in the latest refresh.",
		nil, nil,
	)
	tablesFileStatsReusedDesc = prometheus.NewDesc(
		"iceberg_exporter_tables_file_stats_reused",
		"Number of tables in the latest refresh whose file statistics were reused without reading manifests, because their current snapshot was unchanged.",
		nil, nil,
	)
	tablesFailingDesc = prometheus.NewDesc(
		"iceberg_exporter_tables_failing",
		"Number of tables whose latest collection failed.",
		nil, nil,
	)
)

// Describe implements prometheus.Collector.
func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	for _, desc := range c.descs {
		ch <- desc
	}
	ch <- buildInfoDesc
	ch <- scrapeDurationDesc
	ch <- lastScrapeDesc
	ch <- scrapeSuccessDesc
	ch <- tablesDiscoveredDesc
	ch <- tablesCollectedDesc
	ch <- tablesFailingDesc
	ch <- tablesFileStatsReusedDesc
}

// Collect implements prometheus.Collector.
func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	ch <- prometheus.MustNewConstMetric(buildInfoDesc, prometheus.GaugeValue, 1,
		c.build.Version, c.build.Revision,
		strings.Join(c.build.Catalogs, ","), strings.Join(c.build.ObjectStores, ","))

	st := c.scraper.State()
	if st == nil {
		return
	}

	gauge := func(desc *prometheus.Desc, v float64) {
		ch <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, v)
	}
	gauge(scrapeDurationDesc, st.Duration.Seconds())
	gauge(lastScrapeDesc, float64(st.Finished.Unix()))
	gauge(scrapeSuccessDesc, boolFloat(st.Success))
	gauge(tablesDiscoveredDesc, float64(st.Discovered))
	gauge(tablesCollectedDesc, float64(st.Collected))
	gauge(tablesFailingDesc, float64(st.Failing))
	gauge(tablesFileStatsReusedDesc, float64(st.FileStatsReused))

	seenTables := make(map[string]struct{}, len(st.Tables))
	duplicates := 0
	for _, res := range st.Tables {
		labels := []string{res.Ref.Catalog, res.Ref.Namespace, res.Ref.Table}
		labelKey := strings.Join(labels, "\x00")
		if _, seen := seenTables[labelKey]; seen {
			duplicates++
			continue
		}
		seenTables[labelKey] = struct{}{}
		tableGauge := func(desc *prometheus.Desc, v float64) {
			ch <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, v, labels...)
		}
		tableGauge(c.tableScrapeSuccess, boolFloat(res.Success))
		if !res.LastSuccess.IsZero() {
			tableGauge(c.tableLastSuccess, float64(res.LastSuccess.Unix()))
		}

		s := res.Stats
		if s == nil {
			continue
		}
		tableGauge(c.formatVersion, float64(s.FormatVersion))
		tableGauge(c.partitioned, boolFloat(s.Partitioned))
		tableGauge(c.schemaCount, float64(s.SchemaCount))
		tableGauge(c.partitionSpecCount, float64(s.PartitionSpecCount))
		if s.HasCurrentSnapshot {
			tableGauge(c.currentSnapshotTime, msToSeconds(s.CurrentSnapshotTimestampMs))
		}
		tableGauge(c.lastUpdatedTime, msToSeconds(s.LastUpdatedMs))
		tableGauge(c.snapshotCount, float64(s.SnapshotCount))
		if s.HasSnapshots {
			tableGauge(c.oldestSnapshotTime, msToSeconds(s.OldestSnapshotTimestampMs))
		}
		tableGauge(c.lastSequenceNumber, float64(s.LastSequenceNumber))
		tableGauge(c.targetFileSizeBytes, float64(s.Settings.TargetFileSizeBytes))

		labelGauge := func(desc *prometheus.Desc, v float64, extra string) {
			ch <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, v, append(append([]string{}, labels...), extra)...)
		}
		labelGauge(c.refs, float64(s.Branches), "branch")
		labelGauge(c.refs, float64(s.Tags), "tag")
		if s.HasOldestBranch {
			labelGauge(c.oldestRefSnapshotTime, msToSeconds(s.OldestBranchSnapshotMs), "branch")
		}
		if s.HasOldestTag {
			labelGauge(c.oldestRefSnapshotTime, msToSeconds(s.OldestTagSnapshotMs), "tag")
		}
		tableGauge(c.metadataLogEntries, float64(s.MetadataLogEntries))
		for _, op := range icebergstats.SnapshotOperations {
			if ms, ok := s.LastOperationMs[op]; ok {
				labelGauge(c.lastOperationTime, msToSeconds(ms), op)
			}
		}
		tableGauge(c.statisticsFiles, float64(s.StatisticsFiles))
		tableGauge(c.partitionStatisticsFiles, float64(s.PartitionStatisticsFiles))
		tableGauge(c.currentSnapshotHasStats, boolFloat(s.CurrentSnapshotHasStatistics))
		tableGauge(c.snapshotMaxAge, msToSeconds(s.Settings.SnapshotMaxAgeMs))
		tableGauge(c.minSnapshotsToKeep, float64(s.Settings.MinSnapshotsToKeep))
		tableGauge(c.metadataPreviousVersionMax, float64(s.Settings.MetadataPreviousVersionsMax))
		tableGauge(c.metadataDeleteAfterCommit, boolFloat(s.Settings.MetadataDeleteAfterCommit))
		tableGauge(c.gcEnabled, boolFloat(s.Settings.GCEnabled))
		tableGauge(c.manifestMergeEnabled, boolFloat(s.Settings.ManifestMergeEnabled))

		if s.HasLastSnapshotSummary {
			tableGauge(c.filesAdded, float64(s.LastSnapshot.FilesAdded))
			tableGauge(c.filesRemoved, float64(s.LastSnapshot.FilesRemoved))
			tableGauge(c.bytesAdded, float64(s.LastSnapshot.BytesAdded))
			tableGauge(c.bytesRemoved, float64(s.LastSnapshot.BytesRemoved))
			tableGauge(c.recordsAdded, float64(s.LastSnapshot.RecordsAdded))
			tableGauge(c.recordsRemoved, float64(s.LastSnapshot.RecordsRemoved))
		}

		if !s.FileStatsCollected {
			continue
		}
		tableGauge(c.manifests, float64(s.ManifestCount))
		tableGauge(c.dataFiles, float64(s.DataFiles))
		tableGauge(c.dataBytes, float64(s.DataBytes))
		tableGauge(c.dataRecords, float64(s.DataRecords))
		tableGauge(c.deleteFiles, float64(s.DeleteFiles))
		tableGauge(c.deleteRecords, float64(s.DeleteRecords))
		labelGauge(c.deleteFilesByType, float64(s.PositionDeleteFiles), "position")
		labelGauge(c.deleteFilesByType, float64(s.EqualityDeleteFiles), "equality")
		labelGauge(c.deleteFilesByType, float64(s.DeletionVectors), "deletion_vector")
		labelGauge(c.deleteRecordsByType, float64(s.PositionDeleteRecords), "position")
		labelGauge(c.deleteRecordsByType, float64(s.EqualityDeleteRecords), "equality")
		labelGauge(c.deleteRecordsByType, float64(s.DeletionVectorRecords), "deletion_vector")
		ch <- prometheus.MustNewConstHistogram(c.fileSizes, s.FileSizes.Count, s.FileSizes.Sum, s.FileSizes.PromBuckets(), labels...)
		tableGauge(c.partitions, float64(s.Partitions))
		ch <- prometheus.MustNewConstHistogram(c.partitionFiles, s.PartitionFiles.Count, s.PartitionFiles.Sum, s.PartitionFiles.PromBuckets(), labels...)
		ch <- prometheus.MustNewConstHistogram(c.partitionBytes, s.PartitionBytes.Count, s.PartitionBytes.Sum, s.PartitionBytes.PromBuckets(), labels...)
		ch <- prometheus.MustNewConstHistogram(c.partitionRecords, s.PartitionRecords.Count, s.PartitionRecords.Sum, s.PartitionRecords.PromBuckets(), labels...)
		tableGauge(c.partitionFilesMax, float64(s.PartitionFilesMax))
		tableGauge(c.partitionBytesMax, float64(s.PartitionBytesMax))
		tableGauge(c.partitionTrackingFailed, boolFloat(s.PartitionTrackingTruncated))
	}

	gauge(c.duplicateTableLabels, float64(duplicates))
}

func msToSeconds(ms int64) float64 { return float64(ms) / 1000 }

func boolFloat(v bool) float64 {
	if v {
		return 1
	}
	return 0
}
