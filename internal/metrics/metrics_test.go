package metrics

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/ragnard/iceberg_exporter/internal/icebergstats"
	"github.com/ragnard/iceberg_exporter/internal/scrape"
)

type staticState struct {
	st *scrape.State
}

func (s staticState) State() *scrape.State { return s.st }

func gather(t *testing.T, c *Collector) map[string]*dto.MetricFamily {
	t.Helper()
	reg := prometheus.NewRegistry()
	reg.MustRegister(c)
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	out := make(map[string]*dto.MetricFamily, len(mfs))
	for _, mf := range mfs {
		out[mf.GetName()] = mf
	}
	return out
}

func gaugeValue(t *testing.T, mfs map[string]*dto.MetricFamily, name string) float64 {
	t.Helper()
	mf, ok := mfs[name]
	if !ok {
		t.Fatalf("metric %q not found", name)
	}
	if len(mf.Metric) != 1 {
		t.Fatalf("metric %q has %d samples, want 1", name, len(mf.Metric))
	}
	return mf.Metric[0].GetGauge().GetValue()
}

func TestCollectorRendersTableStats(t *testing.T) {
	fileSizes := icebergstats.NewHistogram(icebergstats.FileSizeBounds)
	fileSizes.Observe(100)
	fileSizes.Observe(200)
	fileSizes.Observe(300)
	partitionFiles := icebergstats.NewHistogram(icebergstats.PartitionFilesBounds)
	partitionFiles.Observe(2)
	partitionFiles.Observe(1)

	now := time.Now()
	st := &scrape.State{
		Started:    now.Add(-2 * time.Second),
		Finished:   now,
		Duration:   2 * time.Second,
		Success:    true,
		Discovered: 1,
		Collected:  1,
		Tables: []scrape.TableResult{
			{
				Ref:         scrape.TableRef{Catalog: "prod", Namespace: "db", Table: "tbl"},
				Success:     true,
				LastSuccess: now,
				Stats: &icebergstats.TableStats{
					Catalog:                    "prod",
					Namespace:                  "db",
					Table:                      "tbl",
					FormatVersion:              2,
					Partitioned:                true,
					SchemaCount:                3,
					PartitionSpecCount:         2,
					HasCurrentSnapshot:         true,
					CurrentSnapshotID:          123,
					CurrentSnapshotTimestampMs: now.Add(-time.Hour).UnixMilli(),
					LastUpdatedMs:              now.Add(-30 * time.Minute).UnixMilli(),
					SnapshotCount:              4,
					HasSnapshots:               true,
					OldestSnapshotTimestampMs:  now.Add(-24 * time.Hour).UnixMilli(),
					LastSequenceNumber:         9,
					HasLastSnapshotSummary:     true,
					LastSnapshot: icebergstats.SnapshotSummary{
						FilesAdded: 2, FilesRemoved: 1, BytesAdded: 400, BytesRemoved: 100,
						RecordsAdded: 20, RecordsRemoved: 10,
					},
					ManifestCount:      2,
					FileStatsCollected: true,
					DataFiles:          3,
					DataBytes:          600,
					DataRecords:        60,
					DeleteFiles:        1,
					DeleteRecords:      2,
					FileSizes:          fileSizes,
					Partitions:         2,
					PartitionFiles:     partitionFiles,
					PartitionFilesMax:  2,
					PartitionBytesMax:  300,
				},
			},
		},
	}

	c := NewCollector(staticState{st: st}, BuildInfo{
		Version:      "test",
		Revision:     "abc",
		Catalogs:     []string{"hive", "rest"},
		ObjectStores: []string{"s3"},
	})
	mfs := gather(t, c)

	if got := gaugeValue(t, mfs, "iceberg_exporter_build_info"); got != 1 {
		t.Errorf("build_info = %v, want 1", got)
	}
	if got := gaugeValue(t, mfs, "iceberg_exporter_tables_discovered"); got != 1 {
		t.Errorf("tables_discovered = %v, want 1", got)
	}
	if got := gaugeValue(t, mfs, "iceberg_table_data_files"); got != 3 {
		t.Errorf("data_files = %v, want 3", got)
	}
	if got := gaugeValue(t, mfs, "iceberg_table_data_bytes"); got != 600 {
		t.Errorf("data_bytes = %v, want 600", got)
	}
	if got := gaugeValue(t, mfs, "iceberg_table_partitions"); got != 2 {
		t.Errorf("partitions = %v, want 2", got)
	}
	if got := gaugeValue(t, mfs, "iceberg_table_partition_files_max"); got != 2 {
		t.Errorf("partition_files_max = %v, want 2", got)
	}
	if got := gaugeValue(t, mfs, "iceberg_table_scrape_success"); got != 1 {
		t.Errorf("table scrape_success = %v, want 1", got)
	}
	if got := gaugeValue(t, mfs, "iceberg_table_last_snapshot_files_added"); got != 2 {
		t.Errorf("last_snapshot_files_added = %v, want 2", got)
	}

	mf := mfs["iceberg_table_file_size_bytes"]
	if mf == nil || len(mf.Metric) != 1 {
		t.Fatal("file_size_bytes histogram missing")
	}
	if got := mf.Metric[0].GetHistogram().GetSampleCount(); got != 3 {
		t.Errorf("file_size_bytes sample count = %d, want 3", got)
	}
	if got := mf.Metric[0].GetHistogram().GetSampleSum(); got != 600 {
		t.Errorf("file_size_bytes sample sum = %v, want 600", got)
	}
	labels := map[string]string{}
	for _, lp := range mf.Metric[0].Label {
		labels[lp.GetName()] = lp.GetValue()
	}
	if labels["catalog"] != "prod" || labels["namespace"] != "db" || labels["table"] != "tbl" {
		t.Errorf("unexpected labels: %v", labels)
	}
}

func TestCollectorRetainsStatsForFailedTable(t *testing.T) {
	now := time.Now()
	st := &scrape.State{
		Finished:        now,
		Success:         true,
		Discovered:      1,
		Failing:         1,
		FileStatsReused: 1,
		Tables: []scrape.TableResult{
			{
				Ref:         scrape.TableRef{Catalog: "prod", Namespace: "db", Table: "tbl"},
				Success:     false,
				LastSuccess: now.Add(-time.Hour),
				Stats: &icebergstats.TableStats{
					Catalog:            "prod",
					Namespace:          "db",
					Table:              "tbl",
					FileStatsCollected: true,
					DataFiles:          5,
				},
			},
		},
	}
	c := NewCollector(staticState{st: st}, BuildInfo{Version: "test"})
	mfs := gather(t, c)

	if got := gaugeValue(t, mfs, "iceberg_table_scrape_success"); got != 0 {
		t.Errorf("table scrape_success = %v, want 0", got)
	}
	if got := gaugeValue(t, mfs, "iceberg_table_data_files"); got != 5 {
		t.Errorf("data_files = %v, want retained value 5", got)
	}
	if got := gaugeValue(t, mfs, "iceberg_exporter_tables_file_stats_reused"); got != 1 {
		t.Errorf("tables_file_stats_reused = %v, want 1", got)
	}
	if got := gaugeValue(t, mfs, "iceberg_exporter_tables_failing"); got != 1 {
		t.Errorf("tables_failing = %v, want 1", got)
	}
}

func TestCollectorDeduplicatesLabelCollisions(t *testing.T) {
	now := time.Now()
	stats := &icebergstats.TableStats{
		Catalog:            "prod",
		Namespace:          "a.b",
		Table:              "tbl",
		FileStatsCollected: true,
		DataFiles:          5,
	}
	st := &scrape.State{
		Finished:   now,
		Success:    true,
		Discovered: 2,
		Collected:  2,
		Tables: []scrape.TableResult{
			{Ref: scrape.TableRef{Catalog: "prod", Namespace: "a.b", Table: "tbl"}, Success: true, Stats: stats},
			{Ref: scrape.TableRef{Catalog: "prod", Namespace: "a.b", Table: "tbl"}, Success: true, Stats: stats},
		},
	}
	c := NewCollector(staticState{st: st}, BuildInfo{Version: "test"})
	mfs := gather(t, c)

	if got := gaugeValue(t, mfs, "iceberg_exporter_duplicate_table_labels"); got != 1 {
		t.Errorf("duplicate_table_labels = %v, want 1", got)
	}
	mf := mfs["iceberg_table_data_files"]
	if mf == nil || len(mf.Metric) != 1 {
		t.Fatalf("data_files series = %d, want 1", len(mf.GetMetric()))
	}
	if got := mf.Metric[0].GetGauge().GetValue(); got != 5 {
		t.Errorf("data_files = %v, want 5", got)
	}
}

func TestCollectorBeforeFirstRefresh(t *testing.T) {
	c := NewCollector(staticState{}, BuildInfo{Version: "test"})
	mfs := gather(t, c)
	if got := gaugeValue(t, mfs, "iceberg_exporter_build_info"); got != 1 {
		t.Errorf("build_info = %v, want 1", got)
	}
	if _, ok := mfs["iceberg_exporter_last_scrape_timestamp_seconds"]; ok {
		t.Error("scrape metrics were emitted before the first refresh")
	}
}
