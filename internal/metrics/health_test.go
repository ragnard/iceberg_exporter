package metrics

import (
	"os"
	"regexp"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/ragnard/iceberg_exporter/internal/icebergstats"
	"github.com/ragnard/iceberg_exporter/internal/scrape"
)

// labeledValue returns the gauge value of the sample whose label name has the
// given value.
func labeledValue(t *testing.T, mfs map[string]*dto.MetricFamily, name, label, value string) (float64, bool) {
	t.Helper()
	mf, ok := mfs[name]
	if !ok {
		return 0, false
	}
	for _, m := range mf.Metric {
		for _, lp := range m.Label {
			if lp.GetName() == label && lp.GetValue() == value {
				return m.GetGauge().GetValue(), true
			}
		}
	}
	return 0, false
}

func TestCollectorRendersHealthMetrics(t *testing.T) {
	now := time.Now()
	st := &scrape.State{
		Finished: now,
		Success:  true,
		Tables: []scrape.TableResult{{
			Ref:     scrape.TableRef{Catalog: "prod", Namespace: "db", Table: "tbl"},
			Success: true,
			Stats: &icebergstats.TableStats{
				Branches:                     2,
				Tags:                         1,
				HasOldestBranch:              true,
				OldestBranchSnapshotMs:       now.Add(-time.Hour).UnixMilli(),
				MetadataLogEntries:           7,
				LastOperationMs:              map[string]int64{"replace": 5000},
				StatisticsFiles:              2,
				PartitionStatisticsFiles:     1,
				CurrentSnapshotHasStatistics: true,
				Settings: icebergstats.TableSettings{
					SnapshotMaxAgeMs:            86400000,
					MinSnapshotsToKeep:          3,
					MetadataPreviousVersionsMax: 50,
					MetadataDeleteAfterCommit:   true,
					GCEnabled:                   false,
					ManifestMergeEnabled:        true,
				},
				FileStatsCollected:    true,
				PositionDeleteFiles:   4,
				EqualityDeleteFiles:   5,
				DeletionVectors:       6,
				EqualityDeleteRecords: 50,
				FileSizes:             icebergstats.NewHistogram(icebergstats.FileSizeBounds),
				PartitionFiles:        icebergstats.NewHistogram(icebergstats.PartitionFilesBounds),
				PartitionBytes:        icebergstats.NewHistogram(icebergstats.PartitionBytesBounds),
				PartitionRecords:      icebergstats.NewHistogram(icebergstats.PartitionRecordsBounds),
			},
		}},
	}
	mfs := gather(t, NewCollector(staticState{st: st}, BuildInfo{}))

	for name, want := range map[string]float64{
		"iceberg_table_metadata_log_entries":                 7,
		"iceberg_table_statistics_files":                     2,
		"iceberg_table_partition_statistics_files":           1,
		"iceberg_table_current_snapshot_has_statistics":      1,
		"iceberg_table_snapshot_max_age_seconds":             86400,
		"iceberg_table_min_snapshots_to_keep":                3,
		"iceberg_table_metadata_previous_versions_max":       50,
		"iceberg_table_metadata_delete_after_commit_enabled": 1,
		"iceberg_table_gc_enabled":                           0,
		"iceberg_table_manifest_merge_enabled":               1,
	} {
		if got := gaugeValue(t, mfs, name); got != want {
			t.Errorf("%s = %v, want %v", name, got, want)
		}
	}

	for _, tc := range []struct {
		name, label, value string
		want               float64
	}{
		{"iceberg_table_refs", "type", "branch", 2},
		{"iceberg_table_refs", "type", "tag", 1},
		{"iceberg_table_last_operation_timestamp_seconds", "operation", "replace", 5},
		{"iceberg_table_delete_files_by_type", "type", "position", 4},
		{"iceberg_table_delete_files_by_type", "type", "equality", 5},
		{"iceberg_table_delete_files_by_type", "type", "deletion_vector", 6},
		{"iceberg_table_delete_records_by_type", "type", "equality", 50},
	} {
		got, ok := labeledValue(t, mfs, tc.name, tc.label, tc.value)
		if !ok || got != tc.want {
			t.Errorf("%s{%s=%q} = %v (present %v), want %v", tc.name, tc.label, tc.value, got, ok, tc.want)
		}
	}

	if age, ok := labeledValue(t, mfs, "iceberg_table_oldest_ref_snapshot_age_seconds", "type", "branch"); !ok || age < 3500 || age > 3700 {
		t.Errorf("oldest branch ref age = %v (present %v), want ~3600", age, ok)
	}
	if _, ok := labeledValue(t, mfs, "iceberg_table_oldest_ref_snapshot_age_seconds", "type", "tag"); ok {
		t.Error("oldest tag ref age rendered without a non-main tag")
	}
	if _, ok := labeledValue(t, mfs, "iceberg_table_last_operation_timestamp_seconds", "operation", "append"); ok {
		t.Error("last append timestamp rendered without an append snapshot")
	}
}

// TestRulesReferenceKnownMetrics keeps prometheus/iceberg_exporter.rules.yml
// in sync with the metric descriptors defined in this package.
func TestRulesReferenceKnownMetrics(t *testing.T) {
	rules, err := os.ReadFile("../../prometheus/iceberg_exporter.rules.yml")
	if err != nil {
		t.Fatal(err)
	}

	known := map[string]bool{}
	ch := make(chan *prometheus.Desc, 256)
	NewCollector(staticState{}, BuildInfo{}).Describe(ch)
	close(ch)
	fqName := regexp.MustCompile(`fqName: "([^"]+)"`)
	for desc := range ch {
		if m := fqName.FindStringSubmatch(desc.String()); m != nil {
			known[m[1]] = true
		}
	}

	// Series produced by the rules themselves.
	recorded := map[string]bool{"iceberg_table_problem": true}

	ref := regexp.MustCompile(`\biceberg_(?:table|exporter)_[a-z0-9_]+`)
	seen := map[string]bool{}
	for _, name := range ref.FindAllString(string(rules), -1) {
		if seen[name] || recorded[name] {
			continue
		}
		seen[name] = true
		if !known[name] {
			t.Errorf("rules reference unknown metric %q", name)
		}
	}
	if len(seen) == 0 {
		t.Fatal("no metric references found in rules file")
	}
}
