package icebergstats

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	icebergio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
)

func TestCollectHealth(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)
	base, err := table.NewMetadata(schema, iceberg.UnpartitionedSpec, table.UnsortedSortOrder,
		"file:///tmp/table", iceberg.Properties{
			"history.expire.max-snapshot-age-ms":         "86400000", // overridden by the main ref below
			"history.expire.min-snapshots-to-keep":       "not-a-number",
			"write.metadata.previous-versions-max":       "10",
			"write.metadata.delete-after-commit.enabled": "TRUE",
			"gc.enabled":                    "false",
			"commit.manifest-merge.enabled": "garbage",
		})
	if err != nil {
		t.Fatal(err)
	}
	builder, err := table.MetadataBuilderFromBase(base, "file:///tmp/table/metadata/v1.metadata.json")
	if err != nil {
		t.Fatal(err)
	}

	// The builder requires snapshot timestamps after the metadata's
	// last-updated time, so offsets are positive and increasing.
	now := time.Now()
	snaps := []struct {
		id     int64
		offset time.Duration
		op     table.Operation
	}{
		{1, 1 * time.Second, table.OpAppend},
		{2, 2 * time.Second, table.OpReplace},
		{3, 3 * time.Second, table.OpAppend},
	}
	for i, sn := range snaps {
		snap := table.Snapshot{
			SnapshotID:     sn.id,
			SequenceNumber: int64(i + 1),
			TimestampMs:    now.Add(sn.offset).UnixMilli(),
			ManifestList:   "file:///tmp/table/list.avro",
			Summary:        &table.Summary{Operation: sn.op},
		}
		if i > 0 {
			parent := snaps[i-1].id
			snap.ParentSnapshotID = &parent
		}
		if err := builder.AddSnapshot(&snap); err != nil {
			t.Fatal(err)
		}
	}
	week := int64(7 * 24 * time.Hour / time.Millisecond)
	if err := builder.SetSnapshotRef(table.MainBranch, 3, table.BranchRef, table.WithMaxSnapshotAgeMs(week)); err != nil {
		t.Fatal(err)
	}
	if err := builder.SetSnapshotRef("audit", 1, table.BranchRef); err != nil {
		t.Fatal(err)
	}
	if err := builder.SetSnapshotRef("v1", 2, table.TagRef); err != nil {
		t.Fatal(err)
	}
	if err := builder.SetStatistics(table.StatisticsFile{SnapshotID: 3, StatisticsPath: "file:///tmp/table/stats.puffin"}); err != nil {
		t.Fatal(err)
	}
	md, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}

	tbl := table.New(table.Identifier{"db", "tbl"}, md, "file:///tmp/table/metadata/v2.metadata.json",
		func(context.Context) (icebergio.IO, error) { return nil, errors.New("no fs") }, nil)
	s, err := Collect(context.Background(), &fakeCatalog{tbl: tbl}, table.Identifier{"db", "tbl"}, Options{Manifests: false})
	if err != nil {
		t.Fatalf("Collect() error = %v", err)
	}

	if s.Branches != 2 || s.Tags != 1 {
		t.Errorf("branches/tags = %d/%d, want 2/1", s.Branches, s.Tags)
	}
	if !s.HasOldestBranch || s.OldestBranchSnapshotMs != md.SnapshotByID(1).TimestampMs {
		t.Errorf("oldest branch = %v/%d, want snapshot 1 (main excluded)", s.HasOldestBranch, s.OldestBranchSnapshotMs)
	}
	if !s.HasOldestTag || s.OldestTagSnapshotMs != md.SnapshotByID(2).TimestampMs {
		t.Errorf("oldest tag = %v/%d, want snapshot 2", s.HasOldestTag, s.OldestTagSnapshotMs)
	}
	if got, want := s.LastOperationMs[string(table.OpAppend)], md.SnapshotByID(3).TimestampMs; got != want {
		t.Errorf("last append = %d, want %d", got, want)
	}
	if got, want := s.LastOperationMs[string(table.OpReplace)], md.SnapshotByID(2).TimestampMs; got != want {
		t.Errorf("last replace = %d, want %d", got, want)
	}
	if _, ok := s.LastOperationMs[string(table.OpDelete)]; ok {
		t.Error("last delete present without a delete snapshot")
	}
	if s.StatisticsFiles != 1 || !s.CurrentSnapshotHasStatistics || s.PartitionStatisticsFiles != 0 {
		t.Errorf("statistics = %d/%v/%d, want 1/true/0", s.StatisticsFiles, s.CurrentSnapshotHasStatistics, s.PartitionStatisticsFiles)
	}
	if s.MetadataLogEntries != 1 {
		t.Errorf("MetadataLogEntries = %d, want 1", s.MetadataLogEntries)
	}

	want := TableSettings{
		SnapshotMaxAgeMs:            week,
		MinSnapshotsToKeep:          DefaultMinSnapshotsToKeep,
		MetadataPreviousVersionsMax: 10,
		MetadataDeleteAfterCommit:   true,
		GCEnabled:                   false,
		ManifestMergeEnabled:        DefaultManifestMergeEnabled,
		TargetFileSizeBytes:         DefaultTargetFileSizeBytes,
	}
	if s.Settings != want {
		t.Errorf("Settings = %+v, want %+v", s.Settings, want)
	}
}

func TestTableSettingsDefaults(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)
	md, err := table.NewMetadata(schema, iceberg.UnpartitionedSpec, table.UnsortedSortOrder, "file:///tmp/table", nil)
	if err != nil {
		t.Fatal(err)
	}
	want := TableSettings{
		SnapshotMaxAgeMs:            DefaultSnapshotMaxAgeMs,
		MinSnapshotsToKeep:          DefaultMinSnapshotsToKeep,
		MetadataPreviousVersionsMax: DefaultMetadataPreviousVersionsMax,
		MetadataDeleteAfterCommit:   DefaultMetadataDeleteAfterCommit,
		GCEnabled:                   DefaultGCEnabled,
		ManifestMergeEnabled:        DefaultManifestMergeEnabled,
		TargetFileSizeBytes:         DefaultTargetFileSizeBytes,
	}
	if got := tableSettings(md); got != want {
		t.Errorf("tableSettings() = %+v, want %+v", got, want)
	}
}

func TestAccumulatorDeleteSplit(t *testing.T) {
	spec := *iceberg.UnpartitionedSpec
	build := func(content iceberg.ManifestEntryContent, format iceberg.FileFormat, records int64) iceberg.DataFile {
		t.Helper()
		b, err := iceberg.NewDataFileBuilder(spec, content, "mem://bucket/f", format, nil, nil, nil, records, 100)
		if err != nil {
			t.Fatal(err)
		}
		return b.Build()
	}
	acc := newAccumulator(10)
	acc.addDeleteFile(build(iceberg.EntryContentPosDeletes, iceberg.ParquetFile, 1))
	acc.addDeleteFile(build(iceberg.EntryContentPosDeletes, iceberg.ParquetFile, 2))
	acc.addDeleteFile(build(iceberg.EntryContentEqDeletes, iceberg.ParquetFile, 4))
	acc.addDeleteFile(build(iceberg.EntryContentPosDeletes, iceberg.PuffinFile, 8))
	s := NewTableStats()
	acc.finalize(s)

	if s.DeleteFiles != 4 || s.DeleteRecords != 15 {
		t.Errorf("delete totals = %d/%d, want 4/15", s.DeleteFiles, s.DeleteRecords)
	}
	if s.PositionDeleteFiles != 2 || s.PositionDeleteRecords != 3 {
		t.Errorf("position = %d/%d, want 2/3", s.PositionDeleteFiles, s.PositionDeleteRecords)
	}
	if s.EqualityDeleteFiles != 1 || s.EqualityDeleteRecords != 4 {
		t.Errorf("equality = %d/%d, want 1/4", s.EqualityDeleteFiles, s.EqualityDeleteRecords)
	}
	if s.DeletionVectors != 1 || s.DeletionVectorRecords != 8 {
		t.Errorf("deletion vectors = %d/%d, want 1/8", s.DeletionVectors, s.DeletionVectorRecords)
	}
}
