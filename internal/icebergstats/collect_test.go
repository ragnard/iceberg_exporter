package icebergstats

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	icebergio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
)

func TestHistogram(t *testing.T) {
	h := NewHistogram([]float64{1, 5, 10})
	for _, v := range []float64{0.5, 3, 7, 20} {
		h.Observe(v)
	}
	if h.Count != 4 {
		t.Errorf("Count = %d, want 4", h.Count)
	}
	if h.Sum != 30.5 {
		t.Errorf("Sum = %v, want 30.5", h.Sum)
	}
	want := map[float64]uint64{1: 1, 5: 2, 10: 3}
	for ub, count := range want {
		if got := h.PromBuckets()[ub]; got != count {
			t.Errorf("bucket <= %v = %d, want %d", ub, got, count)
		}
	}

	other := NewHistogram([]float64{1, 5, 10})
	other.Observe(2)
	h.Merge(other)
	if h.Count != 5 || h.Sum != 32.5 {
		t.Errorf("after merge Count/Sum = %d/%v, want 5/32.5", h.Count, h.Sum)
	}
	if got := h.PromBuckets()[1]; got != 1 {
		t.Errorf("merged bucket <= 1 = %d, want 1", got)
	}
	if got := h.PromBuckets()[5]; got != 3 {
		t.Errorf("merged bucket <= 5 = %d, want 3", got)
	}
	if got := h.PromBuckets()[10]; got != 4 {
		t.Errorf("merged bucket <= 10 = %d, want 4", got)
	}
}

func TestPartitionKey(t *testing.T) {
	sp := &specPartition{
		id: 7,
		fields: []partitionField{
			{id: 1000, name: "id"},
			{id: 1001, name: "ds"},
		},
	}
	got := sp.key(map[int]any{1000: "a", 1001: int32(5)})
	want := `7|1000="a";1001=5;`
	if got != want {
		t.Errorf("key = %q, want %q", got, want)
	}

	got = sp.key(map[int]any{1000: []byte{0xde, 0xad}, 1001: nil})
	want = `7|1000=0xdead;1001=null;`
	if got != want {
		t.Errorf("key = %q, want %q", got, want)
	}
}

func TestAccumulator(t *testing.T) {
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{1},
		FieldID:   1000,
		Name:      "id",
		Transform: iceberg.IdentityTransform{},
	})

	file := func(content iceberg.ManifestEntryContent, part any, records, size int64) iceberg.DataFile {
		t.Helper()
		b, err := iceberg.NewDataFileBuilder(spec, content, "s3://bucket/file.parquet", iceberg.ParquetFile,
			map[int]any{1000: part}, nil, nil, records, size)
		if err != nil {
			t.Fatal(err)
		}
		return b.Build()
	}

	acc := newAccumulator(2)
	acc.smallFileBytes = 150
	acc.largeFileBytes = 250

	acc.addDataFile(file(iceberg.EntryContentData, "a", 10, 100), `0|1000="a";`)
	acc.addDataFile(file(iceberg.EntryContentData, "a", 20, 200), `0|1000="a";`)
	acc.addDataFile(file(iceberg.EntryContentData, "b", 30, 300), `0|1000="b";`)
	acc.addDeleteFile(file(iceberg.EntryContentPosDeletes, "a", 2, 50))

	s := NewTableStats()
	acc.finalize(s)
	if s.DataFiles != 3 || s.DataBytes != 600 || s.DataRecords != 60 {
		t.Errorf("data totals = %d/%d/%d, want 3/600/60", s.DataFiles, s.DataBytes, s.DataRecords)
	}
	if s.DeleteFiles != 1 || s.DeleteRecords != 2 {
		t.Errorf("delete totals = %d/%d, want 1/2", s.DeleteFiles, s.DeleteRecords)
	}
	if s.SmallFiles != 1 {
		t.Errorf("SmallFiles = %d, want 1", s.SmallFiles)
	}
	if s.LargeFiles != 1 {
		t.Errorf("LargeFiles = %d, want 1", s.LargeFiles)
	}
	if s.Partitions != 2 {
		t.Errorf("Partitions = %d, want 2", s.Partitions)
	}
	if s.PartitionFilesMax != 2 {
		t.Errorf("PartitionFilesMax = %d, want 2", s.PartitionFilesMax)
	}
	if s.PartitionBytesMax != 300 {
		t.Errorf("PartitionBytesMax = %d, want 300", s.PartitionBytesMax)
	}
	if s.PartitionTrackingTruncated {
		t.Error("PartitionTrackingTruncated = true, want false")
	}
	if s.FileSizes.Count != 3 {
		t.Errorf("FileSizes.Count = %d, want 3", s.FileSizes.Count)
	}
	if s.PartitionFiles.Count != 2 {
		t.Errorf("PartitionFiles.Count = %d, want 2", s.PartitionFiles.Count)
	}

	// A third distinct partition exceeds maxTracked=2: totals still advance,
	// but the distribution is truncated.
	acc.addDataFile(file(iceberg.EntryContentData, "c", 40, 100), `0|1000="c";`)
	s = NewTableStats()
	acc.finalize(s)
	if s.DataFiles != 4 {
		t.Errorf("DataFiles = %d, want 4", s.DataFiles)
	}
	if s.Partitions != 2 {
		t.Errorf("Partitions = %d, want 2 after truncation", s.Partitions)
	}
	if !s.PartitionTrackingTruncated {
		t.Error("PartitionTrackingTruncated = false, want true")
	}
}

func TestCollectMetadataOnly(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)
	md, err := table.NewMetadata(schema, iceberg.UnpartitionedSpec, table.UnsortedSortOrder,
		"file:///tmp/table", iceberg.Properties{
			"write.target-file-size-bytes": "1048576",
		})
	if err != nil {
		t.Fatal(err)
	}
	tbl := table.New(table.Identifier{"db", "tbl"}, md, "file:///tmp/table/metadata.json",
		func(context.Context) (icebergio.IO, error) {
			return nil, errors.New("filesystem should not be opened")
		}, nil)

	cat := &fakeCatalog{tbl: tbl}
	s, err := Collect(context.Background(), cat, table.Identifier{"db", "tbl"}, Options{
		Catalog:              "prod",
		Manifests:            true,
		MaxTrackedPartitions: 100,
		SmallFileBytes:       64 << 20,
		TargetFileBytes:      512 << 20,
	})
	if err != nil {
		t.Fatalf("Collect() error = %v", err)
	}
	if s.Catalog != "prod" || s.Namespace != "db" || s.Table != "tbl" {
		t.Errorf("identity = %q/%q/%q, want prod/db/tbl", s.Catalog, s.Namespace, s.Table)
	}
	if s.FormatVersion != 2 {
		t.Errorf("FormatVersion = %d, want 2", s.FormatVersion)
	}
	if s.SchemaCount != 1 || s.PartitionSpecCount != 1 {
		t.Errorf("schema/spec counts = %d/%d, want 1/1", s.SchemaCount, s.PartitionSpecCount)
	}
	if s.Partitioned {
		t.Error("Partitioned = true, want false")
	}
	if s.TargetFileSizeBytes != 1048576 {
		t.Errorf("TargetFileSizeBytes = %d, want 1048576", s.TargetFileSizeBytes)
	}
	if s.HasCurrentSnapshot {
		t.Error("HasCurrentSnapshot = true, want false")
	}
	// Without a snapshot the table has no files: file metrics are zero,
	// not missing, and the implicit partition still counts.
	if !s.FileStatsCollected {
		t.Error("FileStatsCollected = false, want true (zeros) without a snapshot")
	}
	if s.DataFiles != 0 || s.DataBytes != 0 || s.ManifestCount != 0 || s.FileSizes.Count != 0 {
		t.Errorf("file totals = %d/%d/%d/%d, want zeros", s.DataFiles, s.DataBytes, s.ManifestCount, s.FileSizes.Count)
	}
	if s.Partitions != 1 {
		t.Errorf("Partitions = %d, want 1 for an unpartitioned table", s.Partitions)
	}

	s, err = Collect(context.Background(), cat, table.Identifier{"db", "tbl"}, Options{Manifests: false})
	if err != nil {
		t.Fatalf("Collect(manifests disabled) error = %v", err)
	}
	if s.FileStatsCollected {
		t.Error("FileStatsCollected = true with manifests disabled")
	}
}

type fakeCatalog struct {
	catalog.Catalog
	tbl    *table.Table
	onLoad func()
}

func (f *fakeCatalog) CatalogType() catalog.Type { return catalog.REST }

func (f *fakeCatalog) LoadTable(context.Context, table.Identifier) (*table.Table, error) {
	if f.onLoad != nil {
		f.onLoad()
	}
	return f.tbl, nil
}

func TestCollectWithManifests(t *testing.T) {
	snapshotID := int64(100)
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)
	spec := *iceberg.UnpartitionedSpec

	dataFile := func(content iceberg.ManifestEntryContent, records, size int64, path string) iceberg.DataFile {
		t.Helper()
		b, err := iceberg.NewDataFileBuilder(spec, content, path, iceberg.ParquetFile, nil, nil, nil, records, size)
		if err != nil {
			t.Fatal(err)
		}
		return b.Build()
	}
	entry := func(df iceberg.DataFile) iceberg.ManifestEntry {
		return iceberg.NewManifestEntryBuilder(iceberg.EntryStatusADDED, &snapshotID, df).
			SequenceNum(1).FileSequenceNum(1).Build()
	}

	var dataBuf bytes.Buffer
	dataWriter, err := iceberg.NewManifestWriter(2, &dataBuf, spec, schema, snapshotID)
	if err != nil {
		t.Fatal(err)
	}
	if err := dataWriter.Add(entry(dataFile(iceberg.EntryContentData, 10, 100, "mem://bucket/a.parquet"))); err != nil {
		t.Fatal(err)
	}
	if err := dataWriter.Add(entry(dataFile(iceberg.EntryContentData, 20, 200, "mem://bucket/b.parquet"))); err != nil {
		t.Fatal(err)
	}
	if err := dataWriter.Close(); err != nil {
		t.Fatal(err)
	}
	dataManifest, err := dataWriter.ToManifestFile("mem://bucket/data.avro", int64(dataBuf.Len()))
	if err != nil {
		t.Fatal(err)
	}

	var deleteBuf bytes.Buffer
	deleteWriter, err := iceberg.NewManifestWriter(2, &deleteBuf, spec, schema, snapshotID,
		iceberg.WithManifestWriterContent(iceberg.ManifestContentDeletes))
	if err != nil {
		t.Fatal(err)
	}
	if err := deleteWriter.Add(entry(dataFile(iceberg.EntryContentPosDeletes, 2, 50, "mem://bucket/del.parquet"))); err != nil {
		t.Fatal(err)
	}
	if err := deleteWriter.Close(); err != nil {
		t.Fatal(err)
	}
	deleteManifest, err := deleteWriter.ToManifestFile("mem://bucket/delete.avro", int64(deleteBuf.Len()))
	if err != nil {
		t.Fatal(err)
	}

	var listBuf bytes.Buffer
	listWriter, err := iceberg.NewManifestListWriterV2(&listBuf, snapshotID, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := listWriter.AddManifests([]iceberg.ManifestFile{dataManifest, deleteManifest}); err != nil {
		t.Fatal(err)
	}
	if err := listWriter.Close(); err != nil {
		t.Fatal(err)
	}

	base, err := table.NewMetadata(schema, iceberg.UnpartitionedSpec, table.UnsortedSortOrder,
		"mem://bucket/table", iceberg.Properties{"format-version": "2"})
	if err != nil {
		t.Fatal(err)
	}
	builder, err := table.MetadataBuilderFromBase(base, "")
	if err != nil {
		t.Fatal(err)
	}
	snap := table.Snapshot{
		SnapshotID:     snapshotID,
		SequenceNumber: 1,
		TimestampMs:    time.Now().UnixMilli(),
		ManifestList:   "mem://bucket/list.avro",
		Summary:        &table.Summary{Operation: table.OpAppend},
	}
	if err := builder.AddSnapshot(&snap); err != nil {
		t.Fatal(err)
	}
	if err := builder.SetSnapshotRef(table.MainBranch, snapshotID, table.BranchRef); err != nil {
		t.Fatal(err)
	}
	md, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}

	mem := icebergio.NewMemFS()
	for name, data := range map[string][]byte{
		"mem://bucket/data.avro":   dataBuf.Bytes(),
		"mem://bucket/delete.avro": deleteBuf.Bytes(),
		"mem://bucket/list.avro":   listBuf.Bytes(),
	} {
		if err := mem.WriteFile(name, data); err != nil {
			t.Fatal(err)
		}
	}

	// The load lock must be held during LoadTable and released before the
	// table filesystem is opened for manifest reads.
	var loadLock sync.Mutex
	var heldDuringLoad, heldDuringFS bool
	var fsOpens int
	tbl := table.New(table.Identifier{"db", "tbl"}, md, "mem://bucket/metadata.json",
		func(context.Context) (icebergio.IO, error) {
			fsOpens++
			heldDuringFS = !loadLock.TryLock()
			if !heldDuringFS {
				loadLock.Unlock()
			}
			return mem, nil
		}, nil)
	cat := &fakeCatalog{tbl: tbl, onLoad: func() {
		heldDuringLoad = !loadLock.TryLock()
		if !heldDuringLoad {
			loadLock.Unlock()
		}
	}}

	opts := Options{
		Catalog:              "prod",
		Manifests:            true,
		ManifestConcurrency:  2,
		MaxTrackedPartitions: 100,
		SmallFileBytes:       150,
		LargeFileBytes:       250,
		LoadLock:             &loadLock,
	}
	s, err := Collect(context.Background(), cat, table.Identifier{"db", "tbl"}, opts)
	if err != nil {
		t.Fatalf("Collect() error = %v", err)
	}
	if s.FileStatsReused || fsOpens != 1 {
		t.Errorf("first collection: reused/fs opens = %v/%d, want false/1", s.FileStatsReused, fsOpens)
	}
	if !heldDuringLoad || heldDuringFS {
		t.Errorf("load lock held during load/fs = %v/%v, want true/false", heldDuringLoad, heldDuringFS)
	}
	if !s.FileStatsCollected || s.ManifestCount != 2 {
		t.Errorf("file stats collected/manifests = %v/%d, want true/2", s.FileStatsCollected, s.ManifestCount)
	}
	if s.DataFiles != 2 || s.DataBytes != 300 || s.DataRecords != 30 {
		t.Errorf("data totals = %d/%d/%d, want 2/300/30", s.DataFiles, s.DataBytes, s.DataRecords)
	}
	if s.DeleteFiles != 1 || s.DeleteRecords != 2 {
		t.Errorf("delete totals = %d/%d, want 1/2", s.DeleteFiles, s.DeleteRecords)
	}
	if s.SmallFiles != 1 || s.LargeFiles != 0 {
		t.Errorf("small/large = %d/%d, want 1/0", s.SmallFiles, s.LargeFiles)
	}
	if s.Partitions != 1 {
		t.Errorf("Partitions = %d, want 1", s.Partitions)
	}
	if s.FileSizes.Count != 2 || s.FileSizes.Sum != 300 {
		t.Errorf("file size histogram = %d/%v, want 2/300", s.FileSizes.Count, s.FileSizes.Sum)
	}
	if !s.HasCurrentSnapshot || s.CurrentSnapshotID != snapshotID {
		t.Errorf("current snapshot = %v/%d, want true/%d", s.HasCurrentSnapshot, s.CurrentSnapshotID, snapshotID)
	}

	testFileStatsReuse(t, cat, opts, s, &fsOpens)
}

// testFileStatsReuse checks that manifest-derived stats are reused only when
// the snapshot, table UUID and thresholds are unchanged.
func testFileStatsReuse(t *testing.T, cat catalog.Catalog, opts Options, first *TableStats, fsOpens *int) {
	t.Helper()
	ident := table.Identifier{"db", "tbl"}

	// Unchanged: no filesystem access, identical file stats.
	opts.Previous = first
	opens := *fsOpens
	s, err := Collect(context.Background(), cat, ident, opts)
	if err != nil {
		t.Fatalf("Collect(unchanged) error = %v", err)
	}
	if !s.FileStatsReused || *fsOpens != opens {
		t.Errorf("unchanged: reused/fs opens = %v/%d, want true/%d", s.FileStatsReused, *fsOpens, opens)
	}
	if s.DataFiles != first.DataFiles || s.DataBytes != first.DataBytes || s.ManifestCount != first.ManifestCount ||
		s.Partitions != first.Partitions || s.FileSizes.Count != first.FileSizes.Count || !s.FileStatsCollected {
		t.Errorf("unchanged: reused stats differ from the first collection")
	}

	// A reused result can itself be reused on the next refresh.
	opts.Previous = s
	if s2, err := Collect(context.Background(), cat, ident, opts); err != nil || !s2.FileStatsReused {
		t.Errorf("chained reuse: reused = %v, err = %v; want true, nil", s2 != nil && s2.FileStatsReused, err)
	}

	recollects := func(name string, prev *TableStats, opts Options) {
		t.Helper()
		opts.Previous = prev
		opens := *fsOpens
		s, err := Collect(context.Background(), cat, ident, opts)
		if err != nil {
			t.Fatalf("Collect(%s) error = %v", name, err)
		}
		if s.FileStatsReused || *fsOpens != opens+1 {
			t.Errorf("%s: reused/fs opens = %v/%d, want false/%d", name, s.FileStatsReused, *fsOpens, opens+1)
		}
	}
	changed := func(mutate func(*fileStatsKey)) *TableStats {
		prev := *first
		mutate(&prev.fileStatsKey)
		return &prev
	}
	recollects("different snapshot", changed(func(k *fileStatsKey) { k.snapshotID++ }), opts)
	recollects("recreated table", changed(func(k *fileStatsKey) { k.tableUUID = "other" }), opts)
	smaller := opts
	smaller.SmallFileBytes++
	recollects("changed small-file threshold", first, smaller)
	larger := opts
	larger.LargeFileBytes++
	recollects("changed large-file threshold", first, larger)
	tracked := opts
	tracked.MaxTrackedPartitions++
	recollects("changed max tracked partitions", first, tracked)
	recollects("previous without file stats", &TableStats{}, opts)
}
