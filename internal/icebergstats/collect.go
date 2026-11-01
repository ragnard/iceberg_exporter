package icebergstats

import (
	"context"
	"encoding/hex"
	"fmt"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	icebergio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"golang.org/x/sync/errgroup"

	"github.com/ragnard/iceberg_exporter/internal/registry"
)

// Options controls how much work Collect performs.
type Options struct {
	Catalog              string
	Manifests            bool
	ManifestConcurrency  int
	MaxTrackedPartitions int
	SmallFileBytes       int64
	LargeFileBytes       int64 // 0 means "2 * target file size"
	TargetFileBytes      int64

	// LoadLock, if set, is held only while the catalog loads the table.
	// Use it for catalog clients that are not safe for concurrent use;
	// manifest reads happen outside the lock.
	LoadLock sync.Locker

	// Previous is the table's last successfully collected stats, if any.
	// When its current snapshot and thresholds match, its manifest-derived
	// stats are reused and no manifest list or manifest is read.
	Previous *TableStats
}

func (o Options) withDefaults() Options {
	if o.ManifestConcurrency <= 0 {
		o.ManifestConcurrency = 1
	}
	if o.MaxTrackedPartitions <= 0 {
		o.MaxTrackedPartitions = 100_000
	}
	if o.TargetFileBytes <= 0 {
		o.TargetFileBytes = 512 << 20
	}
	return o
}

// Collect loads one table and builds its statistics.
func Collect(ctx context.Context, cat catalog.Catalog, ident table.Identifier, opts Options) (*TableStats, error) {
	opts = opts.withDefaults()
	if len(ident) == 0 {
		return nil, fmt.Errorf("empty table identifier")
	}

	if opts.LoadLock != nil {
		opts.LoadLock.Lock()
	}
	tbl, err := cat.LoadTable(ctx, ident)
	if opts.LoadLock != nil {
		opts.LoadLock.Unlock()
	}
	if err != nil {
		return nil, fmt.Errorf("load table: %w", err)
	}

	md := tbl.Metadata()
	s := NewTableStats()
	s.Catalog = opts.Catalog
	s.Namespace = strings.Join(ident[:len(ident)-1], ".")
	s.Table = ident[len(ident)-1]

	s.FormatVersion = md.Version()
	s.SchemaCount = len(md.Schemas())
	s.PartitionSpecCount = len(md.PartitionSpecs())
	currentSpec := md.PartitionSpec()
	s.Partitioned = hasNonVoidFields(&currentSpec)
	s.LastUpdatedMs = md.LastUpdatedMillis()
	s.LastSequenceNumber = md.LastSequenceNumber()
	s.SnapshotCount = len(md.Snapshots())
	for range md.Refs() {
		s.RefCount++
	}

	s.TargetFileSizeBytes = opts.TargetFileBytes
	if v, ok := md.Properties()["write.target-file-size-bytes"]; ok {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			s.TargetFileSizeBytes = n
		}
	}
	largeFileBytes := opts.LargeFileBytes
	if largeFileBytes <= 0 {
		largeFileBytes = 2 * s.TargetFileSizeBytes
	}

	for _, snap := range md.Snapshots() {
		if snap.TimestampMs <= 0 {
			continue
		}
		if !s.HasSnapshots || snap.TimestampMs < s.OldestSnapshotTimestampMs {
			s.HasSnapshots = true
			s.OldestSnapshotTimestampMs = snap.TimestampMs
		}
	}

	snap := md.CurrentSnapshot()
	if snap == nil {
		// A table without a current snapshot has no files. Report zeros
		// rather than omitting the file metrics.
		if opts.Manifests {
			acc := newAccumulator(opts.MaxTrackedPartitions)
			acc.unpartitioned = !s.Partitioned
			acc.finalize(s)
			s.FileStatsCollected = true
		}
		return s, nil
	}
	s.HasCurrentSnapshot = true
	s.CurrentSnapshotID = snap.SnapshotID
	s.CurrentSnapshotTimestampMs = snap.TimestampMs
	if snap.Summary != nil {
		s.HasLastSnapshotSummary = true
		s.LastSnapshot = SnapshotSummary{
			FilesAdded:     summaryInt(snap.Summary.Properties, "added-data-files"),
			FilesRemoved:   summaryInt(snap.Summary.Properties, "deleted-data-files", "removed-data-files"),
			BytesAdded:     summaryInt(snap.Summary.Properties, "added-files-size"),
			BytesRemoved:   summaryInt(snap.Summary.Properties, "removed-files-size", "deleted-files-size"),
			RecordsAdded:   summaryInt(snap.Summary.Properties, "added-records"),
			RecordsRemoved: summaryInt(snap.Summary.Properties, "deleted-records", "removed-records"),
		}
	}

	if !opts.Manifests {
		return s, nil
	}

	key := fileStatsKey{
		tableUUID:      md.TableUUID().String(),
		snapshotID:     snap.SnapshotID,
		smallFileBytes: opts.SmallFileBytes,
		largeFileBytes: largeFileBytes,
		maxTracked:     opts.MaxTrackedPartitions,
	}
	if prev := opts.Previous; prev != nil && prev.FileStatsCollected && prev.fileStatsKey == key {
		s.copyFileStats(prev)
		s.FileStatsReused = true
		return s, nil
	}

	fs, err := tbl.FS(ctx)
	if err != nil {
		return nil, wrapIOError(tbl.Location(), err)
	}
	if snap.ManifestList == "" && len(snap.ManifestLocations) == 0 {
		return nil, fmt.Errorf("current snapshot %d has no manifest list or manifests", snap.SnapshotID)
	}
	manifests, err := snap.Manifests(fs)
	if err != nil {
		return nil, fmt.Errorf("read manifest list: %w", err)
	}
	s.ManifestCount = int64(len(manifests))

	specs := buildSpecIndex(md)
	acc := newAccumulator(opts.MaxTrackedPartitions)
	acc.smallFileBytes = opts.SmallFileBytes
	acc.largeFileBytes = largeFileBytes
	acc.unpartitioned = !s.Partitioned

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(opts.ManifestConcurrency)
	for _, manifest := range manifests {
		manifest := manifest
		g.Go(func() error {
			return scanManifest(gctx, fs, manifest, specs, acc)
		})
	}
	if err := g.Wait(); err != nil {
		return nil, fmt.Errorf("read manifests: %w", err)
	}

	acc.finalize(s)
	s.FileStatsCollected = true
	s.fileStatsKey = key
	return s, nil
}

func wrapIOError(location string, err error) error {
	u, parseErr := url.Parse(location)
	if parseErr == nil {
		scheme := strings.ToLower(u.Scheme)
		if hint := registry.IOHint(scheme); hint != "" && !slices.Contains(registry.ObjectStoreSchemes(), scheme) {
			return fmt.Errorf("open table filesystem: %w (object store %q is not compiled in; rebuild with -tags %s)", err, scheme, hint)
		}
	}
	return fmt.Errorf("open table filesystem: %w", err)
}

func summaryInt(props iceberg.Properties, keys ...string) int64 {
	for _, key := range keys {
		v, ok := props[key]
		if !ok {
			continue
		}
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			return n
		}
	}
	return 0
}

type partitionField struct {
	id   int
	name string
}

type specPartition struct {
	id     int32
	fields []partitionField
}

func hasNonVoidFields(spec *iceberg.PartitionSpec) bool {
	for _, field := range spec.Fields() {
		if _, void := field.Transform.(iceberg.VoidTransform); !void {
			return true
		}
	}
	return false
}

func buildSpecIndex(md table.Metadata) map[int32]*specPartition {
	specs := md.PartitionSpecs()
	out := make(map[int32]*specPartition, len(specs))
	for i := range specs {
		spec := &specs[i]
		sp := &specPartition{id: int32(spec.ID())}
		for _, field := range spec.Fields() {
			if _, void := field.Transform.(iceberg.VoidTransform); void {
				continue
			}
			sp.fields = append(sp.fields, partitionField{id: field.FieldID, name: field.Name})
		}
		out[sp.id] = sp
	}
	return out
}

func (sp *specPartition) key(values map[int]any) string {
	var b strings.Builder
	b.WriteString(strconv.Itoa(int(sp.id)))
	b.WriteByte('|')
	for _, f := range sp.fields {
		b.WriteString(strconv.Itoa(f.id))
		b.WriteByte('=')
		b.WriteString(formatPartitionValue(values[f.id]))
		b.WriteByte(';')
	}
	return b.String()
}

func formatPartitionValue(v any) string {
	switch v := v.(type) {
	case nil:
		return "null"
	case string:
		return strconv.Quote(v)
	case []byte:
		return "0x" + hex.EncodeToString(v)
	default:
		return fmt.Sprint(v)
	}
}

func scanManifest(ctx context.Context, fs icebergio.IO, manifest iceberg.ManifestFile, specs map[int32]*specPartition, acc *accumulator) error {
	for entry, err := range manifest.Entries(fs, true) {
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		df := entry.DataFile()
		if df == nil {
			continue
		}
		switch df.ContentType() {
		case iceberg.EntryContentData:
			key := ""
			if sp, ok := specs[df.SpecID()]; ok {
				key = sp.key(df.Partition())
			}
			acc.addDataFile(df, key)
		case iceberg.EntryContentPosDeletes, iceberg.EntryContentEqDeletes:
			acc.addDeleteFile(df)
		}
	}
	return nil
}

type partitionStat struct {
	files   int64
	bytes   int64
	records int64
}

type accumulator struct {
	mu                sync.Mutex
	smallFileBytes    int64
	largeFileBytes    int64
	maxTracked        int
	dataFiles         int64
	dataBytes         int64
	dataRecords       int64
	deleteFiles       int64
	deleteRecords     int64
	smallFiles        int64
	largeFiles        int64
	fileSizes         Histogram
	partitions        map[string]partitionStat
	trackingTruncated bool
	unpartitioned     bool
}

func newAccumulator(maxTracked int) *accumulator {
	return &accumulator{
		maxTracked: maxTracked,
		fileSizes:  NewHistogram(FileSizeBounds),
		partitions: make(map[string]partitionStat),
	}
}

func (a *accumulator) addDataFile(df iceberg.DataFile, partitionKey string) {
	size := df.FileSizeBytes()
	records := df.Count()
	small := a.smallFileBytes > 0 && size < a.smallFileBytes
	large := a.largeFileBytes > 0 && size > a.largeFileBytes

	a.mu.Lock()
	defer a.mu.Unlock()

	a.dataFiles++
	a.dataBytes += size
	a.dataRecords += records
	if small {
		a.smallFiles++
	}
	if large {
		a.largeFiles++
	}
	a.fileSizes.Observe(float64(size))

	p, ok := a.partitions[partitionKey]
	if !ok {
		if len(a.partitions) >= a.maxTracked {
			a.trackingTruncated = true
			return
		}
	}
	p.files++
	p.bytes += size
	p.records += records
	a.partitions[partitionKey] = p
}

func (a *accumulator) addDeleteFile(df iceberg.DataFile) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.deleteFiles++
	a.deleteRecords += df.Count()
}

func (a *accumulator) finalize(s *TableStats) {
	a.mu.Lock()
	defer a.mu.Unlock()

	s.DataFiles = a.dataFiles
	s.DataBytes = a.dataBytes
	s.DataRecords = a.dataRecords
	s.DeleteFiles = a.deleteFiles
	s.DeleteRecords = a.deleteRecords
	s.SmallFiles = a.smallFiles
	s.LargeFiles = a.largeFiles
	s.FileSizes = a.fileSizes
	s.Partitions = int64(len(a.partitions))
	if a.unpartitioned && len(a.partitions) == 0 {
		// An unpartitioned table always has one implicit partition, even
		// when it currently contains no data files.
		s.Partitions = 1
	}
	s.PartitionTrackingTruncated = a.trackingTruncated

	for _, p := range a.partitions {
		s.PartitionFiles.Observe(float64(p.files))
		s.PartitionBytes.Observe(float64(p.bytes))
		s.PartitionRecords.Observe(float64(p.records))
		if p.files > s.PartitionFilesMax {
			s.PartitionFilesMax = p.files
		}
		if p.bytes > s.PartitionBytesMax {
			s.PartitionBytesMax = p.bytes
		}
	}
}
