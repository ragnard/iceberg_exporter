// Package icebergstats reads Iceberg table metadata and manifests and turns it
// into plain Go values suitable for metric rendering.
package icebergstats

import (
	"sort"
)

// TableStats is the collected state of one table at one point in time.
type TableStats struct {
	Catalog   string
	Namespace string
	Table     string

	FormatVersion      int
	Partitioned        bool
	SchemaCount        int
	PartitionSpecCount int

	HasCurrentSnapshot         bool
	CurrentSnapshotID          int64
	CurrentSnapshotTimestampMs int64
	LastUpdatedMs              int64
	SnapshotCount              int
	HasSnapshots               bool
	OldestSnapshotTimestampMs  int64
	RefCount                   int
	LastSequenceNumber         int64

	TargetFileSizeBytes int64

	// Refs by type. Branch counts include main.
	Branches int
	Tags     int
	// Timestamp of the oldest snapshot referenced by a non-main branch or
	// tag; only meaningful when the matching Has* flag is set.
	HasOldestBranch        bool
	OldestBranchSnapshotMs int64
	HasOldestTag           bool
	OldestTagSnapshotMs    int64

	// MetadataLogEntries is the number of previous metadata files tracked
	// in the metadata log.
	MetadataLogEntries int

	// LastOperationMs is the timestamp of the newest retained snapshot for
	// each known snapshot operation (append, overwrite, delete, replace).
	// Operations without a retained snapshot are absent.
	LastOperationMs map[string]int64

	StatisticsFiles              int
	PartitionStatisticsFiles     int
	CurrentSnapshotHasStatistics bool

	Settings TableSettings

	HasLastSnapshotSummary bool
	LastSnapshot           SnapshotSummary

	ManifestCount int64

	FileStatsCollected bool
	DataFiles          int64
	DataBytes          int64
	DataRecords        int64
	DeleteFiles        int64
	DeleteRecords      int64
	// Delete files split by kind. Deletion vectors are position deletes
	// stored in Puffin files (format v3).
	PositionDeleteFiles   int64
	PositionDeleteRecords int64
	EqualityDeleteFiles   int64
	EqualityDeleteRecords int64
	DeletionVectors       int64
	DeletionVectorRecords int64
	SmallFiles            int64
	LargeFiles            int64
	FileSizes             Histogram

	Partitions                 int64
	PartitionFiles             Histogram
	PartitionBytes             Histogram
	PartitionRecords           Histogram
	PartitionFilesMax          int64
	PartitionBytesMax          int64
	PartitionTrackingTruncated bool

	// FileStatsReused is true when the manifest-derived stats above were
	// carried over from the previous collection instead of being read again,
	// because the current snapshot and thresholds were unchanged.
	FileStatsReused bool

	// fileStatsKey identifies the inputs the manifest-derived stats were
	// computed from; it is zero when they were not computed from manifests.
	fileStatsKey fileStatsKey
}

// fileStatsKey captures everything manifest-derived stats depend on. Manifest
// files are immutable, so a table's live files are fully determined by its
// current snapshot. Partition specs are immutable per ID and need no key
// entry. The thresholds are included because table properties (such as
// write.target-file-size-bytes) can change without a new snapshot.
type fileStatsKey struct {
	tableUUID      string // distinguishes a dropped and recreated table
	snapshotID     int64
	smallFileBytes int64
	largeFileBytes int64
	maxTracked     int
}

// copyFileStats copies the manifest-derived stats from prev.
func (s *TableStats) copyFileStats(prev *TableStats) {
	s.ManifestCount = prev.ManifestCount
	s.FileStatsCollected = prev.FileStatsCollected
	s.DataFiles = prev.DataFiles
	s.DataBytes = prev.DataBytes
	s.DataRecords = prev.DataRecords
	s.DeleteFiles = prev.DeleteFiles
	s.DeleteRecords = prev.DeleteRecords
	s.PositionDeleteFiles = prev.PositionDeleteFiles
	s.PositionDeleteRecords = prev.PositionDeleteRecords
	s.EqualityDeleteFiles = prev.EqualityDeleteFiles
	s.EqualityDeleteRecords = prev.EqualityDeleteRecords
	s.DeletionVectors = prev.DeletionVectors
	s.DeletionVectorRecords = prev.DeletionVectorRecords
	s.SmallFiles = prev.SmallFiles
	s.LargeFiles = prev.LargeFiles
	// Histograms are shared, not copied: published stats are never mutated.
	s.FileSizes = prev.FileSizes
	s.Partitions = prev.Partitions
	s.PartitionFiles = prev.PartitionFiles
	s.PartitionBytes = prev.PartitionBytes
	s.PartitionRecords = prev.PartitionRecords
	s.PartitionFilesMax = prev.PartitionFilesMax
	s.PartitionBytesMax = prev.PartitionBytesMax
	s.PartitionTrackingTruncated = prev.PartitionTrackingTruncated
	s.fileStatsKey = prev.fileStatsKey
}

// TableSettings are effective maintenance-related table settings: the table
// property when set and valid, otherwise the Iceberg default. Only this
// allowlist is read; other properties are never exported.
type TableSettings struct {
	SnapshotMaxAgeMs            int64
	MinSnapshotsToKeep          int64
	MetadataPreviousVersionsMax int64
	MetadataDeleteAfterCommit   bool
	GCEnabled                   bool
	ManifestMergeEnabled        bool
}

// SnapshotSummary is the delta reported by the current snapshot's summary.
type SnapshotSummary struct {
	FilesAdded     int64
	FilesRemoved   int64
	BytesAdded     int64
	BytesRemoved   int64
	RecordsAdded   int64
	RecordsRemoved int64
}

// Histogram is a cumulative-bucket histogram. It is deliberately the same
// shape Prometheus expects for a constant histogram.
type Histogram struct {
	Bounds  []float64
	Buckets []uint64
	Count   uint64
	Sum     float64
}

// NewHistogram creates an empty histogram with the given upper bounds.
func NewHistogram(bounds []float64) Histogram {
	b := append([]float64(nil), bounds...)
	sort.Float64s(b)
	return Histogram{Bounds: b, Buckets: make([]uint64, len(b))}
}

// Observe adds one observation.
func (h *Histogram) Observe(v float64) {
	h.Count++
	h.Sum += v
	for i, ub := range h.Bounds {
		if v <= ub {
			h.Buckets[i]++
		}
	}
}

// Merge adds another histogram's observations.
func (h *Histogram) Merge(other Histogram) {
	if len(h.Bounds) != len(other.Bounds) {
		return
	}
	h.Count += other.Count
	h.Sum += other.Sum
	for i := range h.Buckets {
		h.Buckets[i] += other.Buckets[i]
	}
}

// PromBuckets returns the cumulative bucket counts keyed by upper bound.
func (h *Histogram) PromBuckets() map[float64]uint64 {
	out := make(map[float64]uint64, len(h.Bounds))
	for i, ub := range h.Bounds {
		out[ub] = h.Buckets[i]
	}
	return out
}

// FileSizeBounds are the buckets used for data file sizes.
var FileSizeBounds = []float64{
	1 << 20,   // 1 MiB
	4 << 20,   // 4 MiB
	16 << 20,  // 16 MiB
	64 << 20,  // 64 MiB
	256 << 20, // 256 MiB
	512 << 20, // 512 MiB
	1 << 30,   // 1 GiB
	2 << 30,   // 2 GiB
	4 << 30,   // 4 GiB
	8 << 30,   // 8 GiB
	16 << 30,  // 16 GiB
}

// Partition file-count buckets.
var PartitionFilesBounds = []float64{1, 2, 4, 8, 16, 32, 64, 128, 256, 1024}

// Partition byte-size buckets.
var PartitionBytesBounds = []float64{
	64 << 20,  // 64 MiB
	256 << 20, // 256 MiB
	1 << 30,   // 1 GiB
	4 << 30,   // 4 GiB
	16 << 30,  // 16 GiB
	64 << 30,  // 64 GiB
	256 << 30, // 256 GiB
	1 << 40,   // 1 TiB
}

// Partition record-count buckets.
var PartitionRecordsBounds = []float64{1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9}

// NewTableStats returns a TableStats with initialized histograms.
func NewTableStats() *TableStats {
	return &TableStats{
		FileSizes:        NewHistogram(FileSizeBounds),
		PartitionFiles:   NewHistogram(PartitionFilesBounds),
		PartitionBytes:   NewHistogram(PartitionBytesBounds),
		PartitionRecords: NewHistogram(PartitionRecordsBounds),
	}
}
