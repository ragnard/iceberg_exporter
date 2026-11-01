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

	HasLastSnapshotSummary bool
	LastSnapshot           SnapshotSummary

	ManifestCount int64

	FileStatsCollected bool
	DataFiles          int64
	DataBytes          int64
	DataRecords        int64
	DeleteFiles        int64
	DeleteRecords      int64
	SmallFiles         int64
	LargeFiles         int64
	FileSizes          Histogram

	Partitions                 int64
	PartitionFiles             Histogram
	PartitionBytes             Histogram
	PartitionRecords           Histogram
	PartitionFilesMax          int64
	PartitionBytesMax          int64
	PartitionTrackingTruncated bool
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
