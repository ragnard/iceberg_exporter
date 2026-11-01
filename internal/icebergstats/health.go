package icebergstats

import (
	"strconv"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
)

// Iceberg defaults for the maintenance settings in TableSettings. See the
// table properties reference in the Iceberg docs (configuration.md).
const (
	DefaultSnapshotMaxAgeMs            int64 = 5 * 24 * 60 * 60 * 1000 // history.expire.max-snapshot-age-ms
	DefaultMinSnapshotsToKeep          int64 = 1                       // history.expire.min-snapshots-to-keep
	DefaultMetadataPreviousVersionsMax int64 = 100                     // write.metadata.previous-versions-max
	DefaultMetadataDeleteAfterCommit         = false                   // write.metadata.delete-after-commit.enabled
	DefaultGCEnabled                         = true                    // gc.enabled
	DefaultManifestMergeEnabled              = true                    // commit.manifest-merge.enabled
)

// SnapshotOperations are the snapshot operations reported in
// TableStats.LastOperationMs, in rendering order.
var SnapshotOperations = []string{
	string(table.OpAppend),
	string(table.OpOverwrite),
	string(table.OpDelete),
	string(table.OpReplace),
}

// collectHealth fills the metadata-only health fields of s from md. It does
// no I/O.
func collectHealth(md table.Metadata, s *TableStats) {
	s.Settings = tableSettings(md)

	for range md.PreviousFiles() {
		s.MetadataLogEntries++
	}

	for name, ref := range md.Refs() {
		var ts int64
		if snap := md.SnapshotByID(ref.SnapshotID); snap != nil {
			ts = snap.TimestampMs
		}
		switch ref.SnapshotRefType {
		case table.BranchRef:
			s.Branches++
			if name == table.MainBranch || ts <= 0 {
				continue
			}
			if !s.HasOldestBranch || ts < s.OldestBranchSnapshotMs {
				s.HasOldestBranch = true
				s.OldestBranchSnapshotMs = ts
			}
		case table.TagRef:
			s.Tags++
			if ts <= 0 {
				continue
			}
			if !s.HasOldestTag || ts < s.OldestTagSnapshotMs {
				s.HasOldestTag = true
				s.OldestTagSnapshotMs = ts
			}
		}
	}

	for _, snap := range md.Snapshots() {
		if snap.Summary == nil || snap.TimestampMs <= 0 {
			continue
		}
		op := string(snap.Summary.Operation)
		if !knownOperation(op) {
			continue
		}
		if s.LastOperationMs == nil {
			s.LastOperationMs = make(map[string]int64, len(SnapshotOperations))
		}
		if snap.TimestampMs > s.LastOperationMs[op] {
			s.LastOperationMs[op] = snap.TimestampMs
		}
	}

	var currentID int64
	hasCurrent := false
	if snap := md.CurrentSnapshot(); snap != nil {
		currentID, hasCurrent = snap.SnapshotID, true
	}
	for f := range md.Statistics() {
		s.StatisticsFiles++
		if hasCurrent && f.SnapshotID == currentID {
			s.CurrentSnapshotHasStatistics = true
		}
	}
	for range md.PartitionStatistics() {
		s.PartitionStatisticsFiles++
	}
}

func knownOperation(op string) bool {
	for _, known := range SnapshotOperations {
		if op == known {
			return true
		}
	}
	return false
}

// tableSettings resolves the effective maintenance settings. Snapshot
// retention on the main branch ref overrides the table properties, matching
// how Iceberg's snapshot expiration resolves them.
func tableSettings(md table.Metadata) TableSettings {
	props := md.Properties()
	st := TableSettings{
		SnapshotMaxAgeMs:            propInt64(props, "history.expire.max-snapshot-age-ms", DefaultSnapshotMaxAgeMs),
		MinSnapshotsToKeep:          propInt64(props, "history.expire.min-snapshots-to-keep", DefaultMinSnapshotsToKeep),
		MetadataPreviousVersionsMax: propInt64(props, "write.metadata.previous-versions-max", DefaultMetadataPreviousVersionsMax),
		MetadataDeleteAfterCommit:   propBool(props, "write.metadata.delete-after-commit.enabled", DefaultMetadataDeleteAfterCommit),
		GCEnabled:                   propBool(props, "gc.enabled", DefaultGCEnabled),
		ManifestMergeEnabled:        propBool(props, "commit.manifest-merge.enabled", DefaultManifestMergeEnabled),
	}
	for name, ref := range md.Refs() {
		if name != table.MainBranch {
			continue
		}
		if ref.MaxSnapshotAgeMs != nil && *ref.MaxSnapshotAgeMs > 0 {
			st.SnapshotMaxAgeMs = *ref.MaxSnapshotAgeMs
		}
		if ref.MinSnapshotsToKeep != nil && *ref.MinSnapshotsToKeep > 0 {
			st.MinSnapshotsToKeep = int64(*ref.MinSnapshotsToKeep)
		}
	}
	return st
}

func propInt64(props iceberg.Properties, key string, def int64) int64 {
	if v, ok := props[key]; ok {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			return n
		}
	}
	return def
}

func propBool(props iceberg.Properties, key string, def bool) bool {
	if v, ok := props[key]; ok {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	}
	return def
}
