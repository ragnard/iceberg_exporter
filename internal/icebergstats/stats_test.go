package icebergstats

import (
	"reflect"
	"testing"
)

// metadataOnlyFields are TableStats fields recomputed from table metadata on
// every refresh, so copyFileStats must not copy them. Every other field is
// manifest-derived and must be carried over when file stats are reused.
var metadataOnlyFields = map[string]bool{
	"Catalog": true, "Namespace": true, "Table": true,
	"FormatVersion": true, "Partitioned": true, "SchemaCount": true, "PartitionSpecCount": true,
	"HasCurrentSnapshot": true, "CurrentSnapshotID": true, "CurrentSnapshotTimestampMs": true,
	"LastUpdatedMs": true, "SnapshotCount": true, "HasSnapshots": true, "OldestSnapshotTimestampMs": true,
	"RefCount": true, "LastSequenceNumber": true, "TargetFileSizeBytes": true,
	"Branches": true, "Tags": true,
	"HasOldestBranch": true, "OldestBranchSnapshotMs": true, "HasOldestTag": true, "OldestTagSnapshotMs": true,
	"MetadataLogEntries": true, "LastOperationMs": true,
	"StatisticsFiles": true, "PartitionStatisticsFiles": true, "CurrentSnapshotHasStatistics": true,
	"Settings": true, "HasLastSnapshotSummary": true, "LastSnapshot": true,
	"FileStatsReused": true, // set by Collect itself
}

// TestCopyFileStatsCopiesEveryFileStat fails when a manifest-derived field is
// added to TableStats without being copied on reuse (or listed above).
func TestCopyFileStatsCopiesEveryFileStat(t *testing.T) {
	prev := &TableStats{}
	pv := reflect.ValueOf(prev).Elem()
	for i := 0; i < pv.NumField(); i++ {
		setNonZero(t, pv.Type().Field(i).Name, pv.Field(i))
	}

	s := &TableStats{}
	s.copyFileStats(prev)
	sv := reflect.ValueOf(s).Elem()
	for i := 0; i < sv.NumField(); i++ {
		name := sv.Type().Field(i).Name
		if metadataOnlyFields[name] {
			if !sv.Field(i).IsZero() {
				t.Errorf("copyFileStats copied metadata-only field %s", name)
			}
			continue
		}
		if !reflect.DeepEqual(reflectValue(sv.Field(i)), reflectValue(pv.Field(i))) {
			t.Errorf("copyFileStats does not copy %s; copy it, or add it to metadataOnlyFields if it is recomputed from metadata", name)
		}
	}
}

// setNonZero fills v (including unexported struct fields) with a non-zero
// value.
func setNonZero(t *testing.T, name string, v reflect.Value) {
	t.Helper()
	if !v.CanSet() {
		v = reflect.NewAt(v.Type(), v.Addr().UnsafePointer()).Elem()
	}
	switch v.Kind() {
	case reflect.Bool:
		v.SetBool(true)
	case reflect.Int, reflect.Int32, reflect.Int64:
		v.SetInt(7)
	case reflect.Uint64:
		v.SetUint(7)
	case reflect.Float64:
		v.SetFloat(7)
	case reflect.String:
		v.SetString("x")
	case reflect.Map:
		m := reflect.MakeMap(v.Type())
		m.SetMapIndex(reflect.Zero(v.Type().Key()), reflect.Zero(v.Type().Elem()))
		v.Set(m)
	case reflect.Slice:
		v.Set(reflect.MakeSlice(v.Type(), 1, 1))
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			setNonZero(t, name+"."+v.Type().Field(i).Name, v.Field(i))
		}
	default:
		t.Fatalf("setNonZero: unhandled kind %s for %s", v.Kind(), name)
	}
}

func reflectValue(v reflect.Value) any {
	if !v.CanInterface() {
		v = reflect.NewAt(v.Type(), v.Addr().UnsafePointer()).Elem()
	}
	return v.Interface()
}
