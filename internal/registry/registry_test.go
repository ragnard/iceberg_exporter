package registry

import (
	"slices"
	"testing"
)

func TestDefaultCatalogTypes(t *testing.T) {
	types := CatalogTypes()
	for _, want := range []string{"hive", "rest"} {
		if !slices.Contains(types, want) {
			t.Errorf("CatalogTypes() = %v, missing %q", types, want)
		}
	}
	if !HasCatalogType("rest") || !HasCatalogType("HIVE") {
		t.Error("HasCatalogType failed for a default catalog")
	}
}

func TestDefaultObjectStore(t *testing.T) {
	schemes := ObjectStoreSchemes()
	if !slices.Contains(schemes, "s3") {
		t.Errorf("ObjectStoreSchemes() = %v, missing s3", schemes)
	}
	if slices.Contains(schemes, "file") || slices.Contains(schemes, "") || slices.Contains(schemes, "mem") {
		t.Errorf("ObjectStoreSchemes() = %v, local schemes should be omitted", schemes)
	}
}

func TestIOHint(t *testing.T) {
	tests := map[string]string{
		"s3":   "s3",
		"gs":   "gcs",
		"abfs": "azure",
		"file": "",
	}
	for scheme, want := range tests {
		if got := IOHint(scheme); got != want {
			t.Errorf("IOHint(%q) = %q, want %q", scheme, got, want)
		}
	}
}
