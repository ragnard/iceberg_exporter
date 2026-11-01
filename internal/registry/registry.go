// Package registry links Iceberg catalog and object-store implementations into
// the binary. REST, Hive and S3 are compiled in by default; every other backend
// is opt-in through a Go build tag:
//
//	go build -tags glue ./...
//	go build -tags "sql sqlpgx" ./...
//	go build -tags gcs ./...
//	go build -tags "glue sql hadoop gcs azure" ./...
//
// The tags are documented in the README.
package registry

import (
	"fmt"
	"slices"
	"strings"

	"github.com/apache/iceberg-go/catalog"
	icebergio "github.com/apache/iceberg-go/io"
)

// CatalogTypes returns the catalog types compiled into this binary, sorted.
// REST registers the aliases "http" and "https"; those are omitted when
// "rest" is present.
func CatalogTypes() []string {
	types := catalog.GetRegisteredCatalogs()
	out := make([]string, 0, len(types))
	for _, t := range types {
		if (t == "http" || t == "https") && slices.Contains(types, "rest") {
			continue
		}
		out = append(out, t)
	}
	slices.Sort(out)
	return out
}

// HasCatalogType reports whether the catalog type is compiled in.
func HasCatalogType(t string) bool {
	return slices.Contains(catalog.GetRegisteredCatalogs(), strings.ToLower(t))
}

// ObjectStoreSchemes returns the non-local filesystem schemes compiled into
// this binary, sorted.
func ObjectStoreSchemes() []string {
	schemes := icebergio.GetRegisteredSchemes()
	out := make([]string, 0, len(schemes))
	for _, s := range schemes {
		if s == "" || s == "file" || s == "mem" {
			continue
		}
		out = append(out, s)
	}
	slices.Sort(out)
	return out
}

// CatalogHint returns a startup error explaining how to enable a catalog type
// that is not compiled in.
func CatalogHint(t string) error {
	return fmt.Errorf("catalog type %q is not compiled into this binary (available: %s); rebuild with the matching build tag, e.g. -tags %s",
		t, strings.Join(CatalogTypes(), ", "), t)
}

// IOHint maps an object-store scheme to the build tag that enables it.
func IOHint(scheme string) string {
	switch scheme {
	case "s3", "s3a", "s3n":
		return "s3"
	case "gs", "gcs":
		return "gcs"
	case "wasb", "wasbs", "abfs", "abfss":
		return "azure"
	default:
		return ""
	}
}
