//go:build hadoop

package scrape

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
)

// TestHadoopCatalogIntegration exercises real discovery and metadata loading
// against a local filesystem catalog.
func TestHadoopCatalogIntegration(t *testing.T) {
	ctx := context.Background()
	warehouse := t.TempDir()

	cat, err := catalog.Load(ctx, "local", iceberg.Properties{
		"type":      "hadoop",
		"warehouse": warehouse,
	})
	if err != nil {
		t.Fatalf("load hadoop catalog: %v", err)
	}
	if closer, ok := cat.(catalog.Closer); ok {
		defer closer.Close()
	}

	ns := table.Identifier{"db"}
	if err := cat.CreateNamespace(ctx, ns, nil); err != nil {
		t.Fatalf("create namespace: %v", err)
	}
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)
	ident := table.Identifier{"db", "tbl"}
	if _, err := cat.CreateTable(ctx, ident, schema); err != nil {
		t.Fatalf("create table: %v", err)
	}

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	cats := &Catalogs{byName: map[string]catalog.Catalog{"local": cat}, order: []string{"local"}}
	s, err := New(testConfig(), log, cats)
	if err != nil {
		t.Fatal(err)
	}
	s.Refresh(ctx)

	st := s.State()
	if st == nil || !st.Success {
		t.Fatalf("refresh failed: %+v", st)
	}
	if st.Discovered != 1 || st.Collected != 1 {
		t.Fatalf("discovered/collected = %d/%d, want 1/1", st.Discovered, st.Collected)
	}
	res := st.Tables[0]
	if res.Ref.Namespace != "db" || res.Ref.Table != "tbl" {
		t.Errorf("table identity = %q/%q, want db/tbl", res.Ref.Namespace, res.Ref.Table)
	}
	if res.Stats == nil || res.Stats.FormatVersion != 2 {
		t.Errorf("unexpected stats: %+v", res.Stats)
	}
	if res.Stats.HasCurrentSnapshot {
		t.Error("a newly created table unexpectedly has a snapshot")
	}
}
