package scrape

import (
	"context"
	"errors"
	"io"
	"iter"
	"log/slog"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	icebergio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"

	"github.com/ragnard/iceberg_exporter/internal/config"
)

func testTable(t *testing.T) *table.Table {
	t.Helper()
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)
	md, err := table.NewMetadata(schema, iceberg.UnpartitionedSpec, table.UnsortedSortOrder,
		"file:///tmp/table", nil)
	if err != nil {
		t.Fatal(err)
	}
	return table.New(table.Identifier{"ns", "t1"}, md, "file:///tmp/table/metadata.json",
		func(context.Context) (icebergio.IO, error) {
			return nil, errors.New("filesystem should not be opened")
		}, nil)
}

type fakeCatalog struct {
	catalog.Catalog
	mu                sync.Mutex
	namespaces        []table.Identifier
	tables            map[string][]table.Identifier
	fail              map[string]bool
	listNamespacesErr error
	tbl               *table.Table
	// children maps a dotted parent namespace to what ListNamespaces
	// returns for it; missing namespaces return ErrNoSuchNamespace.
	children map[string][]table.Identifier
	missing  map[string]bool
	listed   []string // parents passed to ListNamespaces
}

func (f *fakeCatalog) CatalogType() catalog.Type { return catalog.REST }

func (f *fakeCatalog) ListNamespaces(_ context.Context, parent table.Identifier) ([]table.Identifier, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.listed = append(f.listed, joinIdent(parent))
	if f.listNamespacesErr != nil {
		return nil, f.listNamespacesErr
	}
	if f.missing[joinIdent(parent)] {
		return nil, catalog.ErrNoSuchNamespace
	}
	if len(parent) == 0 {
		return f.namespaces, nil
	}
	return f.children[joinIdent(parent)], nil
}

func (f *fakeCatalog) ListTables(_ context.Context, ns table.Identifier) iter.Seq2[table.Identifier, error] {
	key := joinIdent(ns)
	f.mu.Lock()
	idents := append([]table.Identifier(nil), f.tables[key]...)
	missing := f.missing[key]
	f.mu.Unlock()
	return func(yield func(table.Identifier, error) bool) {
		if missing {
			yield(nil, catalog.ErrNoSuchNamespace)
			return
		}
		for _, ident := range idents {
			if !yield(ident, nil) {
				return
			}
		}
	}
}

func (f *fakeCatalog) LoadTable(_ context.Context, ident table.Identifier) (*table.Table, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fail[joinIdent(ident)] {
		return nil, errors.New("injected load failure")
	}
	return f.tbl, nil
}

func joinIdent(ident table.Identifier) string {
	out := ""
	for i, part := range ident {
		if i > 0 {
			out += "."
		}
		out += part
	}
	return out
}

func testConfig() *config.Config {
	return &config.Config{
		Scrape: config.ScrapeConfig{
			Interval:        config.Duration(time.Hour),
			Timeout:         config.Duration(time.Minute),
			Concurrency:     2,
			Namespaces:      config.Filter{Include: []string{"**"}},
			Tables:          config.Filter{Include: []string{"**"}},
			Manifests:       config.ManifestsConfig{Enabled: true, Concurrency: 2},
			Partitions:      config.PartitionsConfig{MaxTracked: 100},
			SmallFileBytes:  1 << 20,
			TargetFileBytes: 1 << 20,
		},
	}
}

func newTestScraper(t *testing.T, cfg *config.Config, fc *fakeCatalog) *Scraper {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	cats := &Catalogs{
		byName: map[string]catalog.Catalog{"prod": fc},
		order:  []string{"prod"},
	}
	s, err := New(cfg, log, cats)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestDiscoverAndFilter(t *testing.T) {
	tbl := testTable(t)
	fc := &fakeCatalog{
		namespaces: []table.Identifier{{"ns"}},
		tables: map[string][]table.Identifier{
			"ns": {{"ns", "t1"}, {"ns", "t2"}},
		},
		fail: map[string]bool{},
		tbl:  tbl,
	}
	cfg := testConfig()
	cfg.Scrape.Tables.Exclude = []string{"ns.t2"}
	s := newTestScraper(t, cfg, fc)

	var errorsSeen int
	s.SetTableErrorObserver(func(string) { errorsSeen++ })

	s.Refresh(context.Background())
	st := s.State()
	if st == nil {
		t.Fatal("State() = nil")
	}
	if !st.Success {
		t.Errorf("Success = false, want true")
	}
	if st.Discovered != 1 || st.Collected != 1 || st.Failing != 0 {
		t.Errorf("discovered/collected/failing = %d/%d/%d, want 1/1/0", st.Discovered, st.Collected, st.Failing)
	}
	if len(st.Tables) != 1 {
		t.Fatalf("len(Tables) = %d, want 1", len(st.Tables))
	}
	res := st.Tables[0]
	if res.Ref.FullName() != "ns.t1" || !res.Success || res.Stats == nil {
		t.Errorf("unexpected result: %+v", res)
	}
	if errorsSeen != 0 {
		t.Errorf("table error observer called %d times, want 0", errorsSeen)
	}
}

func TestFailedTableRetainsPreviousStats(t *testing.T) {
	tbl := testTable(t)
	fc := &fakeCatalog{
		namespaces: []table.Identifier{{"ns"}},
		tables: map[string][]table.Identifier{
			"ns": {{"ns", "t1"}},
		},
		fail: map[string]bool{},
		tbl:  tbl,
	}
	s := newTestScraper(t, testConfig(), fc)

	var errorsSeen int
	s.SetTableErrorObserver(func(string) { errorsSeen++ })

	s.Refresh(context.Background())
	first := s.State().Tables[0]
	if first.Stats == nil {
		t.Fatal("first refresh has no stats")
	}

	fc.mu.Lock()
	fc.fail["ns.t1"] = true
	fc.mu.Unlock()

	s.Refresh(context.Background())
	st := s.State()
	if st.Collected != 0 || st.Failing != 1 {
		t.Errorf("collected/failing = %d/%d, want 0/1", st.Collected, st.Failing)
	}
	res := st.Tables[0]
	if res.Success {
		t.Error("result Success = true, want false")
	}
	if res.Stats != first.Stats {
		t.Error("previous stats were not retained")
	}
	if !res.LastSuccess.Equal(first.LastSuccess) {
		t.Error("LastSuccess changed after a failed collection")
	}
	if errorsSeen != 1 {
		t.Errorf("table error observer called %d times, want 1", errorsSeen)
	}
}

func TestNamespaceExclusion(t *testing.T) {
	fc := &fakeCatalog{
		namespaces: []table.Identifier{{"ns"}},
		tables: map[string][]table.Identifier{
			"ns": {{"ns", "t1"}},
		},
		fail: map[string]bool{},
		tbl:  testTable(t),
	}
	cfg := testConfig()
	cfg.Scrape.Namespaces.Exclude = []string{"ns"}
	s := newTestScraper(t, cfg, fc)
	s.Refresh(context.Background())
	if got := s.State().Discovered; got != 0 {
		t.Errorf("Discovered = %d, want 0", got)
	}
}

func TestTableKeyIsCollisionFree(t *testing.T) {
	a := TableRef{Catalog: "c", Identifier: table.Identifier{"a.b", "c"}}
	b := TableRef{Catalog: "c", Identifier: table.Identifier{"a", "b.c"}}
	if a.Key() == b.Key() {
		t.Errorf("distinct identifiers produced the same key: %q", a.Key())
	}
}

func TestDiscoveryFailureRetainsTables(t *testing.T) {
	fc := &fakeCatalog{
		namespaces: []table.Identifier{{"ns"}},
		tables: map[string][]table.Identifier{
			"ns": {{"ns", "t1"}},
		},
		fail: map[string]bool{},
		tbl:  testTable(t),
	}
	s := newTestScraper(t, testConfig(), fc)
	s.Refresh(context.Background())
	if got := s.State().Discovered; got != 1 {
		t.Fatalf("Discovered = %d, want 1", got)
	}

	fc.mu.Lock()
	fc.listNamespacesErr = errors.New("injected discovery failure")
	fc.mu.Unlock()

	s.Refresh(context.Background())
	st := s.State()
	if st.Success {
		t.Error("Success = true after a discovery failure")
	}
	if st.DiscoveryErrors != 1 {
		t.Errorf("DiscoveryErrors = %d, want 1", st.DiscoveryErrors)
	}
	if st.Discovered != 1 {
		t.Errorf("Discovered = %d, want the previous table to be retained", st.Discovered)
	}
}

func discoveredNames(t *testing.T, s *Scraper) []string {
	t.Helper()
	refs, errs := s.discover(context.Background())
	if len(errs) > 0 {
		t.Fatalf("discover() errors = %v", errs)
	}
	var out []string
	for _, ref := range refs {
		out = append(out, ref.FullName())
	}
	return out
}

func TestDiscoverNestedNamespaces(t *testing.T) {
	tables := map[string][]table.Identifier{
		"a":     {{"a", "t"}},
		"a.b":   {{"a", "b", "t"}},
		"a.b.c": {{"a", "b", "c", "t"}},
		"x":     {{"x", "t"}},
	}
	want := []string{"a.t", "a.b.t", "a.b.c.t", "x.t"}

	cases := map[string]map[string][]table.Identifier{
		// REST: full identifiers of direct children.
		"direct children": {
			"a":   {{"a", "b"}},
			"a.b": {{"a", "b", "c"}},
		},
		// SQL: the parent itself plus descendants at every depth.
		"parent and all descendants": {
			"a":     {{"a"}, {"a", "b"}, {"a", "b", "c"}},
			"a.b":   {{"a", "b"}, {"a", "b", "c"}},
			"a.b.c": {{"a", "b", "c"}},
		},
		// A server that ignores the parent parameter must not cause
		// recursion into bogus namespaces such as a.a.
		"parent ignored": {
			"a":     {{"a"}, {"x"}, {"a", "b"}},
			"a.b":   {{"a"}, {"x"}, {"a", "b", "c"}},
			"a.b.c": {{"a"}, {"x"}},
			"x":     {{"a"}, {"x"}},
		},
	}
	for name, children := range cases {
		t.Run(name, func(t *testing.T) {
			fc := &fakeCatalog{
				namespaces: []table.Identifier{{"a"}, {"x"}},
				tables:     tables,
				children:   children,
			}
			got := discoveredNames(t, newTestScraper(t, testConfig(), fc))
			if !slices.Equal(got, want) {
				t.Errorf("discovered = %v, want %v", got, want)
			}
			listed := slices.Sorted(slices.Values(fc.listed))
			if wantListed := []string{"", "a", "a.b", "a.b.c", "x"}; !slices.Equal(listed, wantListed) {
				t.Errorf("listed namespaces = %q, want %q", listed, wantListed)
			}
		})
	}
}

func TestDiscoverToleratesVanishedNamespace(t *testing.T) {
	fc := &fakeCatalog{
		namespaces: []table.Identifier{{"gone"}, {"ns"}},
		tables:     map[string][]table.Identifier{"ns": {{"ns", "t1"}}},
		missing:    map[string]bool{"gone": true},
	}
	got := discoveredNames(t, newTestScraper(t, testConfig(), fc))
	if want := []string{"ns.t1"}; !slices.Equal(got, want) {
		t.Errorf("discovered = %v, want %v", got, want)
	}
}
