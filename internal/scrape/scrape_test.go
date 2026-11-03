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
	typ      catalog.Type
}

func (f *fakeCatalog) CatalogType() catalog.Type {
	if f.typ == "" {
		return catalog.REST
	}
	return f.typ
}

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
			Interval:    config.Duration(time.Hour),
			Timeout:     config.Duration(time.Minute),
			Concurrency: 2,
			Manifests:   config.ManifestsConfig{Enabled: true, Concurrency: 2},
			Partitions:  config.PartitionsConfig{MaxTracked: 100},
		},
	}
}

// fixedCatalogs configures one catalog whose loader always returns cat.
func fixedCatalogs(name string, cat catalog.Catalog) *Catalogs {
	return &Catalogs{
		order: []string{name},
		props: map[string]iceberg.Properties{name: {}},
		load: func(context.Context, string, iceberg.Properties) (catalog.Catalog, error) {
			return cat, nil
		},
	}
}

// closingCatalog records Close calls.
type closingCatalog struct {
	*fakeCatalog
	closed *int
}

func (c closingCatalog) Close() error { *c.closed++; return nil }

func TestCatalogsAreReopenedEveryRefresh(t *testing.T) {
	fc := &fakeCatalog{
		namespaces: []table.Identifier{{"ns"}},
		tables:     map[string][]table.Identifier{"ns": {{"ns", "t1"}}},
		tbl:        testTable(t),
	}
	var loads, closed int
	var loadErr error
	cats := fixedCatalogs("prod", nil)
	cats.load = func(context.Context, string, iceberg.Properties) (catalog.Catalog, error) {
		loads++
		if loadErr != nil {
			return nil, loadErr
		}
		return closingCatalog{fakeCatalog: fc, closed: &closed}, nil
	}
	var tableErrors int
	s, err := New(testConfig(), slog.New(slog.NewTextHandler(io.Discard, nil)), cats)
	if err != nil {
		t.Fatal(err)
	}
	s.SetTableErrorObserver(func(string) { tableErrors++ })

	for i := 1; i <= 2; i++ {
		s.Refresh(context.Background())
		if loads != i || closed != i {
			t.Fatalf("after refresh %d: loads/closes = %d/%d, want %d/%d", i, loads, closed, i, i)
		}
		if st := s.State(); !st.Success || st.Collected != 1 {
			t.Fatalf("refresh %d: success/collected = %v/%d, want true/1", i, st.Success, st.Collected)
		}
	}

	// A catalog that cannot be loaded is a discovery failure: its tables
	// are retained with their previous stats and marked as failing.
	loadErr = errors.New("metastore unreachable")
	s.Refresh(context.Background())
	st := s.State()
	if st.Success || st.DiscoveryErrors != 1 {
		t.Errorf("success/discovery errors = %v/%d, want false/1", st.Success, st.DiscoveryErrors)
	}
	if len(st.Tables) != 1 {
		t.Fatalf("len(Tables) = %d, want 1 retained table", len(st.Tables))
	}
	if res := st.Tables[0]; res.Success || res.Stats == nil || res.LastSuccess.IsZero() {
		t.Errorf("retained table = success %v, stats %v, last success %v; want failing with previous stats",
			res.Success, res.Stats != nil, res.LastSuccess)
	}
	if tableErrors != 1 {
		t.Errorf("table errors = %d, want 1", tableErrors)
	}
	if closed != 2 {
		t.Errorf("closes = %d, want 2 (nothing to close after a failed load)", closed)
	}

	// The next refresh recovers with a fresh client.
	loadErr = nil
	s.Refresh(context.Background())
	if st := s.State(); !st.Success || st.Collected != 1 {
		t.Errorf("after recovery: success/collected = %v/%d, want true/1", st.Success, st.Collected)
	}
}

func newTestScraper(t *testing.T, cfg *config.Config, fc *fakeCatalog) *Scraper {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	s, err := New(cfg, log, fixedCatalogs("prod", fc))
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
	cfg.Scrape.Catalogs = map[string]config.CatalogScrapeConfig{
		"prod": {Tables: config.Filter{Exclude: []string{"ns.t2"}}},
	}
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
	cfg.Scrape.Catalogs = map[string]config.CatalogScrapeConfig{
		"prod": {Namespaces: config.Filter{Exclude: []string{"ns"}}},
	}
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

func TestCollidingLabelsSortDeterministically(t *testing.T) {
	// ["a.b"].t and ["a","b"].t both render as namespace="a.b", table="t".
	// The collector exports the first of the two, so their order must not
	// depend on map iteration.
	both := []table.Identifier{{"a.b", "t"}, {"a", "b", "t"}}
	fc := &fakeCatalog{
		namespaces: []table.Identifier{{"a.b"}, {"a"}},
		children:   map[string][]table.Identifier{"a": {{"a", "b"}}},
		tables:     map[string][]table.Identifier{"a.b": both},
		tbl:        testTable(t),
	}
	s := newTestScraper(t, testConfig(), fc)
	want := table.Identifier{"a", "b", "t"} // smallest collision-free key
	for range 50 {
		s.Refresh(context.Background())
		tables := s.State().Tables
		if len(tables) != 2 {
			t.Fatalf("len(Tables) = %d, want 2", len(tables))
		}
		if got := tables[0].Ref.Identifier; !slices.Equal(got, want) {
			t.Fatalf("first colliding table = %v, want %v", got, want)
		}
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
	cats := s.catalogs.open(context.Background())
	defer cats.close(s.log)
	refs, errs := s.discover(context.Background(), cats)
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

func TestPerCatalogScrapePolicy(t *testing.T) {
	fc := &fakeCatalog{
		namespaces: []table.Identifier{{"ns"}},
		tables:     map[string][]table.Identifier{"ns": {{"ns", "t1"}, {"ns", "t2"}}},
		tbl:        testTable(t),
	}
	cats := &Catalogs{
		order: []string{"archive", "prod"},
		props: map[string]iceberg.Properties{"archive": {}, "prod": {}},
		load: func(context.Context, string, iceberg.Properties) (catalog.Catalog, error) {
			return fc, nil
		},
	}
	disabled := false
	cfg := testConfig()
	cfg.Scrape.Catalogs = map[string]config.CatalogScrapeConfig{
		"prod":    {Tables: config.Filter{Exclude: []string{"ns.t2"}}},
		"archive": {Manifests: config.ManifestsOverride{Enabled: &disabled}},
	}
	s, err := New(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), cats)
	if err != nil {
		t.Fatal(err)
	}
	s.Refresh(context.Background())

	got := map[string][]string{}
	fileStats := map[string]bool{}
	for _, res := range s.State().Tables {
		got[res.Ref.Catalog] = append(got[res.Ref.Catalog], res.Ref.Namespace+"."+res.Ref.Table)
		if res.Stats != nil && res.Stats.FileStatsCollected {
			fileStats[res.Ref.Catalog] = true
		}
	}
	// The prod table filter does not apply to archive.
	if want := []string{"ns.t1", "ns.t2"}; !slices.Equal(got["archive"], want) {
		t.Errorf("archive tables = %v, want %v", got["archive"], want)
	}
	if want := []string{"ns.t1"}; !slices.Equal(got["prod"], want) {
		t.Errorf("prod tables = %v, want %v", got["prod"], want)
	}
	// Only archive disables manifest reads.
	if fileStats["archive"] || !fileStats["prod"] {
		t.Errorf("file stats collected = %v, want prod only", fileStats)
	}
}

func TestDiscoverFlatCatalogs(t *testing.T) {
	for _, typ := range []catalog.Type{catalog.Hive, catalog.Glue} {
		t.Run(string(typ), func(t *testing.T) {
			fc := &fakeCatalog{
				typ:        typ,
				namespaces: []table.Identifier{{"a"}, {"x"}},
				tables: map[string][]table.Identifier{
					"a": {{"a", "t"}},
					"x": {{"x", "t"}},
				},
			}
			got := discoveredNames(t, newTestScraper(t, testConfig(), fc))
			if want := []string{"a.t", "x.t"}; !slices.Equal(got, want) {
				t.Errorf("discovered = %v, want %v", got, want)
			}
			// Flat catalogs are listed only at the root.
			if want := []string{""}; !slices.Equal(fc.listed, want) {
				t.Errorf("listed namespaces = %q, want %q", fc.listed, want)
			}
		})
	}
}
