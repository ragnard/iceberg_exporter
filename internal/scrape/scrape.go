package scrape

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	"golang.org/x/sync/errgroup"

	"github.com/ragnard/iceberg_exporter/internal/config"
	"github.com/ragnard/iceberg_exporter/internal/icebergstats"
)

// TableKey uniquely identifies a table within the exporter. ID is a
// collision-free encoding of the full identifier, so namespaces whose
// components contain dots cannot alias one another.
type TableKey struct {
	Catalog string
	ID      string
}

// TableRef points at one discovered table.
type TableRef struct {
	Catalog    string
	Namespace  string
	Table      string
	Identifier table.Identifier
}

// Key returns the table's stable key.
func (r TableRef) Key() TableKey {
	return TableKey{Catalog: r.Catalog, ID: identifierKey(r.Identifier)}
}

func identifierKey(ident table.Identifier) string {
	var b strings.Builder
	for _, part := range ident {
		b.WriteString(strconv.Itoa(len(part)))
		b.WriteByte(':')
		b.WriteString(part)
		b.WriteByte(';')
	}
	return b.String()
}

// compareRefs orders refs by their exported labels (catalog, dotted
// namespace, table), breaking ties with the collision-free identifier key.
// Refs whose labels collide therefore always sort the same way, so the
// collector consistently exports the same one of them.
func compareRefs(a, b TableRef) int {
	return cmp.Or(
		cmp.Compare(a.Catalog, b.Catalog),
		cmp.Compare(a.Namespace, b.Namespace),
		cmp.Compare(a.Table, b.Table),
		cmp.Compare(identifierKey(a.Identifier), identifierKey(b.Identifier)),
	)
}

// FullName is the dotted namespace and table name.
func (r TableRef) FullName() string {
	if r.Namespace == "" {
		return r.Table
	}
	return r.Namespace + "." + r.Table
}

// TableResult is the outcome of one table for the latest refresh.
type TableResult struct {
	Ref         TableRef
	Stats       *icebergstats.TableStats
	Success     bool
	LastAttempt time.Time
	LastSuccess time.Time
	Err         error
}

// State is an immutable snapshot of the latest refresh.
type State struct {
	Started         time.Time
	Finished        time.Time
	Duration        time.Duration
	Success         bool
	DiscoveryErrors int
	Discovered      int
	Collected       int
	Failing         int
	// FileStatsReused counts tables collected without reading manifests
	// because their current snapshot was unchanged since the last refresh.
	FileStatsReused int
	Tables          []TableResult
}

// Scraper periodically collects table statistics and keeps the latest value
// for every table.
type Scraper struct {
	cfg          *config.Config
	log          *slog.Logger
	catalogs     *Catalogs
	nsMatcher    *config.Matcher
	tableMatcher *config.Matcher

	state atomic.Pointer[State]

	refreshMu sync.Mutex

	// results is only touched while refreshMu is held.
	results map[TableKey]TableResult

	catalogLocks map[string]*sync.Mutex

	onTableError func(catalog string)
}

// New creates a scraper. The configuration must already be validated.
func New(cfg *config.Config, log *slog.Logger, catalogs *Catalogs) (*Scraper, error) {
	nsMatcher, err := config.CompileFilter(cfg.Scrape.Namespaces)
	if err != nil {
		return nil, fmt.Errorf("namespace filter: %w", err)
	}
	tableMatcher, err := config.CompileFilter(cfg.Scrape.Tables)
	if err != nil {
		return nil, fmt.Errorf("table filter: %w", err)
	}
	s := &Scraper{
		cfg:          cfg,
		log:          log,
		catalogs:     catalogs,
		nsMatcher:    nsMatcher,
		tableMatcher: tableMatcher,
		results:      make(map[TableKey]TableResult),
		catalogLocks: make(map[string]*sync.Mutex),
	}
	for _, name := range catalogs.Names() {
		s.catalogLocks[name] = &sync.Mutex{}
	}
	return s, nil
}

// SetTableErrorObserver sets a callback invoked for every failed table.
func (s *Scraper) SetTableErrorObserver(fn func(catalog string)) { s.onTableError = fn }

// State returns the latest refresh state, or nil before the first refresh.
func (s *Scraper) State() *State { return s.state.Load() }

// Run refreshes immediately and then on the configured interval until the
// context is cancelled. Refreshes never overlap.
func (s *Scraper) Run(ctx context.Context) {
	s.Refresh(ctx)

	ticker := time.NewTicker(s.cfg.Scrape.Interval.Duration())
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.Refresh(ctx)
		}
	}
}

// Refresh runs one full discovery and collection pass. It serializes
// concurrent callers.
func (s *Scraper) Refresh(ctx context.Context) {
	s.refreshMu.Lock()
	defer s.refreshMu.Unlock()

	started := time.Now()
	runCtx, cancel := context.WithTimeout(ctx, s.cfg.Scrape.Timeout.Duration())
	defer cancel()

	// Fresh catalog clients for every refresh, closed once all table workers
	// are done (g.Wait below returns before the deferred close runs).
	cats := s.catalogs.open(runCtx)
	defer cats.close(s.log)

	refs, discoveryErrs := s.discover(runCtx, cats)

	results := make(map[TableKey]TableResult, len(refs))
	var mu sync.Mutex

	g, gctx := errgroup.WithContext(runCtx)
	g.SetLimit(s.cfg.Scrape.Concurrency)
	for _, ref := range refs {
		g.Go(func() error {
			cat, catErr := cats.get(ref.Catalog)
			var stats *icebergstats.TableStats
			err := catErr
			if catErr == nil {
				opts := s.optionsFor(ref.Catalog)
				opts.LoadLock = s.loadLock(ref.Catalog, cat)
				// s.results is not written until all workers finish, so it
				// is safe to read here without mu.
				opts.Previous = s.results[ref.Key()].Stats
				stats, err = icebergstats.Collect(gctx, cat, ref.Identifier, opts)
			}
			now := time.Now()

			mu.Lock()
			prev := s.results[ref.Key()]
			res := TableResult{Ref: ref, LastAttempt: now}
			if err != nil {
				res.Success = false
				res.Err = err
				res.Stats = prev.Stats
				res.LastSuccess = prev.LastSuccess
			} else {
				res.Success = true
				res.Stats = stats
				res.LastSuccess = now
			}
			results[ref.Key()] = res
			mu.Unlock()

			if err != nil && runCtx.Err() == nil {
				// A catalog that failed to load is logged once as a
				// discovery error rather than once per retained table.
				if catErr == nil {
					s.log.Warn("table scrape failed", "catalog", ref.Catalog, "table", ref.FullName(), "error", err)
				}
				if s.onTableError != nil {
					s.onTableError(ref.Catalog)
				}
			}
			return nil
		})
	}
	_ = g.Wait()

	finished := time.Now()
	tables := make([]TableResult, 0, len(results))
	collected, failing, reused := 0, 0, 0
	for _, res := range results {
		tables = append(tables, res)
		if res.Success {
			collected++
			if res.Stats != nil && res.Stats.FileStatsReused {
				reused++
			}
		} else {
			failing++
		}
	}
	slices.SortFunc(tables, func(a, b TableResult) int { return compareRefs(a.Ref, b.Ref) })
	s.results = results

	st := &State{
		Started:         started,
		Finished:        finished,
		Duration:        finished.Sub(started),
		Success:         len(discoveryErrs) == 0 && runCtx.Err() == nil,
		DiscoveryErrors: len(discoveryErrs),
		Discovered:      len(refs),
		Collected:       collected,
		Failing:         failing,
		FileStatsReused: reused,
		Tables:          tables,
	}
	s.state.Store(st)

	log := s.log.With(
		"duration", st.Duration.Round(time.Millisecond),
		"discovered", st.Discovered,
		"collected", st.Collected,
		"failing", st.Failing,
		"file_stats_reused", st.FileStatsReused,
		"success", st.Success,
	)
	if !st.Success {
		log.Warn("refresh completed with errors", "discovery_errors", st.DiscoveryErrors)
		for _, err := range discoveryErrs {
			s.log.Warn("catalog discovery failed", "error", err)
		}
	} else {
		log.Info("refresh completed")
	}
}

// loadLock returns the lock that serializes table loads for catalogs whose
// clients are not safe for concurrent use, or nil. The Hive metastore client
// shares a single Thrift transport and is not goroutine-safe. Only the
// catalog call is serialized; manifest reads stay concurrent.
func (s *Scraper) loadLock(name string, cat catalog.Catalog) sync.Locker {
	if cat == nil || cat.CatalogType() != catalog.Hive {
		return nil
	}
	if mu := s.catalogLocks[name]; mu != nil {
		return mu
	}
	return nil
}

func (s *Scraper) optionsFor(catalogName string) icebergstats.Options {
	return icebergstats.Options{
		Catalog:              catalogName,
		Manifests:            s.cfg.Scrape.Manifests.Enabled,
		ManifestConcurrency:  s.cfg.Scrape.Manifests.Concurrency,
		MaxTrackedPartitions: s.cfg.Scrape.Partitions.MaxTracked,
	}
}

func (s *Scraper) discover(ctx context.Context, cats *openCatalogs) ([]TableRef, []error) {
	refByKey := make(map[TableKey]TableRef)
	var errs []error

	for _, name := range s.catalogs.Names() {
		var refs []TableRef
		cat, err := cats.get(name)
		if err == nil {
			refs, err = s.discoverCatalog(ctx, name, cat)
		}
		if err != nil {
			errs = append(errs, fmt.Errorf("catalog %q: %w", name, err))
			// Keep previously known tables so a transient discovery failure
			// does not erase the catalog from the metrics.
			for key, res := range s.results {
				if key.Catalog == name {
					refByKey[key] = res.Ref
				}
			}
		}
		for _, ref := range refs {
			refByKey[ref.Key()] = ref
		}
	}

	out := make([]TableRef, 0, len(refByKey))
	for _, ref := range refByKey {
		out = append(out, ref)
	}
	slices.SortFunc(out, compareRefs)
	return out, errs
}

func (s *Scraper) discoverCatalog(ctx context.Context, name string, cat catalog.Catalog) ([]TableRef, error) {
	if cat == nil {
		return nil, errors.New("catalog is not loaded")
	}
	nested := supportsHierarchy(cat.CatalogType())

	visited := make(map[string]bool)
	queue := []table.Identifier{nil}
	var refs []TableRef

	for len(queue) > 0 {
		ns := queue[0]
		queue = queue[1:]
		nsName := strings.Join(ns, ".")
		nsKey := identifierKey(ns)
		if visited[nsKey] {
			continue
		}
		visited[nsKey] = true

		// Tables always live in a namespace; the root namespace has none.
		// Listing tables at the root is invalid in several catalogs.
		if len(ns) > 0 && s.nsMatcher.Match(nsName) {
			for ident, err := range cat.ListTables(ctx, ns) {
				if errors.Is(err, catalog.ErrNoSuchNamespace) {
					// Dropped since it was listed; nothing to collect.
					s.log.Debug("namespace disappeared during discovery", "catalog", name, "namespace", nsName)
					break
				}
				if err != nil {
					return refs, fmt.Errorf("list tables in namespace %q: %w", nsName, err)
				}
				if err := ctx.Err(); err != nil {
					return refs, err
				}
				if len(ident) == 0 {
					return refs, fmt.Errorf("catalog returned an empty table identifier in namespace %q", nsName)
				}
				fullName := strings.Join(ident, ".")
				if !s.tableMatcher.Match(fullName) {
					continue
				}
				refs = append(refs, TableRef{
					Catalog:    name,
					Namespace:  strings.Join(ident[:len(ident)-1], "."),
					Table:      ident[len(ident)-1],
					Identifier: append(table.Identifier(nil), ident...),
				})
			}
		}

		if !nested {
			continue
		}
		children, err := cat.ListNamespaces(ctx, ns)
		if err != nil {
			if err := ctx.Err(); err != nil {
				return refs, err
			}
			if len(ns) > 0 && errors.Is(err, catalog.ErrNoSuchNamespace) {
				s.log.Debug("namespace disappeared during discovery", "catalog", name, "namespace", nsName)
				continue
			}
			return refs, fmt.Errorf("list namespaces under %q: %w", nsName, err)
		}
		for _, child := range children {
			if isDescendant(ns, child) {
				queue = append(queue, append(table.Identifier(nil), child...))
			}
		}
	}
	return refs, nil
}

func supportsHierarchy(t catalog.Type) bool {
	switch t {
	case catalog.Hive, catalog.Glue:
		return false
	default:
		return true
	}
}

// isDescendant reports whether child is a namespace strictly below parent.
//
// Catalogs return full namespace identifiers, but they differ in what a
// listing under parent contains: REST returns direct children, SQL returns
// parent itself plus descendants at any depth, and a REST server without
// nested-namespace support may ignore parent and return top-level
// namespaces. Keeping only strict descendants handles all of them; deeper
// descendants are deduplicated by the traversal's visited set.
func isDescendant(parent, child table.Identifier) bool {
	return len(child) > len(parent) && slices.Equal(child[:len(parent)], parent)
}
