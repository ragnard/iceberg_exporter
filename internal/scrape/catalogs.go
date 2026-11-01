package scrape

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"

	"github.com/ragnard/iceberg_exporter/internal/config"
	"github.com/ragnard/iceberg_exporter/internal/registry"
)

// Catalogs holds the configured catalogs by name.
type Catalogs struct {
	byName  map[string]catalog.Catalog
	order   []string
	closers []catalog.Closer
}

// LoadCatalogs constructs every selected catalog from the configuration. It
// calls catalog.Load eagerly so configuration errors and missing build tags
// surface at startup rather than on the first scrape.
func LoadCatalogs(ctx context.Context, cfg *config.Config) (*Catalogs, error) {
	names := cfg.Scrape.Catalogs
	if len(names) == 0 {
		names = make([]string, 0, len(cfg.Catalogs))
		for name := range cfg.Catalogs {
			names = append(names, name)
		}
	}
	sort.Strings(names)

	out := &Catalogs{byName: make(map[string]catalog.Catalog, len(names))}
	seen := make(map[string]bool, len(names))
	for _, name := range names {
		if seen[name] {
			continue
		}
		seen[name] = true
		out.order = append(out.order, name)

		catCfg, ok := cfg.Catalogs[name]
		if !ok {
			_ = out.Close()
			return nil, fmt.Errorf("scrape.catalogs references unknown catalog %q", name)
		}
		if catCfg.Type != "" && !registry.HasCatalogType(catCfg.Type) {
			_ = out.Close()
			return nil, fmt.Errorf("catalog %q: %w", name, registry.CatalogHint(catCfg.Type))
		}
		props := iceberg.Properties{}
		if catCfg.Type != "" {
			props["type"] = catCfg.Type
		}
		if catCfg.URI != "" {
			props["uri"] = catCfg.URI
		}
		if catCfg.Warehouse != "" {
			props["warehouse"] = catCfg.Warehouse
		}
		if catCfg.Credential != "" {
			props["credential"] = catCfg.Credential
		}
		for k, v := range catCfg.Properties {
			props[k] = v
		}

		cat, err := catalog.Load(ctx, name, props)
		if err != nil {
			_ = out.Close()
			return nil, fmt.Errorf("catalog %q: %w", name, err)
		}
		out.byName[name] = cat
		if closer, ok := cat.(catalog.Closer); ok {
			out.closers = append(out.closers, closer)
		}
	}
	return out, nil
}

// Get returns a catalog by name.
func (c *Catalogs) Get(name string) (catalog.Catalog, bool) {
	cat, ok := c.byName[name]
	return cat, ok
}

// Names returns the configured catalog names in sorted order.
func (c *Catalogs) Names() []string { return append([]string(nil), c.order...) }

// Close releases catalog resources.
func (c *Catalogs) Close() error {
	var errs []error
	for i := len(c.closers) - 1; i >= 0; i-- {
		if err := c.closers[i].Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
