package scrape

import (
	"context"
	"fmt"
	"log/slog"
	"sort"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"

	"github.com/ragnard/iceberg_exporter/internal/config"
	"github.com/ragnard/iceberg_exporter/internal/registry"
)

// Catalogs describes the configured catalogs. It does not hold catalog
// clients: every refresh opens fresh clients and closes them when it is done,
// so a broken connection (for example a dropped Hive Thrift transport) never
// outlives a single refresh.
type Catalogs struct {
	order []string
	props map[string]iceberg.Properties
	load  func(ctx context.Context, name string, props iceberg.Properties) (catalog.Catalog, error)
}

// NewCatalogs validates the selected catalogs without connecting to them.
// Missing build tags and unknown catalog names fail here, at startup; whether
// a catalog is reachable is only known per refresh and is reported through
// metrics.
func NewCatalogs(cfg *config.Config) (*Catalogs, error) {
	names := cfg.Scrape.Catalogs
	if len(names) == 0 {
		names = make([]string, 0, len(cfg.Catalogs))
		for name := range cfg.Catalogs {
			names = append(names, name)
		}
	}
	names = append([]string(nil), names...)
	sort.Strings(names)

	out := &Catalogs{
		props: make(map[string]iceberg.Properties, len(names)),
		load:  catalog.Load,
	}
	for _, name := range names {
		if _, seen := out.props[name]; seen {
			continue
		}
		catCfg, ok := cfg.Catalogs[name]
		if !ok {
			return nil, fmt.Errorf("scrape.catalogs references unknown catalog %q", name)
		}
		if catCfg.Type != "" && !registry.HasCatalogType(catCfg.Type) {
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
		out.order = append(out.order, name)
		out.props[name] = props
	}
	return out, nil
}

// Names returns the configured catalog names in sorted order.
func (c *Catalogs) Names() []string { return append([]string(nil), c.order...) }

// openCatalogs is the set of catalog clients used by one refresh.
type openCatalogs struct {
	byName map[string]catalog.Catalog
	errs   map[string]error
}

// open creates a fresh client for every catalog. A catalog that fails to
// load is recorded in errs; the others are still usable.
func (c *Catalogs) open(ctx context.Context) *openCatalogs {
	o := &openCatalogs{
		byName: make(map[string]catalog.Catalog, len(c.order)),
		errs:   make(map[string]error),
	}
	for _, name := range c.order {
		cat, err := c.load(ctx, name, c.props[name])
		if err != nil {
			o.errs[name] = fmt.Errorf("load catalog: %w", err)
			continue
		}
		o.byName[name] = cat
	}
	return o
}

// get returns the catalog client, or the error that prevented loading it.
func (o *openCatalogs) get(name string) (catalog.Catalog, error) {
	if err, ok := o.errs[name]; ok {
		return nil, err
	}
	cat, ok := o.byName[name]
	if !ok {
		return nil, fmt.Errorf("catalog %q is not configured", name)
	}
	return cat, nil
}

// close releases every client opened for the refresh.
func (o *openCatalogs) close(log *slog.Logger) {
	for name, cat := range o.byName {
		closer, ok := cat.(catalog.Closer)
		if !ok {
			continue
		}
		if err := closer.Close(); err != nil {
			log.Warn("closing catalog", "catalog", name, "error", err)
		}
	}
}
