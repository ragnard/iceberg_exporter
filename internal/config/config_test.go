package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

func yamlNode(s string) *yaml.Node {
	return &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: s}
}

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadDefaultsAndEnvExpansion(t *testing.T) {
	t.Setenv("TEST_CREDENTIAL", "secret-value")
	path := writeConfig(t, `
catalogs:
  prod:
    type: rest
    uri: https://catalog.example.com
    credential: ${TEST_CREDENTIAL}
    properties:
      s3.region: eu-west-1
      glue.max-retries: 5
`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Listen != DefaultListen {
		t.Errorf("Listen = %q, want %q", cfg.Listen, DefaultListen)
	}
	if got := cfg.Scrape.Interval.Duration(); got != DefaultInterval {
		t.Errorf("interval = %v, want %v", got, DefaultInterval)
	}
	if got := cfg.Scrape.Timeout.Duration(); got != DefaultTimeout {
		t.Errorf("timeout = %v, want %v", got, DefaultTimeout)
	}
	if cfg.Scrape.Concurrency != DefaultConcurrency {
		t.Errorf("concurrency = %d, want %d", cfg.Scrape.Concurrency, DefaultConcurrency)
	}
	if got := cfg.ScrapedCatalogs(); len(got) != 1 || got[0] != "prod" {
		t.Errorf("ScrapedCatalogs() = %v, want [prod]", got)
	}
	if got := cfg.CatalogScrape("prod").Namespaces.Include; len(got) != 1 || got[0] != "**" {
		t.Errorf("namespace include = %v, want [**]", got)
	}
	if !cfg.Scrape.Manifests.Enabled {
		t.Error("manifests.enabled = false, want true by default")
	}
	if cfg.Catalogs["prod"].Credential != "secret-value" {
		t.Errorf("credential was not expanded: %q", cfg.Catalogs["prod"].Credential)
	}
	if cfg.Catalogs["prod"].Properties["glue.max-retries"] != "5" {
		t.Errorf("non-string property was not converted: %q", cfg.Catalogs["prod"].Properties["glue.max-retries"])
	}
}

func TestLoadRejectsUnknownFields(t *testing.T) {
	path := writeConfig(t, `
bogus: true
catalogs:
  prod:
    type: rest
    uri: https://catalog.example.com
`)
	if _, err := Load(path); err == nil {
		t.Fatal("Load() succeeded with an unknown field")
	}

	// Nested typos are rejected too, with the line of the offending key.
	path = writeConfig(t, `catalogs:
  prod:
    type: rest
    uri: https://catalog.example.com
scrape:
  manifests:
    enabeld: false
`)
	_, err := Load(path)
	if err == nil || !strings.Contains(err.Error(), "line 7: field enabeld not found") {
		t.Fatalf("Load() error = %v, want unknown field enabeld on line 7", err)
	}
}

func TestEnvExpansionIsStructureSafe(t *testing.T) {
	for _, secret := range []string{
		"abc #def",
		"key: value",
		`has "quotes" and \backslash`,
		"*starts-with-star",
		"&anchor",
		"line1\nline2: injected",
		"0123",
		"true",
	} {
		t.Run(secret, func(t *testing.T) {
			t.Setenv("SECRET", secret)
			t.Setenv("WORKERS", "3")
			cfg, err := Load(writeConfig(t, `
catalogs:
  prod:
    type: rest
    uri: https://example.com
    credential: ${SECRET}
    properties:
      plain: ${SECRET}
      quoted: "${SECRET}"
      literal: 0123
scrape:
  concurrency: ${WORKERS}  # comments with ${UNSET_IN_COMMENT} are not expanded
`))
			if err != nil {
				t.Fatalf("Load() error = %v", err)
			}
			cat := cfg.Catalogs["prod"]
			if cat.Credential != secret {
				t.Errorf("credential = %q, want %q", cat.Credential, secret)
			}
			for _, key := range []string{"plain", "quoted"} {
				if got := cat.Properties[key]; got != secret {
					t.Errorf("property %s = %q, want %q", key, got, secret)
				}
			}
			if got := cat.Properties["literal"]; got != "0123" {
				t.Errorf("literal property = %q, want verbatim 0123", got)
			}
			if len(cat.Properties) != 3 {
				t.Errorf("properties = %v, want exactly plain/quoted/literal", cat.Properties)
			}
			if cfg.Scrape.Concurrency != 3 {
				t.Errorf("concurrency = %d, want 3 from the environment", cfg.Scrape.Concurrency)
			}
		})
	}
}

func TestValidate(t *testing.T) {
	tests := []struct {
		name    string
		body    string
		wantErr bool
	}{
		{
			name:    "no catalogs",
			body:    `scrape: {}`,
			wantErr: true,
		},
		{
			name: "catalog without type or uri",
			body: `
catalogs:
  prod: {}
`,
			wantErr: true,
		},
		{
			name: "unknown selected catalog",
			body: `
catalogs:
  prod: {type: rest, uri: https://example.com}
scrape:
  catalogs:
    staging: {}
`,
			wantErr: true,
		},
		{
			name: "invalid pattern",
			body: `
catalogs:
  prod: {type: rest, uri: https://example.com}
scrape:
  catalogs:
    prod:
      tables:
        exclude: ["[bad"]
`,
			wantErr: true,
		},
		{
			name: "global namespace filter",
			body: `
catalogs:
  prod: {type: rest, uri: https://example.com}
scrape:
  namespaces:
    include: ["**"]
`,
			wantErr: true,
		},
		{
			name: "zero manifest concurrency override",
			body: `
catalogs:
  prod: {type: rest, uri: https://example.com}
scrape:
  catalogs:
    prod:
      manifests: {concurrency: 0}
`,
			wantErr: true,
		},
		{
			name: "unknown per-catalog key",
			body: `
catalogs:
  prod: {type: rest, uri: https://example.com}
scrape:
  catalogs:
    prod:
      interval: 1m
`,
			wantErr: true,
		},
		{
			name: "valid",
			body: `
catalogs:
  prod: {type: rest, uri: https://example.com}
scrape:
  interval: 30s
  timeout: 20s
`,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Load(writeConfig(t, tt.body))
			if (err != nil) != tt.wantErr {
				t.Fatalf("Load() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestExampleConfigParses(t *testing.T) {
	t.Setenv("ICEBERG_CREDENTIAL", "test")
	cfg, err := Load(filepath.Join("..", "..", "config.example.yaml"))
	if err != nil {
		t.Fatalf("example config does not parse: %v", err)
	}
	if len(cfg.Catalogs) != 1 {
		t.Errorf("example config has %d catalogs, want 1", len(cfg.Catalogs))
	}
}

func TestEnvExpansion(t *testing.T) {
	t.Setenv("EXPAND_ME", "value")
	path := writeConfig(t, `
catalogs:
  prod:
    type: rest
    uri: https://example.com
    credential: ${EXPAND_ME}$$literal${MISSING_PLACEHOLDER}
`)
	// The suffix is not shell syntax; only ${NAME} is expanded.
	if _, err := Load(path); err == nil {
		t.Fatal("Load() accepted an unset environment variable")
	}

	path = writeConfig(t, `
catalogs:
  prod:
    type: rest
    uri: https://example.com
    credential: ${EXPAND_ME}$$literal
`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.Catalogs["prod"].Credential; got != "value$literal" {
		t.Errorf("credential = %q, want %q", got, "value$literal")
	}
}

func TestPerCatalogScrape(t *testing.T) {
	path := writeConfig(t, `
catalogs:
  prod: {type: rest, uri: https://example.com}
  archive: {type: rest, uri: https://example.com}
  dev: {type: rest, uri: https://example.com}
scrape:
  manifests:
    concurrency: 16
  catalogs:
    prod:
      namespaces:
        exclude: ["tmp.**"]
      tables:
        include: ["sales.**"]
    archive:
      manifests:
        enabled: false
    dev:
`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if got := cfg.ScrapedCatalogs(); strings.Join(got, ",") != "archive,dev,prod" {
		t.Errorf("ScrapedCatalogs() = %v, want [archive dev prod]", got)
	}

	prod := cfg.CatalogScrape("prod")
	if strings.Join(prod.Namespaces.Include, ",") != "**" || strings.Join(prod.Namespaces.Exclude, ",") != "tmp.**" {
		t.Errorf("prod namespaces = %+v, want include [**], exclude [tmp.**]", prod.Namespaces)
	}
	if strings.Join(prod.Tables.Include, ",") != "sales.**" {
		t.Errorf("prod tables include = %v, want [sales.**]", prod.Tables.Include)
	}
	if !prod.Manifests.Enabled || prod.Manifests.Concurrency != 16 {
		t.Errorf("prod manifests = %+v, want inherited {true 16}", prod.Manifests)
	}

	archive := cfg.CatalogScrape("archive")
	if archive.Manifests.Enabled || archive.Manifests.Concurrency != 16 {
		t.Errorf("archive manifests = %+v, want {false 16}", archive.Manifests)
	}

	// A bare key selects the catalog with every default.
	dev := cfg.CatalogScrape("dev")
	if strings.Join(dev.Tables.Include, ",") != "**" || !dev.Manifests.Enabled {
		t.Errorf("dev = %+v, want defaults", dev)
	}
}

func TestScrapeCatalogsSubset(t *testing.T) {
	path := writeConfig(t, `
catalogs:
  prod: {type: rest, uri: https://example.com}
  dev: {type: rest, uri: https://example.com}
scrape:
  catalogs:
    prod: {}
`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if got := cfg.ScrapedCatalogs(); strings.Join(got, ",") != "prod" {
		t.Errorf("ScrapedCatalogs() = %v, want [prod]", got)
	}
}

func TestFilterMatch(t *testing.T) {
	m, err := CompileFilter(Filter{
		Include: []string{"prod.**"},
		Exclude: []string{"prod.tmp.**"},
	})
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name string
		want bool
	}{
		{"prod", true},
		{"prod.sales", true},
		{"prod.sales.eu", true},
		{"prod.tmp", false},
		{"prod.tmp.x", false},
		{"staging.sales", false},
	}
	for _, tt := range tests {
		if got := m.Match(tt.name); got != tt.want {
			t.Errorf("Match(%q) = %v, want %v", tt.name, got, tt.want)
		}
	}
}

func TestFilterDefaultsToEverything(t *testing.T) {
	m, err := CompileFilter(Filter{Include: []string{"**"}})
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"", "a", "a.b.c"} {
		if !m.Match(name) {
			t.Errorf("Match(%q) = false, want true", name)
		}
	}
}

func TestDurationParsing(t *testing.T) {
	var d Duration
	if err := d.UnmarshalYAML(yamlNode("1h30m")); err != nil {
		t.Fatal(err)
	}
	if got := d.Duration(); got != 90*time.Minute {
		t.Errorf("duration = %v, want 90m", got)
	}
	if err := d.UnmarshalYAML(yamlNode("not-a-duration")); err == nil {
		t.Fatal("expected an error for an invalid duration")
	}
}
