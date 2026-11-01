// Package config loads and validates the exporter configuration.
package config

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/bmatcuk/doublestar/v4"
	"gopkg.in/yaml.v3"
)

const (
	DefaultListen          = ":8080"
	DefaultInterval        = 5 * time.Minute
	DefaultTimeout         = 4 * time.Minute
	DefaultConcurrency     = 4
	DefaultManifestWorkers = 8
	DefaultMaxPartitions   = 100_000
	DefaultSmallFileBytes  = 64 << 20 // 64 MiB
	DefaultTargetFileBytes = 512 << 20
)

// Config is the top-level exporter configuration.
type Config struct {
	Listen   string                   `yaml:"listen"`
	Log      LogConfig                `yaml:"log"`
	Catalogs map[string]CatalogConfig `yaml:"catalogs"`
	Scrape   ScrapeConfig             `yaml:"scrape"`
}

// LogConfig controls the process logger.
type LogConfig struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"`
}

// CatalogConfig describes one Iceberg catalog passed to iceberg-go.
type CatalogConfig struct {
	Type       string    `yaml:"type"`
	URI        string    `yaml:"uri"`
	Warehouse  string    `yaml:"warehouse"`
	Credential string    `yaml:"credential"`
	Properties StringMap `yaml:"properties"`
}

// ScrapeConfig controls discovery and metric collection.
type ScrapeConfig struct {
	Interval        Duration         `yaml:"interval"`
	Timeout         Duration         `yaml:"timeout"`
	Concurrency     int              `yaml:"concurrency"`
	Catalogs        []string         `yaml:"catalogs"`
	Namespaces      Filter           `yaml:"namespaces"`
	Tables          Filter           `yaml:"tables"`
	Manifests       ManifestsConfig  `yaml:"manifests"`
	Partitions      PartitionsConfig `yaml:"partitions"`
	SmallFileBytes  int64            `yaml:"small_file_bytes"`
	LargeFileBytes  int64            `yaml:"large_file_bytes"`
	TargetFileBytes int64            `yaml:"target_file_size_bytes"`
}

// ManifestsConfig controls reading manifest entries.
type ManifestsConfig struct {
	Enabled     bool `yaml:"enabled"`
	Concurrency int  `yaml:"concurrency"`
}

// PartitionsConfig controls partition aggregation.
type PartitionsConfig struct {
	MaxTracked int `yaml:"max_tracked"`
}

// Filter is a pair of include/exclude glob patterns.
type Filter struct {
	Include []string `yaml:"include"`
	Exclude []string `yaml:"exclude"`
}

// Duration is a time.Duration that unmarshals from a Go duration string.
type Duration time.Duration

func (d *Duration) UnmarshalYAML(value *yaml.Node) error {
	var s string
	if err := value.Decode(&s); err != nil {
		return fmt.Errorf("duration must be a string such as 5m: %w", err)
	}
	parsed, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", s, err)
	}
	*d = Duration(parsed)
	return nil
}

func (d Duration) MarshalYAML() (any, error) { return time.Duration(d).String(), nil }

// Duration returns the value as a time.Duration.
func (d Duration) Duration() time.Duration { return time.Duration(d) }

// StringMap accepts scalar YAML values and stores them as strings, matching
// the iceberg.Properties type used by iceberg-go.
type StringMap map[string]string

func (m *StringMap) UnmarshalYAML(value *yaml.Node) error {
	var raw map[string]any
	if err := value.Decode(&raw); err != nil {
		return err
	}
	out := make(map[string]string, len(raw))
	for k, v := range raw {
		switch v := v.(type) {
		case nil:
			out[k] = ""
		case string:
			out[k] = v
		case bool, int, int64, uint64, float64:
			out[k] = fmt.Sprint(v)
		default:
			return fmt.Errorf("property %q must be a scalar value", k)
		}
	}
	*m = out
	return nil
}

var envPlaceholder = regexp.MustCompile(`\$\$|\$\{([A-Za-z_][A-Za-z0-9_]*)\}`)

func expandEnv(data []byte) ([]byte, error) {
	var missing []string
	out := envPlaceholder.ReplaceAllFunc(data, func(match []byte) []byte {
		if string(match) == "$$" {
			return []byte("$")
		}
		name := string(match[2 : len(match)-1])
		value, ok := os.LookupEnv(name)
		if !ok {
			missing = append(missing, name)
			return nil
		}
		return []byte(value)
	})
	if len(missing) > 0 {
		return nil, fmt.Errorf("unset environment variable(s): %s", strings.Join(uniqueStrings(missing), ", "))
	}
	return out, nil
}

func uniqueStrings(in []string) []string {
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, s := range in {
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	return out
}

// Load reads, expands, parses, applies defaults to, and validates a config file.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}

	// Supports ${VAR} placeholders in the file, which is handy for
	// credentials. "$$" is an escaped literal dollar sign. Unset variables
	// are an error rather than silently becoming empty.
	expanded, err := expandEnv(data)
	if err != nil {
		return nil, err
	}

	var cfg = Config{
		Scrape: ScrapeConfig{
			Manifests:       ManifestsConfig{Enabled: true},
			SmallFileBytes:  DefaultSmallFileBytes,
			TargetFileBytes: DefaultTargetFileBytes,
		},
	}
	dec := yaml.NewDecoder(bytes.NewReader(expanded))
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	if err := cfg.setDefaults(); err != nil {
		return nil, err
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func (c *Config) setDefaults() error {
	if c.Listen == "" {
		c.Listen = DefaultListen
	}
	if c.Log.Level == "" {
		c.Log.Level = "info"
	}
	if c.Log.Format == "" {
		c.Log.Format = "logfmt"
	}
	if c.Scrape.Interval == 0 {
		c.Scrape.Interval = Duration(DefaultInterval)
	}
	if c.Scrape.Timeout == 0 {
		c.Scrape.Timeout = Duration(DefaultTimeout)
	}
	if c.Scrape.Concurrency == 0 {
		c.Scrape.Concurrency = DefaultConcurrency
	}
	if c.Scrape.Namespaces.Include == nil {
		c.Scrape.Namespaces.Include = []string{"**"}
	}
	if c.Scrape.Tables.Include == nil {
		c.Scrape.Tables.Include = []string{"**"}
	}
	if c.Scrape.Manifests.Concurrency == 0 {
		c.Scrape.Manifests.Concurrency = DefaultManifestWorkers
	}
	if c.Scrape.Partitions.MaxTracked == 0 {
		c.Scrape.Partitions.MaxTracked = DefaultMaxPartitions
	}
	return nil
}

// Validate checks the configuration for structural errors.
func (c *Config) Validate() error {
	var errs []error
	if len(c.Catalogs) == 0 {
		errs = append(errs, errors.New("at least one catalog must be configured"))
	}
	for name, cat := range c.Catalogs {
		if name == "" {
			errs = append(errs, errors.New("catalog name must not be empty"))
		}
		if cat.Type == "" && cat.URI == "" {
			errs = append(errs, fmt.Errorf("catalog %q: one of type or uri is required", name))
		}
	}
	if c.Scrape.Interval.Duration() <= 0 {
		errs = append(errs, errors.New("scrape.interval must be greater than zero"))
	}
	if c.Scrape.Timeout.Duration() <= 0 {
		errs = append(errs, errors.New("scrape.timeout must be greater than zero"))
	}
	if c.Scrape.Concurrency <= 0 {
		errs = append(errs, errors.New("scrape.concurrency must be greater than zero"))
	}
	if c.Scrape.Manifests.Concurrency <= 0 {
		errs = append(errs, errors.New("scrape.manifests.concurrency must be greater than zero"))
	}
	if c.Scrape.Partitions.MaxTracked <= 0 {
		errs = append(errs, errors.New("scrape.partitions.max_tracked must be greater than zero"))
	}
	if c.Scrape.SmallFileBytes < 0 {
		errs = append(errs, errors.New("scrape.small_file_bytes must not be negative"))
	}
	if c.Scrape.LargeFileBytes < 0 {
		errs = append(errs, errors.New("scrape.large_file_bytes must not be negative"))
	}
	if c.Scrape.TargetFileBytes <= 0 {
		errs = append(errs, errors.New("scrape.target_file_size_bytes must be greater than zero"))
	}
	if err := validateFilter("scrape.namespaces", c.Scrape.Namespaces); err != nil {
		errs = append(errs, err)
	}
	if err := validateFilter("scrape.tables", c.Scrape.Tables); err != nil {
		errs = append(errs, err)
	}
	for _, name := range c.Scrape.Catalogs {
		if _, ok := c.Catalogs[name]; !ok {
			errs = append(errs, fmt.Errorf("scrape.catalogs references unknown catalog %q", name))
		}
	}
	return errors.Join(errs...)
}

func validateFilter(prefix string, f Filter) error {
	var errs []error
	for _, p := range append(append([]string{}, f.Include...), f.Exclude...) {
		if !doublestar.ValidatePattern(p) {
			errs = append(errs, fmt.Errorf("%s: invalid pattern %q", prefix, p))
		}
	}
	return errors.Join(errs...)
}

// Matcher is a compiled Filter.
type Matcher struct {
	include []string
	exclude []string
}

// CompileFilter validates and compiles a filter.
func CompileFilter(f Filter) (*Matcher, error) {
	m := &Matcher{include: append([]string{}, f.Include...), exclude: append([]string{}, f.Exclude...)}
	for _, p := range append(append([]string{}, m.include...), m.exclude...) {
		if !doublestar.ValidatePattern(p) {
			return nil, fmt.Errorf("invalid pattern %q", p)
		}
	}
	return m, nil
}

// Match reports whether name is included and not excluded. Patterns match the
// full dotted namespace or table name; both * and ** may span dots.
func (m *Matcher) Match(name string) bool {
	if !m.matchAny(m.include, name) {
		return false
	}
	return !m.matchAny(m.exclude, name)
}

func (m *Matcher) matchAny(patterns []string, name string) bool {
	for _, p := range patterns {
		if matchPattern(p, name) {
			return true
		}
	}
	return false
}

func matchPattern(pattern, name string) bool {
	if ok, err := doublestar.Match(pattern, name); err == nil && ok {
		return true
	}
	// A trailing ".**" is a subtree pattern; make it match the subtree root
	// too, so "prod.**" includes the "prod" namespace itself.
	if strings.HasSuffix(pattern, ".**") {
		if ok, err := doublestar.Match(strings.TrimSuffix(pattern, ".**"), name); err == nil && ok {
			return true
		}
	}
	return false
}
