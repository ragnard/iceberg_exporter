// Package config loads and validates the exporter configuration.
package config

import (
	"errors"
	"fmt"
	"os"
	"reflect"
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

// Values are taken verbatim from the YAML text, so "0123", "1.0" or "0x10"
// are not reinterpreted as numbers; a null value becomes "".
func (m *StringMap) UnmarshalYAML(value *yaml.Node) error {
	if value.Kind == yaml.AliasNode && value.Alias != nil {
		value = value.Alias
	}
	if value.Kind != yaml.MappingNode {
		return fmt.Errorf("line %d: properties must be a mapping", value.Line)
	}
	out := make(map[string]string, len(value.Content)/2)
	for i := 0; i+1 < len(value.Content); i += 2 {
		k, v := value.Content[i], value.Content[i+1]
		if v.Kind == yaml.AliasNode && v.Alias != nil {
			v = v.Alias
		}
		if k.Kind != yaml.ScalarNode || v.Kind != yaml.ScalarNode {
			return fmt.Errorf("line %d: property %q must be a scalar value", k.Line, k.Value)
		}
		if v.ShortTag() == "!!null" {
			out[k.Value] = ""
		} else {
			out[k.Value] = v.Value
		}
	}
	*m = out
	return nil
}

var envPlaceholder = regexp.MustCompile(`\$\$|\$\{([A-Za-z_][A-Za-z0-9_]*)\}`)

// expandEnv expands ${VAR} placeholders inside the scalars of a parsed YAML
// document. Expanding after parsing means a value can never change the
// document structure: a secret containing " #", ": ", quotes or newlines is
// taken verbatim. Comments are not expanded.
func expandEnv(root *yaml.Node) error {
	var missing []string
	var walk func(n *yaml.Node)
	walk = func(n *yaml.Node) {
		if n.Kind == yaml.ScalarNode && envPlaceholder.MatchString(n.Value) {
			n.Value = envPlaceholder.ReplaceAllStringFunc(n.Value, func(match string) string {
				if match == "$$" {
					return "$"
				}
				name := match[2 : len(match)-1]
				value, ok := os.LookupEnv(name)
				if !ok {
					missing = append(missing, name)
				}
				return value
			})
			// A plain, untagged scalar was typed from its placeholder text
			// (always a string). Clear the tag so it is re-resolved from the
			// expanded value, letting "concurrency: ${N}" decode as an int.
			// Quoted and explicitly tagged scalars stay strings.
			if n.Style == 0 {
				n.Tag = ""
			}
		}
		// Aliases share their anchor's node, which is expanded where it is
		// defined; following them would expand it twice.
		for _, c := range n.Content {
			walk(c)
		}
	}
	walk(root)
	if len(missing) > 0 {
		return fmt.Errorf("unset environment variable(s): %s", strings.Join(uniqueStrings(missing), ", "))
	}
	return nil
}

var unmarshalerType = reflect.TypeFor[yaml.Unmarshaler]()

// checkKnownFields rejects mapping keys that do not correspond to a field of
// the target struct, recursively. yaml.Node.Decode has no KnownFields option,
// so this restores strict decoding while keeping the original line numbers.
// Type mismatches are left to Decode to report.
func checkKnownFields(n *yaml.Node, t reflect.Type) error {
	for n.Kind == yaml.DocumentNode && len(n.Content) == 1 {
		n = n.Content[0]
	}
	if n.Kind == yaml.AliasNode && n.Alias != nil {
		n = n.Alias
	}
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if reflect.PointerTo(t).Implements(unmarshalerType) {
		return nil
	}

	var errs []error
	switch t.Kind() {
	case reflect.Struct:
		if n.Kind != yaml.MappingNode {
			return nil
		}
		fields := make(map[string]reflect.Type, t.NumField())
		for i := range t.NumField() {
			f := t.Field(i)
			name, _, _ := strings.Cut(f.Tag.Get("yaml"), ",")
			if name == "-" || !f.IsExported() {
				continue
			}
			if name == "" {
				name = strings.ToLower(f.Name)
			}
			fields[name] = f.Type
		}
		for i := 0; i+1 < len(n.Content); i += 2 {
			k, v := n.Content[i], n.Content[i+1]
			if k.ShortTag() == "!!merge" {
				errs = append(errs, checkKnownFields(v, t))
				continue
			}
			ft, ok := fields[k.Value]
			if !ok {
				errs = append(errs, fmt.Errorf("line %d: field %s not found in type %s", k.Line, k.Value, t))
				continue
			}
			errs = append(errs, checkKnownFields(v, ft))
		}
	case reflect.Map:
		if n.Kind != yaml.MappingNode {
			return nil
		}
		for i := 1; i < len(n.Content); i += 2 {
			errs = append(errs, checkKnownFields(n.Content[i], t.Elem()))
		}
	case reflect.Slice, reflect.Array:
		if n.Kind != yaml.SequenceNode {
			return nil
		}
		for _, c := range n.Content {
			errs = append(errs, checkKnownFields(c, t.Elem()))
		}
	}
	return errors.Join(errs...)
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

	var root yaml.Node
	if err := yaml.Unmarshal(data, &root); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}

	var cfg = Config{
		Scrape: ScrapeConfig{
			Manifests:       ManifestsConfig{Enabled: true},
			SmallFileBytes:  DefaultSmallFileBytes,
			TargetFileBytes: DefaultTargetFileBytes,
		},
	}
	if root.Kind != 0 { // an empty file leaves the zero node
		// ${VAR} placeholders are handy for credentials. "$$" is an escaped
		// literal dollar sign. Unset variables are an error rather than
		// silently becoming empty.
		if err := expandEnv(&root); err != nil {
			return nil, err
		}
		if err := checkKnownFields(&root, reflect.TypeFor[Config]()); err != nil {
			return nil, fmt.Errorf("parse config: %w", err)
		}
		if err := root.Decode(&cfg); err != nil {
			return nil, fmt.Errorf("parse config: %w", err)
		}
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
