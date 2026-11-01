package rules

import (
	"reflect"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

type promRule struct {
	APIVersion string `yaml:"apiVersion"`
	Kind       string `yaml:"kind"`
	Metadata   struct {
		Name      string            `yaml:"name"`
		Namespace string            `yaml:"namespace"`
		Labels    map[string]string `yaml:"labels"`
	} `yaml:"metadata"`
	Spec map[string]any `yaml:"spec"`
}

func TestEmbeddedRulesParse(t *testing.T) {
	var f struct {
		Groups []struct {
			Name  string           `yaml:"name"`
			Rules []map[string]any `yaml:"rules"`
		} `yaml:"groups"`
	}
	if err := yaml.Unmarshal(Prometheus, &f); err != nil {
		t.Fatal(err)
	}
	if len(f.Groups) == 0 || len(f.Groups[0].Rules) == 0 {
		t.Fatal("embedded rules file has no rules")
	}
}

func TestPrometheusRuleSpecMatchesCanonical(t *testing.T) {
	gen, err := PrometheusRule(Options{})
	if err != nil {
		t.Fatal(err)
	}
	var want map[string]any
	if err := yaml.Unmarshal(Prometheus, &want); err != nil {
		t.Fatal(err)
	}
	var got promRule
	if err := yaml.Unmarshal(gen, &got); err != nil {
		t.Fatal(err)
	}
	if got.APIVersion != "monitoring.coreos.com/v1" || got.Kind != "PrometheusRule" {
		t.Errorf("type = %s/%s", got.APIVersion, got.Kind)
	}
	if got.Metadata.Name != DefaultName || got.Metadata.Namespace != "" {
		t.Errorf("metadata name/namespace = %q/%q, want %q/\"\"", got.Metadata.Name, got.Metadata.Namespace, DefaultName)
	}
	if !reflect.DeepEqual(got.Metadata.Labels, map[string]string{"app.kubernetes.io/name": "iceberg-exporter"}) {
		t.Errorf("labels = %v", got.Metadata.Labels)
	}
	if !reflect.DeepEqual(got.Spec, want) {
		t.Error("generated spec differs from the canonical rules")
	}
	if !strings.Contains(string(gen), "# --- Snapshot retention") {
		t.Error("rule comments were not preserved")
	}
}

func TestPrometheusRuleOptions(t *testing.T) {
	gen, err := PrometheusRule(Options{
		Name:      "iceberg",
		Namespace: "monitoring",
		Labels:    map[string]string{"release": "prometheus", "app.kubernetes.io/name": "custom"},
	})
	if err != nil {
		t.Fatal(err)
	}
	var got promRule
	if err := yaml.Unmarshal(gen, &got); err != nil {
		t.Fatal(err)
	}
	if got.Metadata.Name != "iceberg" || got.Metadata.Namespace != "monitoring" {
		t.Errorf("metadata name/namespace = %q/%q", got.Metadata.Name, got.Metadata.Namespace)
	}
	want := map[string]string{"release": "prometheus", "app.kubernetes.io/name": "custom"}
	if !reflect.DeepEqual(got.Metadata.Labels, want) {
		t.Errorf("labels = %v, want %v", got.Metadata.Labels, want)
	}
}

func TestPrometheusRuleRejectsUnexpectedShape(t *testing.T) {
	for _, in := range []string{"- a\n", "groups: []\nextra: 1\n", "rules: []\n"} {
		if _, err := prometheusRule([]byte(in), Options{}); err == nil {
			t.Errorf("prometheusRule(%q) succeeded, want error", in)
		}
	}
}
