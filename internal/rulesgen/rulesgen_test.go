package rulesgen

import (
	"bytes"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func readRepoFile(t *testing.T, rel string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "..", rel))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// TestPrometheusRuleUpToDate fails when the committed PrometheusRule was not
// regenerated after the canonical rules changed.
func TestPrometheusRuleUpToDate(t *testing.T) {
	want, err := PrometheusRule(readRepoFile(t, CanonicalPath))
	if err != nil {
		t.Fatal(err)
	}
	if got := readRepoFile(t, PrometheusRulePath); !bytes.Equal(got, want) {
		t.Fatalf("%s is out of date with %s; run `make rules-generate`", PrometheusRulePath, CanonicalPath)
	}
}

// TestPrometheusRuleSpecMatchesCanonical checks the generated spec is
// semantically identical to the canonical rules file.
func TestPrometheusRuleSpecMatchesCanonical(t *testing.T) {
	canonical := readRepoFile(t, CanonicalPath)
	gen, err := PrometheusRule(canonical)
	if err != nil {
		t.Fatal(err)
	}

	var want map[string]any
	if err := yaml.Unmarshal(canonical, &want); err != nil {
		t.Fatal(err)
	}
	var got struct {
		APIVersion string `yaml:"apiVersion"`
		Kind       string `yaml:"kind"`
		Metadata   struct {
			Name string `yaml:"name"`
		} `yaml:"metadata"`
		Spec map[string]any `yaml:"spec"`
	}
	if err := yaml.Unmarshal(gen, &got); err != nil {
		t.Fatal(err)
	}
	if got.APIVersion != "monitoring.coreos.com/v1" || got.Kind != "PrometheusRule" || got.Metadata.Name != Name {
		t.Errorf("header = %s/%s/%s", got.APIVersion, got.Kind, got.Metadata.Name)
	}
	if !reflect.DeepEqual(got.Spec, want) {
		t.Error("generated spec differs from the canonical rules")
	}
	if !strings.HasPrefix(string(gen), "# Code generated") {
		t.Error("generated file lacks the generated-code header")
	}
}

func TestPrometheusRuleRejectsUnexpectedShape(t *testing.T) {
	for _, in := range []string{
		"- a\n",
		"groups: []\nextra: 1\n",
		"rules: []\n",
	} {
		if _, err := PrometheusRule([]byte(in)); err == nil {
			t.Errorf("PrometheusRule(%q) succeeded, want error", in)
		}
	}
}
