package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/ragnard/iceberg_exporter/internal/rules"
)

func TestRunRules(t *testing.T) {
	for _, tc := range []struct {
		name    string
		args    []string
		wantErr string
		check   func(t *testing.T, out string)
	}{
		{
			name: "default prints the plain rules file",
			check: func(t *testing.T, out string) {
				if out != string(rules.Prometheus) {
					t.Error("output differs from the embedded rules file")
				}
			},
		},
		{
			name: "prometheusrule with metadata",
			args: []string{"-format", "prometheusrule", "-namespace", "monitoring", "-label", "release=prometheus"},
			check: func(t *testing.T, out string) {
				for _, want := range []string{"kind: PrometheusRule", "namespace: monitoring", "release: prometheus", "groups:"} {
					if !strings.Contains(out, want) {
						t.Errorf("output lacks %q", want)
					}
				}
			},
		},
		{name: "unknown format", args: []string{"-format", "json"}, wantErr: "unknown -format"},
		{name: "bad label", args: []string{"-format", "prometheusrule", "-label", "nope"}, wantErr: "key=value"},
		{name: "metadata flags need prometheusrule", args: []string{"-label", "a=b"}, wantErr: "require -format prometheusrule"},
		{name: "extra arguments", args: []string{"extra"}, wantErr: "unexpected arguments"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			err := runRules(tc.args, &stdout, &stderr)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want containing %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			tc.check(t, stdout.String())
		})
	}
}
