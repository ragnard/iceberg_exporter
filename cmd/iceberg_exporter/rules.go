package main

import (
	"flag"
	"fmt"
	"io"
	"strings"

	"github.com/ragnard/iceberg_exporter/internal/rules"
)

const rulesUsage = `Usage: iceberg_exporter rules [flags]

Print the embedded table health rules to stdout, either as a Prometheus rules
file (for rule_files) or as a Prometheus Operator PrometheusRule.

Flags:
`

// runRules implements the `rules` subcommand.
func runRules(args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("rules", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() {
		fmt.Fprint(stderr, rulesUsage)
		fs.PrintDefaults()
	}

	format := fs.String("format", "prometheus", "output format: prometheus or prometheusrule")
	opts := rules.Options{Labels: map[string]string{}}
	fs.StringVar(&opts.Name, "name", rules.DefaultName, "PrometheusRule metadata.name")
	fs.StringVar(&opts.Namespace, "namespace", "", "PrometheusRule metadata.namespace (omitted when empty)")
	fs.Func("label", "PrometheusRule label as key=value, e.g. release=prometheus (repeatable)", func(s string) error {
		k, v, ok := strings.Cut(s, "=")
		if !ok || k == "" {
			return fmt.Errorf("want key=value, got %q", s)
		}
		opts.Labels[k] = v
		return nil
	})
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() > 0 {
		fs.Usage()
		return fmt.Errorf("rules: unexpected arguments: %s", strings.Join(fs.Args(), " "))
	}

	switch *format {
	case "prometheus":
		if opts.Namespace != "" || len(opts.Labels) > 0 || opts.Name != rules.DefaultName {
			return fmt.Errorf("rules: -name, -namespace and -label require -format prometheusrule")
		}
		_, err := stdout.Write(rules.Prometheus)
		return err
	case "prometheusrule":
		out, err := rules.PrometheusRule(opts)
		if err != nil {
			return err
		}
		_, err = stdout.Write(out)
		return err
	default:
		return fmt.Errorf("rules: unknown -format %q (want prometheus or prometheusrule)", *format)
	}
}
