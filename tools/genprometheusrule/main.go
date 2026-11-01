// Command genprometheusrule writes the Prometheus Operator PrometheusRule
// manifest generated from the canonical rules file. Run it from the
// repository root (make rules-generate).
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/ragnard/iceberg_exporter/internal/rulesgen"
)

func main() {
	in := flag.String("in", rulesgen.CanonicalPath, "canonical Prometheus rules file")
	out := flag.String("out", rulesgen.PrometheusRulePath, "PrometheusRule manifest to write")
	flag.Parse()

	src, err := os.ReadFile(*in)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	gen, err := rulesgen.PrometheusRule(src)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := os.WriteFile(*out, gen, 0o644); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
