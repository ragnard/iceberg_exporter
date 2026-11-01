package config

import "testing"

func TestPatternMatch(t *testing.T) {
	tests := []struct {
		pattern string
		match   []string
		noMatch []string
	}{
		{"**", []string{"", "a", "a.b.c"}, nil},
		{"*", []string{"sales"}, []string{"sales.eu", ""}},
		{"sales", []string{"sales"}, []string{"sales.eu", "salesx", "x.sales"}},
		// A trailing ".*" matches the parent and exactly one level below.
		{"sales.*", []string{"sales", "sales.eu"}, []string{"sales.eu.daily", "salesx", "sales2.eu"}},
		// A trailing ".**" matches the parent and any depth below.
		{"sales.**", []string{"sales", "sales.eu", "sales.eu.daily"}, []string{"salesx", "x.sales"}},
		// A middle "*" is exactly one segment.
		{"sales.*.orders", []string{"sales.eu.orders"}, []string{"sales.orders", "sales.eu.daily.orders"}},
		// A middle "**" is zero or more segments.
		{"sales.**.orders", []string{"sales.orders", "sales.eu.orders", "sales.eu.daily.orders"}, []string{"sales.eu.ordersx"}},
		// A leading "**" is zero or more segments.
		{"**.staging", []string{"staging", "a.staging", "a.b.staging"}, []string{"a.stagingx", "xstaging"}},
		{"*.staging", []string{"a.staging"}, []string{"staging", "a.b.staging"}},
		// "*" inside a segment stays within it.
		{"tmp*", []string{"tmp", "tmp_1"}, []string{"tmp.x"}},
		{"*_staging", []string{"orders_staging"}, []string{"ns.orders_staging"}},
		{"ns.*_staging", []string{"ns.orders_staging"}, []string{"ns.orders"}},
		// "**" inside a segment spans dots.
		{"**_staging", []string{"orders_staging", "ns.orders_staging"}, []string{"ns.orders"}},
		// Everything else is literal.
		{"a+b.(c)?", []string{"a+b.(c)?"}, []string{"aab.c", "a+b.c"}},
		{"**.**.x", []string{"x", "a.b.x"}, []string{"a.xb"}},
		{"**.*", []string{"a", "a.b"}, []string{""}},
	}
	for _, tt := range tests {
		re, err := compilePattern(tt.pattern)
		if err != nil {
			t.Errorf("compilePattern(%q) error = %v", tt.pattern, err)
			continue
		}
		for _, name := range tt.match {
			if !re.MatchString(name) {
				t.Errorf("%q should match %q (regexp %s)", tt.pattern, name, re)
			}
		}
		for _, name := range tt.noMatch {
			if re.MatchString(name) {
				t.Errorf("%q should not match %q (regexp %s)", tt.pattern, name, re)
			}
		}
	}
}

func TestPatternInvalid(t *testing.T) {
	for _, p := range []string{"", ".", "a..b", ".a", "a.", "a***", "***"} {
		if _, err := compilePattern(p); err == nil {
			t.Errorf("compilePattern(%q) succeeded, want error", p)
		}
	}
}
