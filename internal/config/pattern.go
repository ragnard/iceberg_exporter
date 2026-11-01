package config

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

// Patterns match dotted namespace and table names, with "." as the segment
// separator:
//
//   - "*" matches any characters within one segment, never a ".". As a whole
//     segment it matches exactly one segment, so "sales.*.orders" matches
//     "sales.eu.orders" but not "sales.eu.daily.orders".
//   - "**" as a whole segment matches zero or more segments: "sales.**"
//     matches "sales", "sales.eu" and "sales.eu.daily"; "**.staging" matches
//     "staging" and "a.b.staging"; "a.**.b" matches "a.b" and "a.x.y.b".
//     Inside a segment ("**_staging") it matches any characters, dots
//     included.
//   - A trailing ".*" also matches the parent: "sales.*" matches "sales" and
//     "sales.eu", but not "sales.eu.daily".
//   - Every other character is literal.
//
// Patterns are anchored: they must match the whole name.

// Matcher is a compiled Filter.
type Matcher struct {
	include []*regexp.Regexp
	exclude []*regexp.Regexp
}

// CompileFilter validates and compiles a filter.
func CompileFilter(f Filter) (*Matcher, error) {
	m := &Matcher{}
	var errs []error
	for _, p := range f.Include {
		re, err := compilePattern(p)
		if err != nil {
			errs = append(errs, err)
		}
		m.include = append(m.include, re)
	}
	for _, p := range f.Exclude {
		re, err := compilePattern(p)
		if err != nil {
			errs = append(errs, err)
		}
		m.exclude = append(m.exclude, re)
	}
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}
	return m, nil
}

// Match reports whether name matches an include pattern and no exclude
// pattern.
func (m *Matcher) Match(name string) bool {
	return matchAny(m.include, name) && !matchAny(m.exclude, name)
}

func matchAny(res []*regexp.Regexp, name string) bool {
	for _, re := range res {
		if re.MatchString(name) {
			return true
		}
	}
	return false
}

// compilePattern translates a pattern into an anchored regular expression.
func compilePattern(p string) (*regexp.Regexp, error) {
	if p == "" {
		return nil, errors.New("invalid pattern \"\": must not be empty")
	}
	if strings.Contains(p, "***") {
		return nil, fmt.Errorf("invalid pattern %q: use * or **, not ***", p)
	}
	var segs []string
	for _, seg := range strings.Split(p, ".") {
		if seg == "" {
			return nil, fmt.Errorf("invalid pattern %q: empty segment", p)
		}
		// "**.**" means the same as "**".
		if seg == "**" && len(segs) > 0 && segs[len(segs)-1] == "**" {
			continue
		}
		segs = append(segs, seg)
	}

	var b strings.Builder
	b.WriteString("^")
	// sep is true when the next segment must be preceded by a dot, i.e.
	// after any segment except a leading "**" (which consumes its own dot).
	sep := false
	for i, seg := range segs {
		last := i == len(segs)-1
		switch {
		case seg == "**" && !sep && last:
			b.WriteString(`.*`)
		case seg == "**" && !sep:
			// Zero or more leading segments, each followed by its dot.
			b.WriteString(`(?:.*\.)?`)
		case seg == "**":
			// Zero or more further segments, each preceded by its dot.
			b.WriteString(`(?:\..*)?`)
		case seg == "*" && sep && last:
			// A trailing ".*" also matches the parent.
			b.WriteString(`(?:\.[^.]+)?`)
		default:
			if sep {
				b.WriteString(`\.`)
			}
			writeSegment(&b, seg)
		}
		sep = !(seg == "**" && !sep)
	}
	b.WriteString("$")
	return regexp.MustCompile(b.String()), nil
}

// writeSegment writes one non-globstar segment: "**" spans dots, "*" stays
// within the segment, everything else is literal.
func writeSegment(b *strings.Builder, seg string) {
	if seg == "*" {
		b.WriteString(`[^.]+`)
		return
	}
	for len(seg) > 0 {
		switch {
		case strings.HasPrefix(seg, "**"):
			b.WriteString(`.*`)
			seg = seg[2:]
		case seg[0] == '*':
			b.WriteString(`[^.]*`)
			seg = seg[1:]
		default:
			n := strings.IndexByte(seg, '*')
			if n < 0 {
				n = len(seg)
			}
			b.WriteString(regexp.QuoteMeta(seg[:n]))
			seg = seg[n:]
		}
	}
}
