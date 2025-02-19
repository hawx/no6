package no6

import (
	"testing"

	"hawx.me/code/assert"
)

func TestTyperCompare(t *testing.T) {
	testcases := map[string]struct {
		a, b   any
		result int
	}{
		"strings equal": {
			a:      "hello world",
			b:      "hello world",
			result: 0,
		},
		"strings less": {
			a:      "hella world",
			b:      "hello world",
			result: -1,
		},
		"strings greater": {
			a:      "hellz world",
			b:      "hello world",
			result: 1,
		},
		"ints equals": {
			a:      12345,
			b:      12345,
			result: 0,
		},
		"ints less": {
			a:      2345,
			b:      10000,
			result: -1,
		},
		"ints greater": {
			a:      20000,
			b:      12345,
			result: 1,
		},
		"ints negative less": {
			a:      -100,
			b:      -10,
			result: -1,
		},
		"ints negative more": {
			a:      -999,
			b:      -1000,
			result: 1,
		},
	}

	typer := &Typer{}

	for scenario, tc := range testcases {
		t.Run(scenario, func(t *testing.T) {
			assert.Equal(t, tc.result, typer.Compare(typer.Format(tc.a), typer.Format(tc.b)))
		})
	}
}
