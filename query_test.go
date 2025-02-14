package no6

import (
	"testing"

	"hawx.me/code/assert"
)

func TestIntersect(t *testing.T) {
	testcases := map[string]struct {
		a, b, r []uint64
	}{
		"nil": {},
		"empty": {
			a: []uint64{},
			b: []uint64{},
			r: []uint64{},
		},
		"simple": {
			a: []uint64{1, 2, 3},
			b: []uint64{2, 3, 4},
			r: []uint64{2, 3},
		},
	}

	for scenario, tc := range testcases {
		t.Run(scenario, func(t *testing.T) {
			assert.Equal(t, tc.r, intersect(tc.a, tc.b))
		})
	}
}
