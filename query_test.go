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

func TestRemove(t *testing.T) {
	testcases := map[string]struct {
		a []uint64
		b uint64
		r []uint64
	}{
		"nil": {},
		"empty": {
			a: []uint64{},
			b: 5,
			r: []uint64{},
		},
		"first": {
			a: []uint64{1, 2, 3},
			b: 1,
			r: []uint64{2, 3},
		},
		"middle": {
			a: []uint64{1, 2, 3},
			b: 2,
			r: []uint64{1, 3},
		},
		"last": {
			a: []uint64{1, 2, 3},
			b: 3,
			r: []uint64{1, 2},
		},
		"missing": {
			a: []uint64{1, 2, 4},
			b: 3,
			r: []uint64{1, 2, 4},
		},
	}

	for scenario, tc := range testcases {
		t.Run(scenario, func(t *testing.T) {
			assert.Equal(t, tc.r, remove(tc.a, tc.b))
		})
	}
}
