package no6

import (
	"os"
	"testing"

	"hawx.me/code/assert"
)

func TestDelete(t *testing.T) {
	file, _ := os.CreateTemp("", "")
	file.Close()
	defer os.Remove(file.Name())

	store, _ := Open(file.Name())

	store.PutTriples(
		Triple{"john", "firstName", "John"},
		Triple{"john", "lastName", "Smith"},
	)
	assert.Equal(t,
		[]Triple{{"john", "firstName", "John"}, {"john", "lastName", "Smith"}},
		store.Query(Predicates("firstName", "lastName")),
	)

	store.Delete("john", "firstName")
	assert.Equal(t,
		[]Triple{{"john", "lastName", "Smith"}},
		store.Query(Predicates("firstName", "lastName")),
	)

	store.Delete("john", "lastName")
	assert.Equal(t, []Triple(nil), store.Query(Predicates("firstName", "lastName")))
}
