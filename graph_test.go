package no6

import (
	"testing"

	"hawx.me/code/assert"
)

func TestGraph(t *testing.T) {
	graph := &Store{}
	graph.Insert(
		Triple{"john", "firstName", "John"},
		Triple{"john", "lastName", "Smith"},
		Triple{"john", "age", "20"}, // TODO: types other than string
	)

	assert.Equal(t, "John", graph.Get("john", "firstName"))
	assert.Equal(t, "", graph.Get("dave", "firstName"))
}

func TestGraphQuery(t *testing.T) {
	graph := &Store{}
	graph.Insert(
		Triple{"john", "firstName", "John"},
		Triple{"john", "lastName", "Smith"},
		Triple{"dave", "firstName", "Dave"},
		Triple{"dave", "lastName", "Smith"},
	)

	allResults := graph.Query(Any, Any, Any)
	assert.Equal(t, []Triple{
		{Subject: "john", Predicate: "firstName", Object: "John"},
		{Subject: "john", Predicate: "lastName", Object: "Smith"},
		{Subject: "dave", Predicate: "firstName", Object: "Dave"},
		{Subject: "dave", Predicate: "lastName", Object: "Smith"},
	}, allResults)

	predicateResults := graph.Query(Any, "firstName", Any)
	assert.Equal(t, []Triple{
		{Subject: "john", Predicate: "firstName", Object: "John"},
		{Subject: "dave", Predicate: "firstName", Object: "Dave"},
	}, predicateResults)

	subjectResults := graph.Query("john", Any, Any)
	assert.Equal(t, []Triple{
		{Subject: "john", Predicate: "firstName", Object: "John"},
		{Subject: "john", Predicate: "lastName", Object: "Smith"},
	}, subjectResults)

	objectResults := graph.Query(Any, Any, "Smith")
	assert.Equal(t, []Triple{
		{Subject: "john", Predicate: "lastName", Object: "Smith"},
		{Subject: "dave", Predicate: "lastName", Object: "Smith"},
	}, objectResults)

	specificResults := graph.Query("john", "lastName", "Smith")
	assert.Equal(t, []Triple{
		{Subject: "john", Predicate: "lastName", Object: "Smith"},
	}, specificResults)

	missingResults := graph.Query("dave", "firstName", "John")
	assert.Equal(t, []Triple(nil), missingResults)
}
