package no6

import (
	"bytes"
)

// Indexers can be configured for different predicates, but apply across
// everything for that predicate. So an "age" can be considered as an int, but
// "name" as full text. Also if you know you won't need to search by
// "description" then it can be stored more efficiently by not making it
// searchable.

// I think maybe I am thinking about this wrong. Here are the things I want to
// be able to do:
//
// - Store a long piece of text more efficiently (store (Hash(X), ID(X)) and
//   (ID(X), X) pairs, maybe). This would allow equality but not ordering of
//   text.
//
// - Allow storing typed objects (I feel like string/bool/int/uint/float may be
//   enough?). And do this in a way that allows natural ordering.
//
// So perhaps the first is an indexer and the second is a typer?

type Indexer interface {
	// Index returns how the value should be stored. It may return the same input
	// slice.
	Index([]byte) []byte
	// Less returns true if a < b. We only need this operation as equality can
	// compare uids.
	Less(a, b []byte) bool
}

// A NilIndexer will not store objects in a way that can be queried or sorted.
type NilIndexer struct{}

func (i NilIndexer) Index(data []byte) []byte {
	return []byte{}
}

func (i NilIndexer) Less(a, b []byte) bool {
	return false
}

// A FullTextIndexer will store objects such that they can be queried and
// sorted.
type FullTextIndexer struct{}

func (i FullTextIndexer) Index(data []byte) []byte {
	return data
}

func (i FullTextIndexer) Less(a, b []byte) bool {
	return bytes.Compare(a, b) == -1
}
