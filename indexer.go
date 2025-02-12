package no6

// Indexers can be configured for different predicates, but apply across
// everything for that predicate. So an "age" can be considered as an int, but
// "name" as full text. Also if you know you won't need to search by
// "description" then it can be stored more efficiently by not making it
// searchable.

// A NilIndexer will not store objects in a way that can be queried or sorted.
type NilIndexer struct{}

// A FullTextIndexer will store objects such that they can be queried and
// sorted.
type FullTextIndexer struct{}

// An IntIndexer will store objects as ints, such that they can be queried and
// sorted.
type IntIndexer struct{}
