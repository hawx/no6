// The idea here is, to copy dgraph, storing things as (subject+predicate,
// objects) pairs instead of (subject, predicate, object). Then predicates are
// split into buckets. We'll see how it goes.
package no6

import (
	"log/slog"
	"os"

	"go.etcd.io/bbolt"
)

const Anything = "__Anything__"

var (
	// The id bucket contains a single record, the last ID that was used. This
	// allows assigning a newly incremented ID for each subject and object.
	bucketID = []byte("id")
	keyLast  = []byte("last")

	// The data bucket contains mappings of each ID (should it be merged with
	// above?) to the value it represents. This is maintained in both
	// directions. So a value X will mean the bucket contains (X, ID(X)), and
	// (ID(X), X) pairs.
	//
	// TODO: is it necessary to have both, and what about collisions?
	//
	// TODO: would be good to have some indexers, so we can store other things. So
	// have for indexer f, store (f(X), ID(X)) and (ID(X), X) pairs.
	bucketData = []byte("data")

	// The predicates bucket specifies all predicates (as keys), to support
	// querying over all predicates.
	bucketPredicates = []byte("predicates")

	// And to finish off the structure: the predicate-* bucket contains
	// postinglists for that predicate.
)

type Triple struct {
	Subject   string
	Predicate string
	Object    any
}

type Store struct {
	db     *bbolt.DB
	logger *slog.Logger
	typer  *Typer
}

func Open(path string) (*Store, error) {
	db, err := bbolt.Open(path, 0600, nil)
	if err != nil {
		return nil, err
	}

	handler := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		// Level: slog.LevelDebug,
	})
	logger := slog.New(handler)

	return &Store{
		db:     db,
		logger: logger,
		typer:  &Typer{},
	}, nil
}
