package no6

import (
	"os"
	"testing"

	"go.etcd.io/bbolt"
	"hawx.me/code/assert"
)

func BenchmarkBoltPut(b *testing.B) {
	file, _ := os.CreateTemp("", "")
	file.Close()
	defer os.Remove(file.Name())

	db, _ := bbolt.Open(file.Name(), 0600, nil)

	for n := 0; n < b.N; n++ {
		db.Update(func(tx *bbolt.Tx) error {
			bucket, _ := tx.CreateBucketIfNotExists([]byte("a"))
			bucket.Put([]byte("b"), []byte("c"))
			return nil
		})
	}
}

func BenchmarkStoreInsert(b *testing.B) {
	file, _ := os.CreateTemp("", "")
	file.Close()
	defer os.Remove(file.Name())

	store, _ := Open(file.Name())

	for n := 0; n < b.N; n++ {
		store.PutTriples(Triple{"john", "firstName", "John"})
	}
}

var benchTriples []Triple

func BenchmarkStoreQuery(b *testing.B) {
	file, _ := os.CreateTemp("", "")
	file.Close()
	defer os.Remove(file.Name())

	store, _ := Open(file.Name())

	store.PutTriples(
		Triple{"john", "firstName", "John"},
		Triple{"john", "lastName", "Smith"},
		Triple{"john", "age", "20"}, // TODO: types other than string
		Triple{"john", "knows", "dave"},
		Triple{"john", "knows", "mike"},
		Triple{"dave", "firstName", "Dave"},
		Triple{"dave", "lastName", "Davidson"},
		Triple{"dave", "age", "30"},
	)

	for n := 0; n < b.N; n++ {
		benchTriples = store.Query(Predicates("knows"))
	}
}

func TestSimpleQuery(t *testing.T) {
	file, _ := os.CreateTemp("", "")
	file.Close()
	defer os.Remove(file.Name())

	store, _ := Open(file.Name())

	store.PutTriples(
		Triple{"john", "firstName", "John"},
		Triple{"john", "lastName", "Smith"},
		Triple{"john", "age", 20},
		Triple{"john", "knows", "dave"},
		Triple{"john", "knows", "mike"},
		Triple{"dave", "firstName", "Dave"},
		Triple{"dave", "lastName", "Davidson"},
		Triple{"dave", "age", 30},
	)

	// * P *
	t.Run("predicate", func(t *testing.T) {
		assert.Equal(t,
			[]Triple{{"john", "knows", "dave"}, {"john", "knows", "mike"}},
			store.Query(Predicates("knows")),
		)
	})

	// S P *
	t.Run("subject-predicate", func(t *testing.T) {
		assert.Equal(t,
			[]Triple{{"john", "age", 20}},
			store.Query(Subjects("john"), Predicates("age")),
			// store.QueryValues(Subjects("john"), Predicates("age")) => []any{20}
		)
	})

	// * P O
	t.Run("predicate-object", func(t *testing.T) {
		assert.Equal(t,
			[]Triple{{"dave", "age", 30}},
			store.Query(Predicates("age").Eq(30)),
			// store.QuerySubjects(Predicates("age").Eq(30)) => []string{"save"}
		)
	})

	// S * O
	t.Run("subject-object", func(t *testing.T) {
		assert.Equal(t,
			[]Triple{{"dave", "age", 30}},
			store.Query(Predicates("age", "knows", "firstName", "lastName").Eq(30)),
		)
		// store.QueryHas(Predicates("age", "knows", "firstName", "lastName").Eq(30)) => true
	})

	// S * *
	t.Run("subject", func(t *testing.T) {
		assert.Equal(t,
			[]Triple{
				{"dave", "age", 30},
				{"dave", "firstName", "Dave"},
				{"dave", "lastName", "Davidson"},
			},
			store.Query(Subjects("dave"), Predicates("age", "knows", "firstName", "lastName")),
			// store.Query(Subjects("dave"), Predicates("age", "knows", "firstName", "lastName"))
		)
	})

	// API should be nicer:
	//
	// 1. A way of determining if something exists, returns true/false:
	//
	//   Has(Predicates("a")) => true if (?/a/?) exists
	//   Has(Subjects("x"), Predicates("a")) => true if (x/a/?) exists
	//   Has(Predicates("a").Eq(1)) => true if (x/a/1) exists
	//
	// 2. A way of getting matching subjects:
	//
	//   QuerySubjects(Predicates("a")) => returns all subjects where (?/a/?) exists
	//   QuerySubjects(Subjects("x"), Predicates("a")) => returns all subjects where (?/a/?) exists
	//   QuerySubjects(Predicates("a").Eq(1)) => returns all subjects where (?/a/1) exists
	//
	// 3. A way of getting matching objects:
	//
	//   QueryObjects(Predicates("a")) => returns all objects where (?/a/?) exists
	//   QueryObjects(Subjects("x"), Predicates("a")) => returns all objects where (?/a/?) exists
	//   QueryObjects(Predicates("a").Gt(1)) => returns all objects where (?/a/>1) exists
	//
	// 4. Existing Query which returns Triples.
	//
	// 5. CountSubjects and CountObjects to return counts instead of the data.
	//
	// 6. Need a limit and sort to allow paging.
}

func TestQuerySortLimit(t *testing.T) {
	file, _ := os.CreateTemp("", "")
	file.Close()
	defer os.Remove(file.Name())

	store, _ := Open(file.Name())

	store.PutTriples(
		Triple{"a", "size", 1},
		Triple{"b", "size", 4},
		Triple{"c", "size", 2},
		Triple{"d", "size", 5},
		Triple{"e", "size", 3},
	)

	assert.Equal(t, []string{"a", "c", "e", "b"},
		store.QuerySubjects(
			Predicates("size"),
			Sort("size"),
			Limit(4),
		),
	)
}

func TestQuerySubject(t *testing.T) {
	file, _ := os.CreateTemp("", "")
	file.Close()
	defer os.Remove(file.Name())

	store, _ := Open(file.Name())

	store.PutTriples(
		Triple{"john", "lives-in", "sf"},
		Triple{"john", "eats", "sushi"},
		Triple{"john", "eats", "indian"},
		Triple{"dave", "lives-in", "nyc"},
		Triple{"dave", "eats", "thai"},
		Triple{"adam", "lives-in", "sf"},
		Triple{"adam", "eats", "thai"},
	)

	assert.Equal(t, []string{"john"},
		store.QuerySubjects(
			Predicates("lives-in").Eq("sf"),
			Predicates("eats").Eq("sushi"),
		),
	)
}

func TestQuerySorting(t *testing.T) {
	file, _ := os.CreateTemp("", "")
	file.Close()
	defer os.Remove(file.Name())

	store, _ := Open(file.Name())

	store.PutTriples(
		Triple{"x", "count", "1"},
		Triple{"x", "count", "3"},
		Triple{"x", "count", "5"},
		Triple{"y", "count", "2"},
		Triple{"y", "count", "4"},
		Triple{"y", "count", "6"},
	)

	t.Run("Eq", func(t *testing.T) {
		assert.Equal(t, []Triple{
			{"x", "count", "3"},
		}, store.Query(Predicates("count").Eq("3")))
	})

	t.Run("Ne", func(t *testing.T) {
		assert.Equal(t, []Triple{
			{"x", "count", "1"},
			{"x", "count", "5"},
			{"y", "count", "2"},
			{"y", "count", "4"},
			{"y", "count", "6"},
		}, store.Query(Predicates("count").Ne("3")))
	})

	t.Run("Lt", func(t *testing.T) {
		assert.Equal(t, []Triple{
			{"x", "count", "1"},
			{"y", "count", "2"},
		}, store.Query(Predicates("count").Lt("3")))
	})

	t.Run("Gt", func(t *testing.T) {
		assert.Equal(t, []Triple{
			{"x", "count", "5"},
			{"y", "count", "4"},
			{"y", "count", "6"},
		}, store.Query(Predicates("count").Gt("3")))
	})
}

func TestQueryIntSorting(t *testing.T) {
	file, _ := os.CreateTemp("", "")
	file.Close()
	defer os.Remove(file.Name())

	store, _ := Open(file.Name())

	store.PutTriples(
		Triple{"x", "count", 1},
		Triple{"x", "count", 3},
		Triple{"x", "count", 5},
		Triple{"y", "count", 2},
		Triple{"y", "count", 4},
		Triple{"y", "count", 6},
	)

	t.Run("Eq", func(t *testing.T) {
		assert.Equal(t, []Triple{
			{"x", "count", 3},
		}, store.Query(Predicates("count").Eq(3)))
	})

	t.Run("Ne", func(t *testing.T) {
		assert.Equal(t, []Triple{
			{"x", "count", 1},
			{"x", "count", 5},
			{"y", "count", 2},
			{"y", "count", 4},
			{"y", "count", 6},
		}, store.Query(Predicates("count").Ne(3)))
	})

	t.Run("Lt", func(t *testing.T) {
		assert.Equal(t, []Triple{
			{"x", "count", 1},
			{"y", "count", 2},
		}, store.Query(Predicates("count").Lt(3)))
	})

	t.Run("Gt", func(t *testing.T) {
		assert.Equal(t, []Triple{
			{"x", "count", 5},
			{"y", "count", 4},
			{"y", "count", 6},
		}, store.Query(Predicates("count").Gt(3)))
	})
}

func TestUseCaseMicropub(t *testing.T) {
	file, _ := os.CreateTemp("", "")
	file.Close()
	defer os.Remove(file.Name())

	store, _ := Open(file.Name())

	store.PutTriples(
		// https://micropub.spec.indieweb.org/ EXAMPLE 1
		Triple{"uid1", "type", "h-entry"},
		Triple{"uid1", "content", "hello world"},
		Triple{"uid1", "category", "foo"},
		Triple{"uid1", "category", "bar"},

		// https://micropub.spec.indieweb.org/ EXAMPLE 6
		Triple{"uid2", "type", "h-entry"},
		Triple{"uid2", "summary", "Weighed 70.64 kg"},
		Triple{"uid2", "weight", "uid3"},
		Triple{"uid2", "bodyfat", "uid4"},

		Triple{"uid3", "type", "h-measure"},
		Triple{"uid3", "num", "70.64"},
		Triple{"uid3", "unit", "kg"},

		Triple{"uid4", "type", "h-measure"},
		Triple{"uid4", "num", "19.83"},
		Triple{"uid4", "unit", "%"},
	)
}
