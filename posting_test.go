package no6

import (
	"os"
	"testing"

	"hawx.me/code/assert"
)

func BenchmarkStoreInsert(b *testing.B) {
	file, _ := os.CreateTemp("", "")
	file.Close()
	defer os.Remove(file.Name())

	store, _ := Open(file.Name())

	for n := 0; n < b.N; n++ {
		store.Insert(Triple{"john", "firstName", "John"})
	}
}

var benchTriples []Triple

func BenchmarkStoreQuery(b *testing.B) {
	file, _ := os.CreateTemp("", "")
	file.Close()
	defer os.Remove(file.Name())

	store, _ := Open(file.Name())

	store.Insert(
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
		benchTriples = store.Query(Anything, "knows", Eq, Anything)
	}
}

func TestSimpleQuery(t *testing.T) {
	file, _ := os.CreateTemp("", "")
	file.Close()
	defer os.Remove(file.Name())

	store, _ := Open(file.Name())

	store.Insert(
		Triple{"john", "firstName", "John"},
		Triple{"john", "lastName", "Smith"},
		Triple{"john", "age", "20"}, // TODO: types other than string
		Triple{"john", "knows", "dave"},
		Triple{"john", "knows", "mike"},
		Triple{"dave", "firstName", "Dave"},
		Triple{"dave", "lastName", "Davidson"},
		Triple{"dave", "age", "30"},
	)

	// * P *
	t.Run("predicate", func(t *testing.T) {
		assert.Equal(t,
			[]Triple{{"john", "knows", "dave"}, {"john", "knows", "mike"}},
			store.Query(Anything, "knows", Eq, Anything),
		)
	})

	// S P *
	t.Run("subject-predicate", func(t *testing.T) {
		assert.Equal(t,
			[]Triple{{"john", "age", "20"}},
			store.Query("john", "age", Eq, Anything),
		)
	})

	// * P O
	t.Run("predicate-object", func(t *testing.T) {
		assert.Equal(t,
			[]Triple{{"dave", "age", "30"}},
			store.Query(Anything, "age", Eq, "30"),
		)
	})

	// S * O
	t.Run("subject-object", func(t *testing.T) {
		assert.Equal(t,
			[]Triple{{"dave", "age", "30"}},
			store.Query("dave", Anything, Eq, "30"),
		)
	})

	// S * *
	t.Run("subject", func(t *testing.T) {
		assert.Equal(t,
			[]Triple{
				{"dave", "age", "30"},
				{"dave", "firstName", "Dave"},
				{"dave", "lastName", "Davidson"},
			},
			store.Query("dave", Anything, Eq, Anything),
		)
	})
}

func TestQuerySubject(t *testing.T) {
	file, _ := os.CreateTemp("", "")
	file.Close()
	defer os.Remove(file.Name())

	store, _ := Open(file.Name())

	store.Insert(
		Triple{"john", "lives-in", "sf"},
		Triple{"john", "eats", "sushi"},
		Triple{"john", "eats", "indian"},
		Triple{"dave", "lives-in", "nyc"},
		Triple{"dave", "eats", "thai"},
		Triple{"adam", "lives-in", "sf"},
		Triple{"adam", "eats", "thai"},
	)

	assert.Equal(t, []string{"john"},
		store.QuerySubject(
			PredicateObject("lives-in", Eq, "sf"),
			PredicateObject("eats", Eq, "sushi"),
		),
	)
}

func TestQuerySorting(t *testing.T) {
	file, _ := os.CreateTemp("", "")
	file.Close()
	defer os.Remove(file.Name())

	store, _ := Open(file.Name())

	store.Insert(
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
		}, store.Query(Anything, "count", Eq, "3"))
	})

	t.Run("Ne", func(t *testing.T) {
		assert.Equal(t, []Triple{
			{"x", "count", "1"},
			{"x", "count", "5"},
			{"y", "count", "2"},
			{"y", "count", "4"},
			{"y", "count", "6"},
		}, store.Query(Anything, "count", Ne, "3"))
	})

	t.Run("Lt", func(t *testing.T) {
		assert.Equal(t, []Triple{
			{"x", "count", "1"},
			{"y", "count", "2"},
		}, store.Query(Anything, "count", Lt, "3"))
	})

	t.Run("Gt", func(t *testing.T) {
		assert.Equal(t, []Triple{
			{"x", "count", "5"},
			{"y", "count", "4"},
			{"y", "count", "6"},
		}, store.Query(Anything, "count", Gt, "3"))
	})
}

func TestUseCaseMicropub(t *testing.T) {
	file, _ := os.CreateTemp("", "")
	file.Close()
	defer os.Remove(file.Name())

	store, _ := Open(file.Name())

	store.Insert(
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
