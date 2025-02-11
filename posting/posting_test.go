package posting

import (
	"log"
	"os"
	"testing"

	"hawx.me/code/assert"
)

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
	assert.Equal(t,
		[]Triple{{"john", "knows", "dave"}, {"john", "knows", "mike"}},
		store.Query(Anything, "knows", Anything),
	)

	// S P *
	assert.Equal(t,
		[]Triple{{"john", "age", "20"}},
		store.Query("john", "age", Anything),
	)

	// * P O
	assert.Equal(t,
		[]Triple{{"dave", "age", "30"}},
		store.Query(Anything, "age", "30"),
	)
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
	)

	log.Println(store.Query(Anything, "lives-in", "sf"))
	log.Println(store.Query(Anything, "eats", "sushi"))

	assert.Equal(t, []string{"john"},
		store.QuerySubject(PredicateObject("lives-in", "sf"), PredicateObject("eats", "sushi")))
}
