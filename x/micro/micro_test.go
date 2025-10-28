package micro

import (
	"os"
	"testing"

	"github.com/google/uuid"
	"hawx.me/code/assert"
	"hawx.me/code/no6"
)

func newSubject(typ string) string {
	return typ + "/" + uuid.NewString()
}

func TestInsertFind(t *testing.T) {
	file, _ := os.CreateTemp("", "")
	file.Close()
	defer os.Remove(file.Name())

	store, _ := Open(file.Name(), newSubject)

	expectedBodyfat := map[string]any{
		"type": []string{"h-measure"},
		"properties": map[string]any{
			"num":  []string{"19.83"},
			"unit": []string{"%"},
		},
	}

	expectedEntry := map[string]any{
		"type": []string{"h-entry"},
		"properties": map[string]any{
			"summary": []string{"Weighed 70.64 kg"},
			"weight": []map[string]any{
				{
					"type": []string{"h-measure"},
					"properties": map[string]any{
						"num":  []string{"70.64"},
						"unit": []string{"kg"},
					},
				},
			},
			"bodyfat": []map[string]any{
				expectedBodyfat,
			},
		},
	}

	_, err := store.Insert(expectedEntry)
	assert.Nil(t, err)

	bodyfat, _ := store.Find([]string{"type", "summary", "weight", "bodyfat", "num", "unit"}, no6.Predicates("num").Eq("19.83"))
	assert.Equal(t, expectedBodyfat, bodyfat)

	entry, _ := store.Find([]string{"type", "summary", "weight", "bodyfat", "num", "unit"}, no6.Predicates("summary").Eq("Weighed 70.64 kg"))
	assert.Equal(t, expectedEntry, entry)
}

func TestInsertFindWhenPaged(t *testing.T) {
	file, _ := os.CreateTemp("", "")
	file.Close()
	defer os.Remove(file.Name())

	store, _ := Open(file.Name(), newSubject)

	post1 := map[string]any{
		"type": []string{"h-entry"},
		"properties": map[string]any{
			"url":       []string{"/hello-world"},
			"content":   []string{"Hello world"},
			"published": []string{"2021-01-02"},
		},
	}

	post2 := map[string]any{
		"type": []string{"h-entry"},
		"properties": map[string]any{
			"url":       []string{"/continue-world"},
			"content":   []string{"Continue world"},
			"published": []string{"2022-01-02"},
		},
	}

	post3 := map[string]any{
		"type": []string{"h-entry"},
		"properties": map[string]any{
			"url":       []string{"/continue-again-world"},
			"content":   []string{"Continue again world"},
			"published": []string{"2022-06-02"},
		},
	}

	post4 := map[string]any{
		"type": []string{"h-entry"},
		"properties": map[string]any{
			"url":       []string{"/finish-world"},
			"content":   []string{"Finish world"},
			"published": []string{"2023-01-02"},
		},
	}

	store.Insert(post1)
	store.Insert(post2)
	store.Insert(post3)
	store.Insert(post4)

	assert.Equal(t, []map[string]any{post3, post4}, store.FindAll(
		[]string{"type", "url", "content", "published"},
		no6.Predicates("type").Eq("h-entry"),
		no6.Predicates("published").Gt("2022-03-02")))

	assert.Equal(t, []map[string]any{post1, post2}, store.FindAll(
		[]string{"type", "url", "content", "published"},
		no6.Predicates("type").Eq("h-entry"),
		no6.Predicates("published").Lt("2022-03-02")))
}

func TestInsertPartial(t *testing.T) {
	file, _ := os.CreateTemp("", "")
	file.Close()
	defer os.Remove(file.Name())

	store, _ := Open(file.Name(), newSubject)

	// https://indieweb.org/Micropub#New_Note
	uid, _ := store.Insert(map[string]any{
		"type": []string{"h-card"},
		"properties": map[string]any{
			"name":           []string{"Ford Food and Drink"},
			"url":            []string{"http://www.fordfoodanddrink.com/"},
			"street-address": []string{"2505 SE 11th Ave"},
			"locality":       []string{"Portland"},
			"region":         []string{"OR"},
			"postal-code":    []string{"97214"},
			"geo":            []string{"geo:45.5048473,-122.6549551"},
			"tel":            []string{"(503) 236-3023"},
		},
	})

	store.Insert(map[string]any{
		"type": []string{"h-entry"},
		"properties": map[string]any{
			"location": []string{uid},
			"name":     []string{"Working on Micropub"},
			"category": []string{"indieweb"},
		},
	})

	assert.Equal(t, []map[string]any{
		{
			"type": []string{"h-entry"},
			"properties": map[string]any{
				"location": []map[string]any{{
					"type": []string{"h-card"},
					"properties": map[string]any{
						"name":           []string{"Ford Food and Drink"},
						"url":            []string{"http://www.fordfoodanddrink.com/"},
						"street-address": []string{"2505 SE 11th Ave"},
						"locality":       []string{"Portland"},
						"region":         []string{"OR"},
						"postal-code":    []string{"97214"},
						"geo":            []string{"geo:45.5048473,-122.6549551"},
						"tel":            []string{"(503) 236-3023"},
					},
				}},
				"name":     []string{"Working on Micropub"},
				"category": []string{"indieweb"},
			},
		},
	}, store.FindAll(
		[]string{"type", "location", "name", "category", "url", "street-address", "locality", "region", "postal-code", "geo", "tel"},
		no6.Predicates("type").Eq("h-entry")))
}

var benchErr error

func BenchmarkStoreInsert(b *testing.B) {
	file, _ := os.CreateTemp("", "")
	file.Close()
	defer os.Remove(file.Name())

	store, _ := Open(file.Name(), newSubject)

	post := map[string]any{
		"type": []string{"h-entry"},
		"properties": map[string]any{
			"url":       []string{"/finish-world"},
			"content":   []string{"Finish world"},
			"published": []string{"2023-01-02"},
		},
	}

	for n := 0; n < b.N; n++ {
		_, benchErr = store.Insert(post)
	}
}

var benchFound []map[string]any

func BenchmarkStoreFindAll(b *testing.B) {
	file, _ := os.CreateTemp("", "")
	file.Close()
	defer os.Remove(file.Name())

	store, _ := Open(file.Name(), newSubject)

	post1 := map[string]any{
		"type": []string{"h-entry"},
		"properties": map[string]any{
			"url":       []string{"/hello-world"},
			"content":   []string{"Hello world"},
			"published": []string{"2021-01-02"},
		},
	}

	post2 := map[string]any{
		"type": []string{"h-entry"},
		"properties": map[string]any{
			"url":       []string{"/continue-world"},
			"content":   []string{"Continue world"},
			"published": []string{"2022-01-02"},
		},
	}

	post3 := map[string]any{
		"type": []string{"h-entry"},
		"properties": map[string]any{
			"url":       []string{"/continue-again-world"},
			"content":   []string{"Continue again world"},
			"published": []string{"2022-06-02"},
		},
	}

	post4 := map[string]any{
		"type": []string{"h-entry"},
		"properties": map[string]any{
			"url":       []string{"/finish-world"},
			"content":   []string{"Finish world"},
			"published": []string{"2023-01-02"},
		},
	}

	store.Insert(post1)
	store.Insert(post2)
	store.Insert(post3)
	store.Insert(post4)

	for n := 0; n < b.N; n++ {
		benchFound = store.FindAll(
			[]string{"type", "url", "content", "published"},
			no6.Predicates("type").Eq("h-entry"),
			no6.Predicates("published").Lt("2022-03-02"))
	}
}

func TestUseCase(t *testing.T) {
	newStore := func() (*Store, func() error) {
		file, _ := os.CreateTemp("", "")
		store, _ := Open(file.Name(), newSubject)

		return store, func() error {
			file.Close()
			return os.Remove(file.Name())
		}
	}

	post := map[string]any{
		"type": []string{"h-entry"},
		"properties": map[string]any{
			"url":       []string{"/hello-world"},
			"content":   []string{"Hello world"},
			"published": []string{"2021-01-02T12:00:00Z"},
			"deleted":   []string{"true"},
		},
	}

	post2 := map[string]any{
		"type": []string{"h-entry"},
		"properties": map[string]any{
			"url":       []string{"/goodbye-world"},
			"content":   []string{"Goodbye world"},
			"published": []string{"2022-01-02T12:00:00Z"},
		},
	}

	post3 := map[string]any{
		"type": []string{"h-entry"},
		"properties": map[string]any{
			"url":       []string{"/hello-world-again"},
			"content":   []string{"Hello world again"},
			"published": []string{"2023-01-02T10:00:00Z"},
		},
	}

	post4 := map[string]any{
		"type": []string{"h-entry"},
		"properties": map[string]any{
			"category":  []string{"findable"},
			"url":       []string{"/hello-world-2"},
			"content":   []string{"Hello world 2"},
			"published": []string{"2023-01-02T11:30:00Z"},
		},
	}

	post5 := map[string]any{
		"type": []string{"h-entry"},
		"properties": map[string]any{
			"category":  []string{"findable"},
			"url":       []string{"/hello-world-1"},
			"content":   []string{"Hello world 1"},
			"published": []string{"2023-01-02T12:00:00Z"},
		},
	}

	t.Run("delete an entry", func(t *testing.T) {
		store, closer := newStore()
		defer closer()

		uid, err := store.Insert(post)
		assert.Nil(t, err)

		_, ok := store.Find([]string{"url"}, no6.Predicates("type").Eq("h-entry"))
		assert.True(t, ok)

		err = store.DeleteByUID(uid)
		assert.Nil(t, err)

		_, ok = store.Find([]string{"url"}, no6.Predicates("type").Eq("h-entry"))
		assert.False(t, ok)
	})

	t.Run("replace an entry", func(t *testing.T) {
		store, closer := newStore()
		defer closer()

		uid, err := store.Insert(post)
		assert.Nil(t, err)

		_, ok := store.Find([]string{"url"}, no6.Predicates("content").Eq("Hello world"))
		assert.True(t, ok)

		err = store.Replace(uid, post2)
		assert.Nil(t, err)

		_, ok = store.Find([]string{"url"}, no6.Predicates("content").Eq("Hello world"))
		assert.False(t, ok)
		_, ok = store.Find([]string{"url"}, no6.Predicates("content").Eq("Goodbye world"))
		assert.True(t, ok)
	})

	t.Run("get by uid", func(t *testing.T) {
		store, closer := newStore()
		defer closer()

		uid, err := store.Insert(post)
		assert.Nil(t, err)

		entry, ok := store.Get(uid)
		assert.True(t, ok)
		assert.Equal(t, post, entry)
	})

	t.Run("get by url", func(t *testing.T) {
		store, closer := newStore()
		defer closer()

		_, err := store.Insert(post)
		assert.Nil(t, err)

		entry, ok := store.One(no6.Predicates("url").Eq("/hello-world"))
		assert.True(t, ok)
		assert.Equal(t, post, entry)
	})

	t.Run("list before", func(t *testing.T) {
		store, closer := newStore()
		defer closer()

		_, _ = store.Insert(post)
		_, _ = store.Insert(post2)
		_, _ = store.Insert(post3)

		entries := store.All(no6.Predicates("published").Lt("2022-02-02"))

		assert.Equal(t, []map[string]any{post, post2}, entries)
	})

	t.Run("list before with and without", func(t *testing.T) {
		store, closer := newStore()
		defer closer()

		_, _ = store.Insert(post)
		_, _ = store.Insert(post2)
		_, _ = store.Insert(post3)
		_, _ = store.Insert(post4)
		_, _ = store.Insert(post5)

		entries := store.All(
			no6.Predicates("published").Gt("2023-01-02T00:00:00Z"),
			no6.Predicates("published").Lt("2023-01-03T00:00:00Z"),
			no6.Predicates("category").Eq("findable"),
			no6.Without("deleted"),
			no6.Sort("published").Asc(),
		)

		assert.Equal(t, []map[string]any{post4, post5}, entries)

		entriesDesc := store.All(
			no6.Predicates("published").Gt("2023-01-02T00:00:00Z"),
			no6.Predicates("published").Lt("2023-01-03T00:00:00Z"),
			no6.Predicates("category").Eq("findable"),
			no6.Without("deleted"),
			no6.Sort("published").Desc(),
		)

		assert.Equal(t, []map[string]any{post5, post4}, entriesDesc)
	})
}
