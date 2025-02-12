package micro

import (
	"os"
	"testing"

	"hawx.me/code/assert"
	"hawx.me/code/no6/posting"
)

func TestInsertFind(t *testing.T) {
	file, _ := os.CreateTemp("", "")
	file.Close()
	defer os.Remove(file.Name())

	store, _ := Open(file.Name())

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

	bodyfat, _ := store.Find(posting.PredicateObject("num", posting.Eq, "19.83"))
	assert.Equal(t, expectedBodyfat, bodyfat)

	entry, _ := store.Find(posting.PredicateObject("summary", posting.Eq, "Weighed 70.64 kg"))
	assert.Equal(t, expectedEntry, entry)
}

func TestInsertFindWhenPaged(t *testing.T) {
	file, _ := os.CreateTemp("", "")
	file.Close()
	defer os.Remove(file.Name())

	store, _ := Open(file.Name())

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

	entries := store.FindAll(
		posting.PredicateObject("type", posting.Eq, "h-entry"),
		posting.PredicateObject("published", posting.Gt, "2022-03-02"))

	assert.Equal(t, []map[string]any{post3, post4}, entries)
}
