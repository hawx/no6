// Package micro provides a microformat-style interface to no6 databases.
package micro

import (
	"errors"
	"log"

	"hawx.me/code/no6"
)

type Store struct {
	inner      *no6.Store
	newSubject func(string) string
}

// Open returns a new store using the path given. Insert uses newSubject to
// determine the naming for new triples, which is given the type of the
// microformat object.
func Open(path string, newSubject func(string) string) (*Store, error) {
	store, err := no6.Open(path)

	return &Store{inner: store, newSubject: newSubject}, err
}

// Insert adds triples for each item in a typical microformat object, e.g.
//
//	{
//		"type": ["h-..."],
//		"properties": { ... }
//	}
//
// The subject  is returned, or an error if there was a problem.
func (s *Store) Insert(data map[string]any) (string, error) {
	typ, ok := data["type"].([]string)
	if !ok || len(typ) != 1 {
		return "", errors.New("data must include a single string 'type' (I think)")
	}

	uid := s.newSubject(typ[0])
	log.Println(uid)

	props, ok := data["properties"].(map[string]any)
	if !ok {
		return "", errors.New("data must include properties")
	}

	var triples []no6.Triple
	triples = append(triples, no6.Triple{Subject: uid, Predicate: "type", Object: typ[0]})

	for k, v := range props {
		switch vv := v.(type) {
		case []string:
			for _, vvv := range vv {
				triples = append(triples, no6.Triple{Subject: uid, Predicate: k, Object: vvv})
			}
		case []map[string]any:
			for _, vvv := range vv {
				vuid, err := s.Insert(vvv)
				if err != nil {
					return "", err
				}
				triples = append(triples, no6.Triple{Subject: uid, Predicate: k, Object: vuid})
			}
		default:
			return "", errors.New("invalid properties")
		}
	}

	s.inner.Insert(triples...)

	return uid, nil
}

// Find retrieves a single microformat object using the query. It will resolve any
// nested objects also in the database, but not any remote references.
func (s *Store) Find(qs ...no6.Query) (map[string]any, bool) {
	subjects := s.inner.QuerySubject(qs...)
	if len(subjects) == 0 {
		return nil, false
	}

	return s.tryResolve(subjects[0])
}

// FindAll retrieves all matching microformat objects. It resolves any nested
// objects also in the database, but not any remote references.
func (s *Store) FindAll(qs ...no6.Query) []map[string]any {
	subjects := s.inner.QuerySubject(qs...)
	if len(subjects) == 0 {
		return nil
	}

	var resolved []map[string]any
	for _, subject := range subjects {
		if v, ok := s.tryResolve(subject); ok {
			resolved = append(resolved, v)
		}
	}

	return resolved
}

func (s *Store) tryResolve(id string) (map[string]any, bool) {
	triples := s.inner.Query(id, no6.Anything, no6.Eq, no6.Anything)
	if len(triples) == 0 {
		return nil, false
	}

	var (
		typ   []string
		props = map[string]any{}
	)

	for _, triple := range triples {
		if triple.Predicate == "type" {
			typ = append(typ, triple.Object.(string))
		} else {
			if found, ok := props[triple.Predicate]; ok {
				switch v := found.(type) {
				case []string:
					props[triple.Predicate] = append(v, triple.Object.(string))
				case []map[string]any:
					if resolved, ok := s.tryResolve(triple.Object.(string)); ok {
						props[triple.Predicate] = append(v, resolved)
					}
				}
			} else {
				if resolved, ok := s.tryResolve(triple.Object.(string)); ok {
					props[triple.Predicate] = []map[string]any{resolved}
				} else {
					props[triple.Predicate] = []string{triple.Object.(string)}
				}
			}
		}
	}

	return map[string]any{
		"type":       typ,
		"properties": props,
	}, true
}
