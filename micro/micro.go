// Package micro provides a micropub-style interface to no6 databases.
package micro

import (
	"errors"

	"github.com/google/uuid"
	"hawx.me/code/no6"
)

type Store struct {
	inner *no6.Store
}

func Open(path string) (*Store, error) {
	store, err := no6.Open(path)

	return &Store{inner: store}, err
}

func (s *Store) Insert(data map[string]any) (string, error) {
	uid := uuid.NewString()

	typ, ok := data["type"].([]string)
	if !ok || len(typ) != 1 {
		return "", errors.New("data must include a single string 'type' (I think)")
	}

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

func (s *Store) Find(qs ...no6.Query) (map[string]any, bool) {
	subjects := s.inner.QuerySubject(qs...)
	if len(subjects) == 0 {
		return nil, false
	}

	return s.tryResolve(subjects[0])
}

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
	if _, err := uuid.Parse(id); err != nil {
		return nil, false
	}

	triples := s.inner.Query(id, no6.Anything, no6.Eq, no6.Anything)

	var (
		typ   []string
		props = map[string]any{}
	)

	for _, triple := range triples {
		if triple.Predicate == "type" {
			typ = append(typ, triple.Object)
		} else {
			if found, ok := props[triple.Predicate]; ok {
				switch v := found.(type) {
				case []string:
					props[triple.Predicate] = append(v, triple.Object)
				case []map[string]any:
					if resolved, ok := s.tryResolve(triple.Object); ok {
						props[triple.Predicate] = append(v, resolved)
					}
				}
			} else {
				if resolved, ok := s.tryResolve(triple.Object); ok {
					props[triple.Predicate] = []map[string]any{resolved}
				} else {
					props[triple.Predicate] = []string{triple.Object}
				}
			}
		}
	}

	return map[string]any{
		"type":       typ,
		"properties": props,
	}, true
}
