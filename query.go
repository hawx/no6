package no6

import (
	"bytes"
	"log/slog"
	"sort"

	"go.etcd.io/bbolt"
)

type Constraint uint8

const (
	Eq Constraint = iota
	Ne
	Lt
	Gt
)

type Matcher interface {
	isMatcher()
}

type SubjectsMatcher struct {
	subjects []string
}

// Subjects returns a matcher that matches triples with the given subject.
func Subjects(subjects ...string) SubjectsMatcher {
	return SubjectsMatcher{subjects: subjects}
}

func (q SubjectsMatcher) isMatcher() {}

type PredicatesMatcher struct {
	predicates []string
	//
	constraint Constraint
	object     any
}

// Predicates returns a matcher that matches triples with the given predicate.
func Predicates(predicates ...string) PredicatesMatcher {
	return PredicatesMatcher{predicates: predicates}
}

func (q PredicatesMatcher) isMatcher() {}

// Eq returns a matcher that matches triples with the predicate and equal object
func (q PredicatesMatcher) Eq(object any) PredicatesMatcher {
	return PredicatesMatcher{predicates: q.predicates, constraint: Eq, object: object}
}

func (q PredicatesMatcher) Ne(object any) PredicatesMatcher {
	return PredicatesMatcher{predicates: q.predicates, constraint: Ne, object: object}
}

func (q PredicatesMatcher) Lt(object any) PredicatesMatcher {
	return PredicatesMatcher{predicates: q.predicates, constraint: Lt, object: object}
}

func (q PredicatesMatcher) Gt(object any) PredicatesMatcher {
	return PredicatesMatcher{predicates: q.predicates, constraint: Gt, object: object}
}

type Query struct {
	predicate  string
	constraint Constraint
	object     string
}

// PredicateObject returns a query for (?, predicate, object) triples.
func PredicateObject(predicate string, constraint Constraint, object string) Query {
	if predicate == Anything {
		panic("predicate must be given")
	}
	if object == Anything {
		panic("object must be given")
	}

	return Query{predicate: predicate, constraint: constraint, object: object}
}

// QuerySubject finds subjects that match all of the given queries. That is, it
// finds all ? that satisfy the intersection of (?, p1, o1)...(?, pN, oN).
func (s *Store) QuerySubject(queries ...Query) []string {
	var val []string

	s.db.View(func(tx *bbolt.Tx) error {
		dataBucket := tx.Bucket(bucketData)
		if dataBucket == nil {
			return nil
		}

		var subjects []uint64

		for qi, query := range queries {
			predicateBucket := tx.Bucket([]byte("predicate-" + query.predicate))
			if predicateBucket == nil {
				return nil
			}

			var thisQuerySubjects []uint64

			predicateBucket.ForEach(func(k, v []byte) error {
				for i := 0; i < len(v); i += 8 {
					obj := v[i : i+8]

					switch query.constraint {
					case Eq:
						objectUID := dataBucket.Get(s.typer.Format(query.object))
						if !bytes.Equal(objectUID, obj) {
							continue
						}
					case Ne:
						objectUID := dataBucket.Get(s.typer.Format(query.object))
						if bytes.Equal(objectUID, obj) {
							continue
						}
					case Lt:
						item := dataBucket.Get(obj)
						if s.typer.Compare(item, s.typer.Format(query.object)) > -1 {
							continue
						}
					case Gt:
						item := dataBucket.Get(obj)
						if s.typer.Compare(item, s.typer.Format(query.object)) < 1 {
							continue
						}
					}

					thisQuerySubjects = append(thisQuerySubjects, keySubject(k))
				}

				return nil
			})

			if qi == 0 {
				subjects = thisQuerySubjects
			} else {
				subjects = intersect(subjects, thisQuerySubjects)
			}
		}

		for _, subj := range subjects {
			item := dataBucket.Get(writeUID(subj))
			val = append(val, string(item))
		}

		return nil
	})

	return val
}

type namedList struct {
	subject   string
	predicate string
	list      []byte
}

type namedBucket struct {
	predicate string
	bucket    *bbolt.Bucket
}

type constraintObject struct {
	constraint Constraint
	object     any
}

// Query returns the results matching the given matchers.
func (s *Store) Query(matchers ...Matcher) []Triple {
	var val []Triple

	s.db.View(func(tx *bbolt.Tx) error {
		dataBucket := tx.Bucket(bucketData)
		if dataBucket == nil {
			return nil
		}

		// step 1. figure out which buckets/predicates are needed.
		// step 2. figure out which posting lists/subject-predicates are needed.
		// step 3. figure out what objects to match each predicate to

		var predicates []string
		var subjects []string
		constraints := map[string]constraintObject{}
		for _, matcher := range matchers {
			switch v := matcher.(type) {
			case PredicatesMatcher:
				predicates = append(predicates, v.predicates...)
				if v.object != nil {
					for _, predicate := range v.predicates {
						constraints[predicate] = constraintObject{
							constraint: v.constraint,
							object:     v.object,
						}
					}
				}
			case SubjectsMatcher:
				subjects = append(subjects, v.subjects...)
			}
		}

		var predicateBuckets []namedBucket
		for _, p := range predicates {
			b := tx.Bucket([]byte("predicate-" + p))
			if b == nil {
				continue
			}
			predicateBuckets = append(predicateBuckets, namedBucket{predicate: p, bucket: b})
		}

		var postingLists []namedList
		if len(subjects) > 0 {
			for _, subject := range subjects {
				subjectUID := dataBucket.Get([]byte(subject))
				if subjectUID == nil {
					return nil
				}

				for _, nb := range predicateBuckets {
					key := makeKey(readUID(subjectUID), nb.predicate)

					postingList := nb.bucket.Get(key)
					if postingList == nil {
						continue
					}

					postingLists = append(postingLists, namedList{subject: subject, predicate: nb.predicate, list: postingList})
				}
			}
		} else {
			for _, nb := range predicateBuckets {
				nb.bucket.ForEach(func(k, v []byte) error {
					subjectVal := dataBucket.Get([]byte(k[:8]))

					postingLists = append(postingLists, namedList{subject: string(subjectVal), predicate: nb.predicate, list: v})
					return nil
				})
			}
		}

		s.logger.Debug("checking posting lists", slog.Int("count", len(postingLists)))
		if len(postingLists) == 0 {
			return nil
		}

		for _, postingList := range postingLists {
			for i := 0; i < len(postingList.list); i += 8 {
				obj := postingList.list[i : i+8]

				var data []byte
				if constraint, ok := constraints[postingList.predicate]; ok {
					switch constraint.constraint {
					case Eq:
						objectUID := dataBucket.Get(s.typer.Format(constraint.object))
						if !bytes.Equal(objectUID, obj) {
							continue
						}
					case Ne:
						objectUID := dataBucket.Get(s.typer.Format(constraint.object))
						if bytes.Equal(objectUID, obj) {
							continue
						}
					case Lt:
						data = dataBucket.Get(obj)
						if s.typer.Compare(data, s.typer.Format(constraint.object)) > -1 {
							continue
						}
					case Gt:
						data = dataBucket.Get(obj)
						if s.typer.Compare(data, s.typer.Format(constraint.object)) < 1 {
							continue
						}
					}
				}

				if data == nil {
					data = dataBucket.Get(obj)
				}
				_, item := s.typer.Read(data)

				val = append(val, Triple{Subject: postingList.subject, Predicate: postingList.predicate, Object: item})
			}
		}

		return nil
	})

	return val
}

func intersect(a, b []uint64) []uint64 {
	if a == nil || b == nil {
		return nil
	}

	result := []uint64{}

	for _, v := range a {
		idx := sort.Search(len(b), func(i int) bool {
			return b[i] >= v
		})
		if idx < len(b) && b[idx] == v {
			result = append(result, v)
		}
	}

	return result
}
