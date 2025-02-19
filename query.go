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

// Query allows simple (?/X, Y, ?/Z) queries, returning any matching triples.
func (s *Store) Query(subject, predicate string, constraint Constraint, object any) []Triple {
	s.logger.Debug("QUERY", slog.String("subject", subject), slog.String("predicate", predicate), slog.Any("constraint", constraint), slog.Any("object", object))
	var val []Triple

	s.db.View(func(tx *bbolt.Tx) error {
		dataBucket := tx.Bucket(bucketData)
		if dataBucket == nil {
			return nil
		}

		var predicateBuckets []namedBucket

		if predicate != Anything {
			predicateBucket := tx.Bucket([]byte("predicate-" + predicate))
			if predicateBucket == nil {
				return nil
			}
			predicateBuckets = append(predicateBuckets, namedBucket{predicate: predicate, bucket: predicateBucket})
		} else {
			predicatesBucket := tx.Bucket(bucketPredicates)
			if predicatesBucket == nil {
				return nil
			}
			predicatesBucket.ForEach(func(k, v []byte) error {
				predicateBuckets = append(predicateBuckets, namedBucket{predicate: string(k), bucket: tx.Bucket([]byte("predicate-" + string(k)))})
				return nil
			})
		}

		var postingLists []namedList

		if subject != Anything {
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

		var objectUID []byte
		if object != Anything {
			objectUID = dataBucket.Get(s.typer.Format(object))
			if objectUID == nil && (constraint == Eq || constraint == Ne) {
				return nil
			}
		}

		for _, postingList := range postingLists {
			for i := 0; i < len(postingList.list); i += 8 {
				obj := postingList.list[i : i+8]

				var data []byte
				if object != Anything {
					switch constraint {
					case Eq:
						if !bytes.Equal(objectUID, obj) {
							continue
						}
					case Ne:
						if bytes.Equal(objectUID, obj) {
							continue
						}
					case Lt:
						data = dataBucket.Get(obj)
						if s.typer.Compare(data, s.typer.Format(object)) > -1 {
							continue
						}
					case Gt:
						data = dataBucket.Get(obj)
						if s.typer.Compare(data, s.typer.Format(object)) < 1 {
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
