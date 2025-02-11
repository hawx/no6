// The idea here is storing things as (subject+predicate, objects) pairs instead of
// (subject, predicate, object). Might also be able to partition those pairs by
// predicate into buckets. We'll see.
package posting

import (
	"bytes"
	"encoding/binary"
	"log"

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
	bucketData = []byte("data")

	// And to finish off the structure: the predicate-* bucket contains
	// postinglists for that predicate.
)

type Triple struct{ Subject, Predicate, Object string }

type Store struct {
	db *bbolt.DB
}

func Open(path string) (*Store, error) {
	db, err := bbolt.Open(path, 0600, nil)
	if err != nil {
		return nil, err
	}

	return &Store{db: db}, nil
}

func makeKey(subject uint64, predicate string) []byte {
	data := make([]byte, 8+len(predicate))
	binary.LittleEndian.PutUint64(data[:8], subject)
	copy(data[8:], []byte(predicate))
	return data
}

func makeValue(objects []uint64) []byte {
	data := make([]byte, len(objects)*8)
	for i, object := range objects {
		binary.LittleEndian.PutUint64(data[i*8:(i+1)*8], object)
	}
	return data
}

func appendValue(list []byte, value uint64) []byte {
	data := make([]byte, 8)
	binary.LittleEndian.PutUint64(data, value)

	// should be sorted though
	return append(list, data...)
}

func incKey(last []byte) ([]byte, []byte) {
	n := binary.LittleEndian.Uint64(last)
	data := make([]byte, 8)
	binary.LittleEndian.PutUint64(data, n+1)
	return data, data
}

func (s *Store) Insert(triples ...Triple) {
	for _, triple := range triples {
		s.insertTriple(triple.Subject, triple.Predicate, triple.Object)
	}
}

func (s *Store) insertTriple(subject, predicate, object string) {
	s.db.Update(func(tx *bbolt.Tx) error {
		idBucket, _ := tx.CreateBucketIfNotExists(bucketID)
		lastID := idBucket.Get(keyLast)
		if lastID == nil {
			lastID = make([]byte, 8)
			binary.LittleEndian.PutUint64(lastID, 0)
		}

		dataBucket, _ := tx.CreateBucketIfNotExists(bucketData)
		predicateBucket, _ := tx.CreateBucketIfNotExists([]byte("predicate-" + predicate))

		subjectUID := dataBucket.Get([]byte(subject))
		if subjectUID == nil {
			subjectUID, lastID = incKey(lastID)
			dataBucket.Put(subjectUID, []byte(subject))
			dataBucket.Put([]byte(subject), subjectUID)
		}

		objectUID := dataBucket.Get([]byte(object))
		if objectUID == nil {
			objectUID, lastID = incKey(lastID)
			dataBucket.Put(objectUID, []byte(object))
			dataBucket.Put([]byte(object), objectUID)
		}

		key := makeKey(binary.LittleEndian.Uint64(subjectUID), predicate)

		postingList := predicateBucket.Get(key)
		if postingList == nil {
			predicateBucket.Put(key, appendValue([]byte{}, binary.LittleEndian.Uint64(objectUID)))
		} else {
			predicateBucket.Put(key, appendValue(postingList, binary.LittleEndian.Uint64(objectUID)))
		}

		return idBucket.Put(keyLast, lastID)
	})
}

type Query struct {
	predicate string
	object    string
}

// PredicateObject returns a query for (?, predicate, object) triples.
func PredicateObject(predicate, object string) Query {
	if predicate == Anything {
		panic("predicate must be given")
	}
	if object == Anything {
		panic("object must be given")
	}

	return Query{predicate: predicate, object: object}
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

		// need to intersect each subjects

		for _, query := range queries {
			var subjects [][]byte

			predicateBucket := tx.Bucket([]byte("predicate-" + query.predicate))
			if predicateBucket == nil {
				return nil
			}

			objectUID := dataBucket.Get([]byte(query.object))
			if objectUID == nil {
				return nil
			}

			predicateBucket.ForEach(func(k, v []byte) error {
				for i := 0; i < len(v); i += 8 {
					obj := v[i : i+8]
					if objectUID != nil && !bytes.Equal(objectUID, obj) {
						continue
					}

					subjects = append(subjects, k[:8])
				}

				return nil
			})

			log.Println(query, subjects)
		}

		return nil
	})

	return val
}

// Query allows simple (?/X, Y, ?/Z) queries, returning any matching triples.
func (s *Store) Query(subject, predicate, object string) []Triple {
	var val []Triple

	if predicate == Anything {
		// TODO: should this query pattern be supported?
		return nil
	}

	s.db.View(func(tx *bbolt.Tx) error {
		dataBucket := tx.Bucket(bucketData)
		if dataBucket == nil {
			return nil
		}

		predicateBucket := tx.Bucket([]byte("predicate-" + predicate))
		if predicateBucket == nil {
			return nil
		}

		postingLists := map[string][]byte{}

		if subject != Anything {
			subjectUID := dataBucket.Get([]byte(subject))
			if subjectUID == nil {
				return nil
			}

			key := makeKey(binary.LittleEndian.Uint64(subjectUID), predicate)

			postingList := predicateBucket.Get(key)
			if postingList == nil {
				return nil
			}

			postingLists = map[string][]byte{subject: postingList}
		} else {
			predicateBucket.ForEach(func(k, v []byte) error {
				subjectVal := dataBucket.Get([]byte(k[:8]))

				postingLists[string(subjectVal)] = v
				return nil
			})
		}

		var objectUID []byte
		if object != Anything {
			objectUID = dataBucket.Get([]byte(object))
			if objectUID == nil {
				return nil
			}
		}

		for subj, postingList := range postingLists {
			for i := 0; i < len(postingList); i += 8 {
				obj := postingList[i : i+8]
				if objectUID != nil && !bytes.Equal(objectUID, obj) {
					continue
				}

				item := dataBucket.Get(obj)
				val = append(val, Triple{Subject: subj, Predicate: predicate, Object: string(item)})
			}
		}

		return nil
	})

	return val
}
