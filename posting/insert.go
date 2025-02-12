package posting

import (
	"encoding/binary"
	"log/slog"

	"go.etcd.io/bbolt"
)

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
		predicatesBucket, _ := tx.CreateBucketIfNotExists(bucketPredicates)
		predicateBucket, _ := tx.CreateBucketIfNotExists([]byte("predicate-" + predicate))

		predicatesBucket.Put([]byte(predicate), []byte{})
		s.logger.Debug("PUT",
			slog.String("bucket", string(bucketPredicates)),
			slog.String("key", predicate))

		subjectUID := dataBucket.Get([]byte(subject))
		if subjectUID == nil {
			subjectUID, lastID = incKey(lastID)
			dataBucket.Put(subjectUID, []byte(subject))
			dataBucket.Put([]byte(subject), subjectUID)

			s.logger.Debug("PUT",
				slog.String("bucket", string(bucketData)),
				slog.Uint64("uid", readUID(subjectUID)),
				slog.String("subject", subject))
		}

		objectUID := dataBucket.Get([]byte(object))
		if objectUID == nil {
			objectUID, lastID = incKey(lastID)
			dataBucket.Put(objectUID, []byte(object))
			dataBucket.Put([]byte(object), objectUID)

			s.logger.Debug("PUT",
				slog.String("bucket", string(bucketData)),
				slog.Uint64("uid", readUID(objectUID)),
				slog.String("object", object))
		}

		key := makeKey(readUID(subjectUID), predicate)

		postingList := predicateBucket.Get(key)
		if postingList == nil {
			predicateBucket.Put(key, appendValue([]byte{}, readUID(objectUID)))

			s.logger.Debug("PUT",
				slog.String("bucket", "predicate-"+predicate),
				slog.String("key", prettyPrintKey(key)),
				slog.String("value", prettyPrintList(appendValue([]byte{}, readUID(objectUID)))))
		} else {
			predicateBucket.Put(key, appendValue(postingList, readUID(objectUID)))

			s.logger.Debug("PUT",
				slog.String("bucket", "predicate-"+predicate),
				slog.String("key", prettyPrintKey(key)),
				slog.String("value", prettyPrintList(appendValue(postingList, readUID(objectUID)))))
		}

		s.logger.Debug("PUT",
			slog.String("bucket", string(bucketID)),
			slog.String("key", string(keyLast)),
			slog.Uint64("lastID", readUID(lastID)))

		return idBucket.Put(keyLast, lastID)
	})
}
