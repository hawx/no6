package no6

import (
	"encoding/binary"
	"log/slog"

	"go.etcd.io/bbolt"
)

func (s *Store) PutTriples(triples ...Triple) {
	for _, triple := range triples {
		_ = s.Put(triple.Subject, triple.Predicate, triple.Object)
	}
}

func (s *Store) Put(subject, predicate string, object any) error {
	// TODO: should probably make sure the ID is updated first, otherwise the next
	// operation might do something weird.
	return s.db.Update(func(tx *bbolt.Tx) error {
		idBucket, err := tx.CreateBucketIfNotExists(bucketID)
		if err != nil {
			return err
		}

		lastID := idBucket.Get(keyLast)
		if lastID == nil {
			lastID = make([]byte, 8)
			binary.LittleEndian.PutUint64(lastID, 0)
		}

		dataBucket, err := tx.CreateBucketIfNotExists(bucketData)
		if err != nil {
			return err
		}
		predicatesBucket, err := tx.CreateBucketIfNotExists(bucketPredicates)
		if err != nil {
			return err
		}
		predicateBucket, err := tx.CreateBucketIfNotExists([]byte("predicate-" + predicate))
		if err != nil {
			return err
		}

		if err := predicatesBucket.Put([]byte(predicate), []byte{}); err != nil {
			return err
		}
		s.logger.Debug("PUT",
			slog.String("bucket", string(bucketPredicates)),
			slog.String("key", predicate))

		subjectUID := dataBucket.Get([]byte(subject))
		if subjectUID == nil {
			subjectUID, lastID = incKey(lastID)
			if err := dataBucket.Put(subjectUID, []byte(subject)); err != nil {
				return err
			}
			if err := dataBucket.Put([]byte(subject), subjectUID); err != nil {
				return err
			}

			s.logger.Debug("PUT",
				slog.String("bucket", string(bucketData)),
				slog.Uint64("uid", readUID(subjectUID)),
				slog.String("subject", subject))
		}

		objectUID := dataBucket.Get(s.typer.Format(object))
		if objectUID == nil {
			objectUID, lastID = incKey(lastID)
			objectData := s.typer.Format(object)

			if err := dataBucket.Put(objectUID, objectData); err != nil {
				return err
			}
			if err := dataBucket.Put(objectData, objectUID); err != nil {
				return err
			}

			s.logger.Debug("PUT",
				slog.String("bucket", string(bucketData)),
				slog.Uint64("uid", readUID(objectUID)),
				slog.Any("object", object))
		}

		key := makeKey(readUID(subjectUID), predicate)

		postingList := predicateBucket.Get(key)
		if postingList == nil {
			if err := predicateBucket.Put(key, appendValue([]byte{}, readUID(objectUID))); err != nil {
				return err
			}

			s.logger.Debug("PUT",
				slog.String("bucket", "predicate-"+predicate),
				slog.String("key", prettyPrintKey(key)),
				slog.String("value", prettyPrintList(appendValue([]byte{}, readUID(objectUID)))))
		} else {
			if err := predicateBucket.Put(key, appendValue(postingList, readUID(objectUID))); err != nil {
				return err
			}

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
