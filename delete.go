package no6

import "go.etcd.io/bbolt"

func (s *Store) Delete(subject, predicate string) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		dataBucket := tx.Bucket(bucketData)
		if dataBucket == nil {
			return nil
		}

		subjectUID := dataBucket.Get([]byte(subject))
		if subjectUID == nil {
			return nil
		}

		predicateBucket := tx.Bucket([]byte("predicate-" + predicate))
		if predicateBucket == nil {
			return nil
		}

		key := makeKey(readUID(subjectUID), predicate)
		return predicateBucket.Delete(key)
	})
}

func (s *Store) DeleteSubject(subject string) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		dataBucket := tx.Bucket(bucketData)
		if dataBucket == nil {
			return nil
		}

		subjectUID := dataBucket.Get([]byte(subject))
		if subjectUID == nil {
			return nil
		}

		return tx.Bucket(bucketPredicates).ForEach(func(p []byte, _ []byte) error {
			if b := tx.Bucket([]byte("predicate-" + string(p))); b != nil {
				key := makeKey(readUID(subjectUID), string(p))
				return b.Delete(key)
			}

			return nil
		})
	})
}
