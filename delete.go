package no6

import "go.etcd.io/bbolt"

// TODO: Delete by subject/predicate/whole triple?
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
