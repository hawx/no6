package posting

import (
	"encoding/binary"
	"fmt"
)

func toBytes(subject uint64) []byte {
	data := make([]byte, 8)
	binary.LittleEndian.PutUint64(data, subject)
	return data
}

func readUID(uid []byte) uint64 {
	return binary.LittleEndian.Uint64(uid)
}

func makeKey(subject uint64, predicate string) []byte {
	data := make([]byte, 8+len(predicate))
	binary.LittleEndian.PutUint64(data[:8], subject)
	copy(data[8:], []byte(predicate))
	return data
}

func keySubject(key []byte) uint64 {
	return binary.LittleEndian.Uint64(key[:8])
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

func prettyPrintKey(key []byte) string {
	return fmt.Sprintf("%v|%v", keySubject(key), string(key[8:]))
}

func prettyPrintList(list []byte) string {
	s := "["
	for i := 0; i < len(list); i += 8 {
		if i > 0 {
			s += ","
		}

		s += fmt.Sprint(binary.LittleEndian.Uint64(list[i : i+8]))
	}
	return s + "]"
}
