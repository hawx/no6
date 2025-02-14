package no6

import (
	"encoding/binary"
	"testing"

	"hawx.me/code/assert"
)

func TestReadWriteUID(t *testing.T) {
	subject := uint64(123)

	assert.Equal(t, subject, readUID(writeUID(subject)))
}

func TestMakeKey(t *testing.T) {
	subject := uint64(123)
	predicate := "hello"

	key := makeKey(subject, predicate)

	assert.Equal(t, subject, keySubject(key))
}

func TestMakeValue(t *testing.T) {
	value := makeValue([]uint64{1, 4, 2})

	expected := make([]byte, 8*3)
	binary.LittleEndian.PutUint64(expected[0:8], 1)
	binary.LittleEndian.PutUint64(expected[8:16], 2)
	binary.LittleEndian.PutUint64(expected[16:24], 4)

	assert.Equal(t, expected, value)
}

func TestAppendValue(t *testing.T) {
	value := makeValue([]uint64{1, 5, 2})
	value = appendValue(value, 3)

	expected := make([]byte, 8*4)
	binary.LittleEndian.PutUint64(expected[0:8], 1)
	binary.LittleEndian.PutUint64(expected[8:16], 2)
	binary.LittleEndian.PutUint64(expected[16:24], 3)
	binary.LittleEndian.PutUint64(expected[24:32], 5)

	assert.Equal(t, expected, value)

	value = appendValue(value, 4)

	expected2 := make([]byte, 8*5)
	binary.LittleEndian.PutUint64(expected2[0:8], 1)
	binary.LittleEndian.PutUint64(expected2[8:16], 2)
	binary.LittleEndian.PutUint64(expected2[16:24], 3)
	binary.LittleEndian.PutUint64(expected2[24:32], 4)
	binary.LittleEndian.PutUint64(expected2[32:40], 5)

	assert.Equal(t, expected2, value)
}

func TestCompareBytes(t *testing.T) {
	assert.Equal(t, 0, compareBytes([]byte{1, 2, 3, 4, 5, 6, 7, 8}, []byte{1, 2, 3, 4, 5, 6, 7, 8}))
	assert.Equal(t, -1, compareBytes([]byte{1, 2, 3, 4, 5, 6, 7, 7}, []byte{1, 2, 3, 4, 5, 6, 7, 8}))
	assert.Equal(t, -1, compareBytes([]byte{0, 2, 3, 4, 5, 6, 7, 8}, []byte{1, 2, 3, 4, 5, 6, 7, 8}))
	assert.Equal(t, 1, compareBytes([]byte{1, 2, 3, 4, 5, 6, 7, 8}, []byte{1, 2, 3, 4, 5, 6, 7, 7}))
	assert.Equal(t, 1, compareBytes([]byte{1, 2, 3, 4, 5, 6, 7, 8}, []byte{0, 2, 3, 4, 5, 6, 7, 8}))

	assert.Equal(t, 0, compareBytes(writeUID(5001), writeUID(5001)))
	assert.Equal(t, -1, compareBytes(writeUID(4999), writeUID(5001)))
	assert.Equal(t, 1, compareBytes(writeUID(5001), writeUID(4999)))
}
