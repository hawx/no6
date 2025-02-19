package no6

import (
	"bytes"
	"encoding/binary"
)

type Type byte

const (
	TypeString Type = iota
	TypeBool
	TypeInt
	TypeUint
	TypeFloat
)

type Typer struct{}

// Format writes val to a byte slice as typ.
func (t *Typer) Format(val any) []byte {
	switch v := val.(type) {
	case string:
		return append([]byte{byte(TypeString)}, []byte(v)...)
	case int:
		sgn := byte(1)
		if v < 0 {
			sgn = byte(0)
			v = -v
		}

		data := make([]byte, 10)
		data[0] = byte(TypeInt)
		data[1] = sgn
		binary.BigEndian.PutUint64(data[2:], uint64(v))
		return data
	default:
		panic("Format only understands some types")
	}
}

// Read parses the value in data as typ.
func (t *Typer) Read(data []byte) (Type, any) {
	typ := Type(data[0])
	switch typ {
	case TypeString:
		return typ, string(data[1:])
	case TypeInt:
		num := int(binary.BigEndian.Uint64(data[2:]))
		if data[1] == 0 {
			num = -num
		}
		return typ, num
	default:
		panic("Read only understands some types")
	}
}

// Compare returns -1 if a < b, 0 if a == b, 1 if a > b.
func (t *Typer) Compare(a, b []byte) int {
	if a[0] != b[0] {
		panic("Compare only works on the same types")
	}

	typ := Type(a[0])
	switch typ {
	case TypeString:
		return bytes.Compare(a[1:], b[1:])
	case TypeInt:
		// simple case when signs differ
		if a[1] < b[1] {
			return -1
		} else if a[1] > b[1] {
			return 1
		}

		if a[1] == 0 {
			return bytes.Compare(b[2:], a[2:])
		}

		return bytes.Compare(a[2:], b[2:])
	default:
		panic("Compare only understands some types")
	}
}
