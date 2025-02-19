package no6

import "bytes"

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
	default:
		panic("Compare only understands some types")
	}
}
