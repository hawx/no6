package no6

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
		panic("Format only understands some types, better error needed...")
	}
}

// Read parses the value in data as typ.
func (t *Typer) Read(data []byte) (Type, any) {
	typ := Type(data[0])
	switch typ {
	case TypeString:
		return typ, string(data[1:])
	default:
		panic("Read only understands some types, better error needed...")
	}
}
