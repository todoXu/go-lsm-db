package kv

type Value struct {
	val []byte
}

func NewValue(value []byte) Value {
	return Value{val: value}
}

func (value Value) IsEmpty() bool {
	return len(value.val) == 0
}

func (value Value) String() string {
	return string(value.val)
}
func (value Value) Value() []byte {
	return value.val
}

func (value Value) SizeInBytes() int {
	return len(value.val)
}

func (value Value) EncodeToByte(buffer []byte) uint32 {
	return uint32(copy(buffer, value.val))
}

func (value *Value) DecodeFrom(buffer []byte) {
	tmp := make([]byte, len(buffer))
	copy(tmp, buffer)
	value.val = tmp
}

func (value Value) Bytes() []byte {
	return value.val
}
