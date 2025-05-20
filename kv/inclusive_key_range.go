package kv

import "bytes"

func ByteSliceIsLessThanOrEqualTo(key, other []byte) bool {
	if len(key) == 0 && len(other) == 0 {
		return true
	}
	if len(key) == 0 {
		return true
	}
	if len(other) == 0 {
		return false
	}
	return bytes.Compare(key, other) <= 0
}

type InclusiveKeyRange struct {
	start Key
	end   Key
}

func NewInclusiveKeyRange(start, end Key) InclusiveKeyRange {
	if ByteSliceIsLessThanOrEqualTo(start.EncodedBytes(), end.EncodedBytes()) {
		return InclusiveKeyRange{
			start: start,
			end:   end,
		}
	}
	panic("end key must be greater than or equal to start key in InclusiveKeyRange")
}

func (inclusiveKeyRange InclusiveKeyRange) Start() Key {
	return inclusiveKeyRange.start
}

func (inclusiveKeyRange InclusiveKeyRange) End() Key {
	return inclusiveKeyRange.end
}
