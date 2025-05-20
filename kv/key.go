package kv

import (
	"bytes"
	"encoding/binary"
	"strconv"
	"unsafe"
)

const TimeStampSize = int(unsafe.Sizeof(uint64(0)))

type Key struct {
	key []byte
	// key的时间戳
	timestamp uint64
}

func NewKey(key []byte, timestamp uint64) Key {
	return Key{
		key:       key,
		timestamp: timestamp,
	}
}

func (k Key) isLesserThan(other Key) bool {
	cmp := bytes.Compare(k.key, other.key)
	if cmp < 0 {
		return true
	} else if cmp > 0 {
		return false
	}
	// cmp == 0
	return k.timestamp < other.timestamp
}

func (k Key) isEqual(other Key) bool {
	return bytes.Equal(k.key, other.key) && k.timestamp == other.timestamp
}

// 升序排列
func (k Key) CompareKeysWithDescendingTimestamp(other Key) int {
	cmp := bytes.Compare(k.key, other.key)
	if cmp != 0 {
		return cmp
	}
	// 根据时间戳降序排列
	if k.timestamp < other.timestamp {
		return 1
	} else if k.timestamp > other.timestamp {
		return -1
	} else if k.timestamp == other.timestamp {
		return 0
	}

	return 0
}

func CompareKeys(userKey, systemKey Key) int {
	return userKey.CompareKeysWithDescendingTimestamp(systemKey)
}

func (k Key) IsRawKeyEqualTo(other Key) bool {
	return bytes.Equal(k.key, other.key)
}

func (k Key) IsRawKeyGreaterThan(other Key) bool {
	return bytes.Compare(k.key, other.key) > 0
}

func (k Key) IsRawKeyLesserThan(other Key) bool {
	return bytes.Compare(k.key, other.key) < 0
}

func (k Key) IsRawKeyEmpty() bool {
	return k.RawSizeInBytes() == 0
}

func (k Key) EncodedBytes() []byte {
	if k.IsRawKeyEmpty() {
		return nil
	}
	buffer := make([]byte, k.EncodedSizeInBytes())

	numberOfBytesWritten := copy(buffer, k.key)
	binary.BigEndian.PutUint64(buffer[numberOfBytesWritten:], k.timestamp)

	return buffer
}

func (k Key) RawBytes() []byte {
	return k.key
}

func (k Key) RawString() string {
	return string(k.RawBytes()) + "@" + strconv.FormatUint(k.timestamp, 10)
}

func (k Key) EncodedSizeInBytes() int {
	if k.IsRawKeyEmpty() {
		return 0
	}
	return len(k.key) + TimeStampSize
}

func (k Key) RawSizeInBytes() int {
	return len(k.RawBytes())
}

func (k Key) Timestamp() uint64 {
	return k.timestamp
}

func DecodeFromByte(buffer []byte) Key {
	if len(buffer) < TimeStampSize {
		panic("buffer length is less than TimeStampSize")
	}
	size := len(buffer)
	return Key{
		key:       buffer[:size-TimeStampSize],
		timestamp: binary.BigEndian.Uint64(buffer[size-TimeStampSize:]),
	}
}
