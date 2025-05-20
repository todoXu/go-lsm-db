package block

import (
	"encoding/binary"
	"go-lsm-db/kv"
)

type BlockIterator struct {
	key         kv.Key
	val         kv.Value
	offsetIndex uint32
	block       Block
}

func NewBlockIterator(block Block) *BlockIterator {
	it := &BlockIterator{
		block:       block,
		offsetIndex: 0,
	}
	it.seekToOffsetIndex(0)

	return it
}

func (it *BlockIterator) Key() kv.Key {
	return it.key
}

func (it *BlockIterator) Value() kv.Value {
	return it.val
}

func (it *BlockIterator) IsValid() bool {
	return !it.key.IsRawKeyEmpty()
}

func (iterator *BlockIterator) markInvalid() {
	iterator.key = kv.NewKey(nil, 0)
	iterator.val = kv.NewValue(nil)
}

func (it *BlockIterator) seekToOffset(keyValueBeginOffset uint32) {
	data := it.block.data[keyValueBeginOffset:]
	keySize := binary.BigEndian.Uint32(data[0:])
	keyBuffer := data[ReservedKeySize : uint32(ReservedKeySize)+keySize]
	key := kv.DecodeFromByte(keyBuffer)

	valSize := binary.BigEndian.Uint32(data[ReservedKeySize+keySize:])
	valBuffer := data[ReservedKeySize+keySize+ReservedValueSize : ReservedKeySize+keySize+ReservedValueSize+valSize]
	val := kv.NewValue(valBuffer)

	it.key = key
	it.val = val
}

func (it *BlockIterator) seekToOffsetIndex(index uint32) {
	if index >= uint32(len(it.block.offsets)) {
		it.markInvalid()
		return
	}

	KeyValueOffset := it.block.offsets[index]
	it.seekToOffset(KeyValueOffset)
}

// 找到第一个大于等于key的元素
func (it *BlockIterator) seekToGreaterOrEqual(key kv.Key) {
	left, right := 0, len(it.block.offsets)-1
	for left <= right {
		mid := (left + right) / 2
		it.seekToOffsetIndex(uint32(mid))
		if !it.IsValid() {
			panic("invalid iterator")
		}

		cmp := it.key.CompareKeysWithDescendingTimestamp(key)
		if cmp == 0 {
			return
		} else if cmp == -1 {
			left = mid + 1
		} else if cmp == 1 {
			right = mid - 1
		}
		it.seekToOffsetIndex(uint32(left))
	}
}

func (it *BlockIterator) Next() {
	if it.offsetIndex >= uint32(len(it.block.offsets)) {
		it.markInvalid()
		return
	}
	it.offsetIndex++
	if it.offsetIndex < uint32(len(it.block.offsets)) {
		it.seekToOffsetIndex(it.offsetIndex)
	} else {
		it.markInvalid()
	}
}

// close不需要释放资源
func (it *BlockIterator) Close() {}
