package sstable

import (
	"go-lsm-db/kv"
	"go-lsm-db/sstable/block"
)

type SSTableIterator struct {
	table         *SSTable
	blockIndex    int
	blockIterator *block.BlockIterator
}

func (it *SSTableIterator) IsValid() bool {
	return it.blockIterator.IsValid()
}

func (it *SSTableIterator) Next() {
	it.blockIterator.Next()
	if !it.blockIterator.IsValid() {
		it.blockIndex++
		if it.blockIndex < it.table.BlocksSize() {
			readBlock, _ := it.table.readBlock(it.blockIndex)
			it.blockIterator = readBlock.SeekToFirst()
		}
	}
}

func (it *SSTableIterator) Key() kv.Key {
	return it.blockIterator.Key()
}

func (it *SSTableIterator) Value() kv.Value {
	return it.blockIterator.Value()
}

func (it *SSTableIterator) Close() {

}
