package sstable

import (
	"bytes"
	"encoding/binary"
	"go-lsm-db/kv"
	"go-lsm-db/sstable/block"
	"go-lsm-db/sstable/bloom"
	"sync/atomic"
)

type SSTableBuilder struct {
	blockBuilder       *block.Builder
	blockMetaList      *block.MetaBlockList
	bloomFilterBuilder *bloom.BloomFilterBuilder
	startingKey        kv.Key
	endingKey          kv.Key
	allBlocksData      []byte
	blockSize          uint32
}

func NewSSTableBuilder(blockSize uint32, expectedElements int, falsePositiveRate float64) *SSTableBuilder {
	return &SSTableBuilder{
		blockBuilder:       block.NewBlockBuilder(blockSize),
		blockMetaList:      block.NewMetaList(),
		bloomFilterBuilder: bloom.NewBloomFilterBuilder(expectedElements, falsePositiveRate),
		startingKey:        kv.NewKey(nil, 0),
		endingKey:          kv.NewKey(nil, 0),
		allBlocksData:      make([]byte, 0),
		blockSize:          blockSize,
	}
}

func (builder *SSTableBuilder) Add(key kv.Key, value kv.Value) {
	if builder.startingKey.IsRawKeyEmpty() {
		builder.startingKey = key
	}
	//block添加KV失败，说明当前block已满
	if !builder.blockBuilder.Add(key, value) {
		builder.finishBlock()
		builder.startNewBlockBuilder(key)
		builder.blockBuilder.Add(key, value)
	}
	
	builder.endingKey = key
	builder.bloomFilterBuilder.Add(key)

	
}

func (builder *SSTableBuilder) finishBlock() {
	blockData := builder.blockBuilder.Build().Encode()
	builder.blockMetaList.Add(block.MetaBlock{
		StartOffset: uint32(len(builder.allBlocksData)),
		StartKey:    builder.startingKey,
		EndKey:      builder.endingKey,
	})
	builder.allBlocksData = append(builder.allBlocksData, blockData...)
}

func (builder *SSTableBuilder) startNewBlockBuilder(key kv.Key) {
	builder.blockBuilder = block.NewBlockBuilder(builder.blockSize)
	builder.startingKey = key
	builder.endingKey = key
}

func (builder SSTableBuilder) allBlocksDataSize() int {
	return len(builder.allBlocksData)
}

/*
*

	----------------------------------------------------------------------------------------------------------------------------------------------------------

| data block | data block |...| data block | metadata section | 4 bytes for meta starting offset | bloom filter section | 4 bytes for bloom starting offset |
|										   |				  |									 |		                |                                   |			                                        |

	----------------------------------------------------------------------------------------------------------------------------------------------------------
*/
func (builder *SSTableBuilder) Build(id uint64, rootPath string) (*SSTable, error) {
	builder.finishBlock()
	buffer := new(bytes.Buffer)
	buffer.Write(builder.allBlocksData)
	buffer.Write(builder.blockMetaList.Encode())

	blockMetaStartingOffset := make([]byte, block.Uint32Size)
	binary.BigEndian.PutUint32(blockMetaStartingOffset, uint32(len(builder.allBlocksData)))
	buffer.Write(blockMetaStartingOffset)

	bloomFilter := builder.bloomFilterBuilder.Build()
	bloomFilterBuffer, _ := bloomFilter.Encode()
	buffer.Write(bloomFilterBuffer)

	bloomStartingOffset := make([]byte, block.Uint32Size)
	binary.BigEndian.PutUint32(bloomStartingOffset, uint32(buffer.Len()-len(bloomFilterBuffer)))
	buffer.Write(bloomStartingOffset)

	file := CreateNewFile(SSTableFilePath(id, rootPath), buffer.Bytes())

	startKey, _ := builder.blockMetaList.GetStartKeyOfFirstBlock()
	endKey, _ := builder.blockMetaList.GetEndKeyOfLastBlock()

	return &SSTable{
		id:                      id,
		file:                    file,
		blockMetaList:           builder.blockMetaList,
		bloomFilter:             bloomFilter,
		blockMetaStartingOffset: uint32(len(builder.allBlocksData)),
		blockSize:               uint(builder.blockSize),
		startingKey:             startKey,
		endingKey:               endKey,
		references:              atomic.Int64{},
	}, nil
}
