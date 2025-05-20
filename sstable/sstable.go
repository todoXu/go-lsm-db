package sstable

import (
	"encoding/binary"
	"fmt"
	"go-lsm-db/kv"
	"go-lsm-db/sstable/block"
	"go-lsm-db/sstable/bloom"
	"os"
	"path/filepath"
	"sync/atomic"
)

type SSTable struct {
	id                      uint64
	blockMetaList           *block.MetaBlockList
	bloomFilter             *bloom.BloomFilter
	file                    *SSTableFile
	blockMetaStartingOffset uint32 // 元数据区域的起始偏移量
	blockSize               uint
	startingKey             kv.Key
	endingKey               kv.Key
	references              atomic.Int64 //引用计数
}

func SSTableFilePath(id uint64, rootPath string) string {
	return filepath.Join(rootPath, fmt.Sprintf("%v.sst", id))
}

func NewSTable(id uint64, blockMetaList *block.MetaBlockList, bloomFilter *bloom.BloomFilter, file *SSTableFile, blockMetaStartingOffset uint32, blockSize uint, startingKey kv.Key, endingKey kv.Key) *SSTable {

	return &SSTable{
		id:                      id,
		blockMetaList:           blockMetaList,
		bloomFilter:             bloomFilter,
		file:                    file,
		blockMetaStartingOffset: blockMetaStartingOffset,
		blockSize:               blockSize,
		startingKey:             startingKey,
		endingKey:               endingKey,
		references:              atomic.Int64{},
	}
}

/*
	----------------------------------------------------------------------------------------------------------------------------------------------------------

| data block | metadata section | 4 bytes for meta starting offset | bloom filter section | 4 bytes for bloom starting offset |

	---------------------------------------------------------------------------------------------------------------------------------------------------------
*/
func Load(id uint64, rootPath string, blockSize uint) (*SSTable, error) {

	file, err := Open(SSTableFilePath(id, rootPath))
	if err != nil {
		return nil, err
	}

	fileSize := file.Size()

	bloomStartingOffsetBuffer, _ := file.Read(int64(fileSize-block.Uint32Size), block.Uint32Size)
	bloomStartingOffset := binary.BigEndian.Uint32(bloomStartingOffsetBuffer)

	bloomFilterSize := uint32(fileSize) - block.Uint32Size - bloomStartingOffset
	bloomFilterBuffer, _ := file.Read(int64(bloomStartingOffset), bloomFilterSize)
	bloomFilter, _ := bloom.DecodeToBloomFilter(bloomFilterBuffer)

	metaBlockListStartOffsetBuffer, _ := file.Read(int64(bloomStartingOffset-block.Uint32Size), block.Uint32Size)
	metaBlockListStartOffset := binary.BigEndian.Uint32(metaBlockListStartOffsetBuffer)
	metaBlockListBufferSize := bloomStartingOffset - block.Uint32Size - metaBlockListStartOffset
	metaBlockListBuffer, _ := file.Read(int64(metaBlockListStartOffset), uint32(metaBlockListBufferSize))
	metaBlockList := block.DecodeToBlockMetaList(metaBlockListBuffer)

	startingKey, _ := metaBlockList.GetStartKeyOfFirstBlock()
	endingKey, _ := metaBlockList.GetEndKeyOfLastBlock()

	return &SSTable{
		id:                      id,
		blockMetaList:           metaBlockList,
		bloomFilter:             &bloomFilter,
		blockMetaStartingOffset: metaBlockListStartOffset,
		file:                    file,
		blockSize:               blockSize,
		startingKey:             startingKey,
		endingKey:               endingKey,
	}, nil

}

func (table *SSTable) SeekToFirst() (*SSTableIterator, error) {
	readBlock, _ := table.readBlock(0)
	table.incrementReference()
	return &SSTableIterator{
		table:         table,
		blockIndex:    0,
		blockIterator: readBlock.SeekToFirst(),
	}, nil
}

/*
	func (table *SSTable) SeekToKey(key kv.Key) (*SSTableIterator, error) {
		//哪个block包含key
		_, index := table.blockMetaList.MaybeBlockMetaContaining(key)

		readBlock, _ := table.readBlock(index)
		blockIt := readBlock.SeekToKey(key)

		//当前block不包含key 用下一个block
		if !blockIt.IsValid() {
			index++
			if index < table.BlocksSize() {
				readBlock, _ = table.readBlock(index)
				blockIt = readBlock.SeekToFirst()
			}
		}
		//当前有迭代器正在使用 防止table被删除
		table.incrementReference()

		return &SSTableIterator{
			table:         table,
			blockIndex:    index,
			blockIterator: blockIt,
		}, nil
	}
*/
func (table *SSTable) readBlock(blockIndex int) (block.Block, error) {
	startOffset, endOffset := table.getBlockOffsetRange(blockIndex)
	buffer, err := table.file.Read(int64(startOffset), endOffset-startOffset)
	if err != nil {
		return block.Block{}, err
	}
	metaBlock := block.DecodeToBlock(buffer)
	return metaBlock, nil
}

func (table *SSTable) SeekToKey(key kv.Key) (*SSTableIterator, error) {
	for blockIndex := 0; blockIndex < table.BlocksSize(); blockIndex++ {
		block, err := table.readBlock(blockIndex)
		if err != nil {
			continue
		}

		blockIt := block.SeekToKey(key)
		for blockIt.IsValid() {
			if kv.CompareKeys(blockIt.Key(), key) == 0 {
				table.incrementReference()
				return &SSTableIterator{
					table:         table,
					blockIndex:    blockIndex,
					blockIterator: blockIt,
				}, nil
			}
			blockIt.Next()
		}

	}

	// 如果没有找到，返回一个新的迭代器
	block, _ := table.readBlock(0)
	it := block.SeekToFirst()
	return &SSTableIterator{
		table:         table,
		blockIndex:    0,
		blockIterator: it,
	}, nil
}

// 获取指定block的起始偏移量和结束偏移量
// +------------------+------------------+------------------+------------------+------------------+
// | 数据块区域       | 元数据区域       | 元数据偏移量     | 布隆过滤器       | 布隆过滤器偏移量 |
// +------------------+------------------+------------------+------------------+------------------+
// | 从0开始          | blockMetaOffset  | bloomOffset-4    | bloomOffset      | fileSize-4       |
// +------------------+------------------+------------------+------------------+------------------+
func (table *SSTable) getBlockOffsetRange(blockIndex int) (uint32, uint32) {
	metaBlock, ok := table.blockMetaList.GetAt(blockIndex)
	if !ok {
		panic("block index out of range")
	}
	startOffset := metaBlock.StartOffset

	nextMetaBlock, ok := table.blockMetaList.GetAt(blockIndex + 1)
	endOffset := uint32(0)
	if ok {
		endOffset = nextMetaBlock.StartOffset
	} else {
		endOffset = table.blockMetaStartingOffset
	}
	return startOffset, endOffset
}

func (table *SSTable) BlocksSize() int {
	return table.blockMetaList.Size()
}

func (table *SSTable) Remove() error {
	if err := table.file.fp.Close(); err != nil {
		return err
	}
	if err := os.Remove(table.file.fp.Name()); err != nil {
		return err
	}
	return nil
}

func (table *SSTable) TotalReferences() int64 {
	return table.references.Load()
}

func (table *SSTable) Id() uint64 {
	return table.id
}

func DecrementReferenceFor(tables []*SSTable) {
	for _, table := range tables {
		table.references.Add(-1)
	}
}

func (table *SSTable) incrementReference() {
	table.references.Add(1)
}

func (table *SSTable) MayContain(key kv.Key) bool {
	return table.bloomFilter.MayContain(key)
}

// 给定的key范围和sstable的key范围是否有交集
func (table *SSTable) ContainsInclusive(inclusiveKeyRange kv.InclusiveKeyRange) bool {
	if inclusiveKeyRange.Start().IsRawKeyGreaterThan(table.endingKey) {
		return false
	}
	if inclusiveKeyRange.End().IsRawKeyLesserThan(table.startingKey) {
		return false
	}
	return true
}

func (table *SSTable) StartingKey() kv.Key {
	return table.startingKey
}

func (table *SSTable) EndingKey() kv.Key {
	return table.endingKey
}
