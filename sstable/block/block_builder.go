package block

import (
	"encoding/binary"
	"fmt"
	"go-lsm-db/kv"
)

type Builder struct {
	offsets         []uint32
	firstKey        kv.Key
	blockSize       uint32
	data            []byte
	latestDataIndex uint32 //数据段的结束位置
}

func NewBlockBuilder(blockSize uint32) *Builder {
	return &Builder{
		blockSize:       blockSize,
		data:            make([]byte, blockSize),
		latestDataIndex: 0,
	}
}

func (builder *Builder) Add(key kv.Key, value kv.Value) bool {

	if uint32(builder.size()+key.EncodedSizeInBytes()+value.SizeInBytes()+ReservedKeySize+ReservedValueSize+KeyValueOffsetSize) > builder.blockSize {
		fmt.Println("builder.size()", builder.size())
		fmt.Println("key.EncodedSizeInBytes()", key.EncodedSizeInBytes())
		fmt.Println("value.SizeInBytes()", value.SizeInBytes())
		return false
	}

	if builder.firstKey.IsRawKeyEmpty() {
		builder.firstKey = key
	}
	kvBuffer := make([]byte, ReservedKeySize+ReservedValueSize+key.EncodedSizeInBytes()+value.SizeInBytes())
	//把key大小和数据放入kvBuffer
	binary.BigEndian.PutUint32(kvBuffer[0:], uint32(key.EncodedSizeInBytes()))
	copy(kvBuffer[ReservedKeySize:], key.EncodedBytes())
	//把value大小和数据放入kvBuffer
	binary.BigEndian.PutUint32(kvBuffer[ReservedKeySize+key.EncodedSizeInBytes():], uint32(value.SizeInBytes()))
	copy(kvBuffer[ReservedKeySize+ReservedKeySize+key.EncodedSizeInBytes():], value.Bytes())

	//添加偏移量
	builder.offsets = append(builder.offsets, builder.latestDataIndex)

	n := copy(builder.data[builder.latestDataIndex:], kvBuffer)
	builder.latestDataIndex += uint32(n)

	return true
}

// 没有存储kv对
func (builder *Builder) isEmpty() bool {
	return len(builder.offsets) == 0
}

// Build creates a new instance of Block.
func (builder *Builder) Build() Block {
	if builder.isEmpty() {
		panic("cannot build an empty Block")
	}
	return NewBlock(builder.data, builder.latestDataIndex, builder.offsets)
}

// byte可能分配了很多 但是实际使用的很少
// 偏移量的长度和数据段长度在block中被编码放到最后
// binary.BigEndian.PutUint32(data[len(data)-Uint32Size:], uint32(len(block.offsets)))
// binary.BigEndian.PutUint32(data[len(data)-Uint32Size*2:], block.lastDataIndex)
func (builder *Builder) size() int {
	return int(builder.latestDataIndex) +
		len(builder.offsets)*Uint32Size +
		Uint32Size + //偏移量长度和数据段长度
		Uint32Size
}
