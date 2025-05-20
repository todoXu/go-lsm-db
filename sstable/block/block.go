package block

import (
	"encoding/binary"
	"go-lsm-db/kv"
)

type Block struct {
	data          []byte
	offsets       []uint32
	lastDataIndex uint32
}

const (
	Uint8Size          = 1
	Uint16Size         = 2
	Uint32Size         = 4
	kb                 = 1024
	DefaultBlockSize   = 4 * kb
	ReservedKeySize    = 4
	ReservedValueSize  = 4
	KeyValueOffsetSize = 4
)

func NewBlock(data []byte, lastDataIndex uint32, offsets []uint32) Block {
	return Block{
		data:          data,
		offsets:       offsets,
		lastDataIndex: lastDataIndex,
	}
}

func (block Block) Encode() []byte {
	offsetBuffer := make([]byte, Uint32Size*len(block.offsets))
	offsetIndex := 0
	for _, offset := range block.offsets {
		binary.BigEndian.PutUint32(offsetBuffer[offsetIndex:], offset)
		offsetIndex += Uint32Size
	}
	data := make([]byte, len(block.data))
	copy(data, block.data)
	copy(data[block.lastDataIndex:], offsetBuffer)
	binary.BigEndian.PutUint32(data[len(data)-Uint32Size:], uint32(len(block.offsets)))
	binary.BigEndian.PutUint32(data[len(data)-Uint32Size*2:], block.lastDataIndex)

	return data
}

func DecodeToBlock(data []byte) Block {
	numberOfOffsets := binary.BigEndian.Uint32(data[len(data)-Uint32Size:])
	lastDataIndex := binary.BigEndian.Uint32(data[len(data)-Uint32Size*2:])
	offsetsBuffer := data[lastDataIndex : lastDataIndex+numberOfOffsets*Uint32Size]
	dataBuffer := data[:lastDataIndex]

	offsets := make([]uint32, 0, numberOfOffsets)
	for index := 0; index < len(offsetsBuffer); index += Uint32Size {
		offsets = append(offsets, binary.BigEndian.Uint32(offsetsBuffer[index:]))

	}

	newData := make([]byte, len(dataBuffer))
	copy(newData, dataBuffer)
	return Block{data: newData, offsets: offsets, lastDataIndex: lastDataIndex}
}

func (block Block) SeekToFirst() *BlockIterator {
	it := &BlockIterator{
		block:       block,
		offsetIndex: 0,
	}
	it.seekToOffsetIndex(0)
	return it
}

func (block Block) SeekToKey(key kv.Key) *BlockIterator {
	it := &BlockIterator{
		block: block,
	}
	it.seekToGreaterOrEqual(key)
	return it
}

func (block Block) encodeKeyValueBeginOffsets() []byte {
	keyValueBeginOffsets := make([]byte, 0, len(block.offsets)*Uint32Size)
	offsetIndex := 0
	for _, offset := range block.offsets {
		binary.BigEndian.PutUint32(keyValueBeginOffsets[offsetIndex:], offset)
		offsetIndex += Uint32Size
	}
	return keyValueBeginOffsets
}
