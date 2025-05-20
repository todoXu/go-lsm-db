package block

import (
	"bytes"
	"encoding/binary"
	"go-lsm-db/kv"
)

// 当构建 SSTable 时，系统会为每个数据块创建一个 Meta 对象
// 所有块的 Meta 被收集到一个 MetaList 中
// MetaList 被编码并存储在 SSTable 文件的尾部
// 读取 SSTable 时，首先加载 MetaList 索引
// 查找特定键时，先在 MetaList 中二分查找可能包含该键的数据块
// 然后只读取那个特定的数据块，而不需要扫描整个文件

type MetaBlock struct {
	StartOffset uint32 //块在文件中的起始偏移量
	StartKey    kv.Key //块的起始key
	EndKey      kv.Key //块的结束key
}

// MetaList 是多个块元数据的集合，相当于 SSTable 文件的索引部分
type MetaBlockList struct {
	list []MetaBlock
}

func NewMetaList() *MetaBlockList {
	return &MetaBlockList{
		list: make([]MetaBlock, 0),
	}
}

func (ml *MetaBlockList) Add(meta MetaBlock) {
	ml.list = append(ml.list, meta)
}

func (ml *MetaBlockList) GetAt(index int) (MetaBlock, bool) {
	if index < 0 || index >= len(ml.list) {
		return MetaBlock{}, false
	}
	return ml.list[index], true
}

func (ml *MetaBlockList) Size() int {
	return len(ml.list)
}

// |---数量---|---起始偏移量---|---起始key长度---|---起始key---|---结束key长度---|---结束key---|
func (ml *MetaBlockList) Encode() []byte {
	retBuffer := new(bytes.Buffer)

	numBuffer := make([]byte, Uint32Size)
	binary.BigEndian.PutUint32(numBuffer, uint32(len(ml.list)))

	retBuffer.Write(numBuffer)

	for _, meta := range ml.list {
		size := Uint32Size + ReservedKeySize + meta.StartKey.EncodedSizeInBytes() + ReservedKeySize + meta.EndKey.EncodedSizeInBytes()
		buffer := make([]byte, size)

		binary.BigEndian.PutUint32(buffer[0:], meta.StartOffset)

		binary.BigEndian.PutUint32(buffer[Uint32Size:], uint32(meta.StartKey.EncodedSizeInBytes()))
		copy(buffer[Uint32Size+ReservedKeySize:], meta.StartKey.EncodedBytes())

		binary.BigEndian.PutUint32(buffer[Uint32Size+ReservedKeySize+meta.StartKey.EncodedSizeInBytes():], uint32(meta.EndKey.EncodedSizeInBytes()))
		copy(buffer[Uint32Size+ReservedKeySize+meta.StartKey.EncodedSizeInBytes()+ReservedKeySize:], meta.EndKey.EncodedBytes())

		retBuffer.Write(buffer)
	}
	return retBuffer.Bytes()

}

func (ml *MetaBlockList) GetStartKeyOfFirstBlock() (kv.Key, bool) {
	if len(ml.list) == 0 {
		return kv.NewKey(nil, 0), false
	}
	return ml.list[0].StartKey, true
}

func (ml *MetaBlockList) GetEndKeyOfLastBlock() (kv.Key, bool) {
	if len(ml.list) == 0 {
		return kv.NewKey(nil, 0), false
	}
	return ml.list[len(ml.list)-1].EndKey, true
}

func (ml *MetaBlockList) MaybeBlockMetaContaining(key kv.Key) (MetaBlock, int) {
	low, high := 0, ml.Size()-1
	possibleIndex := low
	for low <= high {
		mid := low + (high-low)/2
		meta := ml.list[mid]
		switch key.CompareKeysWithDescendingTimestamp(meta.StartKey) {
		case -1:
			high = mid - 1
		case 0:
			return meta, mid
		case 1:
			possibleIndex = mid
			low = mid + 1
		}
	}
	return ml.list[possibleIndex], possibleIndex
}

func DecodeToBlockMetaList(buffer []byte) *MetaBlockList {
	numberOfBlocks := binary.BigEndian.Uint32(buffer[0:Uint32Size])
	metaBlockList := NewMetaList()
	buffer = buffer[Uint32Size:]
	for i := 0; i < int(numberOfBlocks); i++ {
		startOffset := binary.BigEndian.Uint32(buffer[0:Uint32Size])
		startKeySize := binary.BigEndian.Uint32(buffer[Uint32Size:])
		startingKeyBegin := Uint32Size + ReservedKeySize
		startKeyBuffer := buffer[startingKeyBegin : startingKeyBegin+int(startKeySize)]
		startKey := kv.DecodeFromByte(startKeyBuffer)

		endKeyBegin := startingKeyBegin + int(startKeySize) + ReservedKeySize
		endKeySize := binary.BigEndian.Uint32(buffer[startingKeyBegin+int(startKeySize):])
		endKeyBuffer := buffer[endKeyBegin : endKeyBegin+int(endKeySize)]
		endKey := kv.DecodeFromByte(endKeyBuffer)
		metaBlockList.list = append(metaBlockList.list, MetaBlock{
			StartOffset: startOffset,
			StartKey:    startKey,
			EndKey:      endKey,
		})
		buffer = buffer[endKeyBegin+int(endKeySize):]
	}

	return metaBlockList
}
