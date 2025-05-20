package kv

import (
	"bytes"
	"fmt"
)

type RawKeyValuePair struct {
	key   []byte
	value []byte
	kind  Kind
}

func (kv RawKeyValuePair) Key() []byte {
	return kv.key
}

func (kv RawKeyValuePair) Value() []byte {
	return kv.value
}

type Batch struct {
	pairs []RawKeyValuePair
}

func NewBatch() *Batch {
	return &Batch{}
}

func (batch *Batch) Put(key, value []byte) bool {
	if batch.Contains(key) {
		fmt.Println("Duplicate key in batch")
		return false
	}
	batch.pairs = append(batch.pairs, RawKeyValuePair{
		key:   key,
		value: value,
		kind:  EntryKindPut,
	})
	return true
}

// Delete 方法的实现添加了一个新的键值对（而非删除现有的键值对）是 LSM-Tree 数据库常用的设计模式，原因有以下几点：

// LSM-Tree 的写入只追加特性：

// LSM-Tree 是基于追加写入的数据结构，不直接修改已有数据
// 实际的删除操作通过写入一个特殊的"删除标记"（墓碑记录）来实现
// 原来的 pair 仍然存在：

// 原来的键值对并不会立即从数据库中移除
// 当系统读取数据时，如果发现某个键有删除标记且时间戳更新，则认为该键已被删除
// 延迟删除机制：

// 实际的物理删除发生在后续的合并（compaction）操作中
// 当合并器处理到一个键的删除标记时，会将该键的所有旧版本（包括删除标记本身）从新生成的 SSTable 中排除

// 在 LSM-Tree 架构中，即使要删除的 key 在当前 batch 中不存在，创建删除标记仍然是必要的，原因如下：

// 删除可能存在于其他位置的数据：

// 该 key 可能存在于内存表(MemTable)中
// 该 key 可能已经刷入磁盘的 SSTable 文件中
// 该 key 可能由另一个并发事务写入
// 删除标记的作用：

// 墓碑记录会被持久化到 SSTable
// 在后续的查询中，如果发现某个 key 的最新记录是删除标记，则认为该 key 已被删除
// 在合并(compaction)过程中，删除标记会导致该 key 的所有旧版本被物理删除
// 逻辑删除而非物理删除：

// LSM-Tree 使用"逻辑删除"模式，将删除操作视为一种特殊的写操作

// 这种设计简化了并发控制并提高了写入性能
func (batch *Batch) Delete(key []byte) {
	batch.pairs = append(batch.pairs, RawKeyValuePair{
		key:   key,
		value: nil,
		kind:  EntryKindDelete,
	})
}

func (batch *Batch) Get(key []byte) ([]byte, bool) {
	for _, pair := range batch.pairs {
		if bytes.Equal(pair.key, key) {
			return pair.value, true
		}
	}
	return nil, false
}

func (batch *Batch) Contains(key []byte) bool {
	_, ok := batch.Get(key)
	return ok
}

func (batch *Batch) IsEmpty() bool {
	return len(batch.pairs) == 0
}

func (batch *Batch) Length() int {
	return len(batch.pairs)
}
