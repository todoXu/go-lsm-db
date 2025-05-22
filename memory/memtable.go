package memory

import (
	"bytes"
	"fmt"
	"go-lsm-db/kv"
	"go-lsm-db/wal"
	"path/filepath"

	"github.com/huandu/skiplist"
)

type Memtable struct {
	id             uint64
	sizeInBytes    uint64
	entries        *skiplist.SkipList
	wal            *wal.WAL
	maxSizeInBytes uint64
}

func NewMemtableWithWAL(id uint64, memTableSizeInBytes uint64, walDirectoryPath string) *Memtable {
	wal, err := wal.NewWAL(id, walDirectoryPath)
	if err != nil {
		panic(fmt.Errorf("error creating new WAL: %v", err))
	}
	compareFunc := func(a, b interface{}) int {
		keyA := a.(kv.Key)
		keyB := b.(kv.Key)
		return kv.CompareKeys(keyA, keyB)
	}

	comparator := skiplist.GreaterThanFunc(compareFunc)

	return &Memtable{
		id:             id,
		sizeInBytes:    0,
		maxSizeInBytes: memTableSizeInBytes,
		entries:        skiplist.New(comparator),
		wal:            wal,
	}

}

func RecoverFromWAL(id uint64, memTableSizeInBytes uint64, walDirectoryPath string) (*Memtable, uint64, error) {

	compareFunc := func(a, b interface{}) int {
		keyA := a.(kv.Key)
		keyB := b.(kv.Key)
		return kv.CompareKeys(keyA, keyB)
	}

	memtable := &Memtable{
		id:          id,
		sizeInBytes: memTableSizeInBytes,
		entries:     skiplist.New(skiplist.GreaterThanFunc(compareFunc)),
	}
	var maxTimestamp uint64

	walPath := filepath.Join(walDirectoryPath, fmt.Sprintf("%v.wal", id))
	wal, err := wal.Recover(walPath, func(key kv.Key, value kv.Value) {
		memtable.entries.Set(key, value)
		maxTimestamp = max(maxTimestamp, key.Timestamp())
	})
	if err != nil {
		return nil, 0, err
	}
	memtable.wal = wal
	return memtable, maxTimestamp, nil
}

func (memtable *Memtable) Get(key kv.Key) (kv.Value, bool) {
	elem := memtable.entries.Get(key)
	if elem == nil {
		return kv.NewValue(nil), false
	}
	val := elem.Value.(kv.Value)

	if val.SizeInBytes() == 0 {
		return kv.NewValue(nil), false
	}
	return val, true
}

func (memtable *Memtable) Set(key kv.Key, value kv.Value) error {
	if !memtable.CanFit(key, value) {
		return fmt.Errorf("memtable is full, cannot fit key: %s, value: %s", key.RawBytes(), value.String())
	}

	existingElem := memtable.entries.Get(key)

	var additionalSpace uint64 = 0
	if existingElem != nil {
		existingValue := existingElem.Value.(kv.Value)
		oldSize := existingValue.SizeInBytes()
		newSize := value.SizeInBytes()
		additionalSpace = uint64(newSize - oldSize)
	} else {
		additionalSpace = uint64(key.EncodedSizeInBytes() + value.SizeInBytes())
	}

	//wal是日志 所有写操作都要写入wal
	err := memtable.wal.Append(key, value)
	if err != nil {
		return fmt.Errorf("error appending to WAL: %v", err)
	}

	memtable.entries.Set(key, value)
	memtable.sizeInBytes += additionalSpace

	return nil
}

func (memtable *Memtable) Delete(key kv.Key) error {
	return memtable.Set(key, kv.NewValue(nil))
}

func (memtable *Memtable) CanFit(key kv.Key, value kv.Value) bool {
	existingElem := memtable.entries.Get(key)

	var additionalSpace uint64 = 0
	if existingElem != nil {
		existingValue := existingElem.Value.(kv.Value)
		oldSize := existingValue.SizeInBytes()
		newSize := value.SizeInBytes()
		additionalSpace = uint64(newSize - oldSize)
	} else {
		additionalSpace = uint64(key.EncodedSizeInBytes() + value.SizeInBytes())
	}

	return (memtable.sizeInBytes + additionalSpace) <= memtable.maxSizeInBytes
}

func (memtable *Memtable) Scan(inclusiveRange kv.InclusiveKeyRange) *MemtableIterator {
	iter := &MemtableIterator{nil, inclusiveRange}
	if inclusiveRange.Start().IsRawKeyEmpty() {
		iter.skipListIter = memtable.entries.Front()
		return iter
	}

	for elem := memtable.entries.Front(); elem != nil; elem = elem.Next() {
		currKey := elem.Key().(kv.Key)

		if CompareKeysForRangeCheck(inclusiveRange.Start(), currKey) <= 0 &&
			(inclusiveRange.End().IsRawKeyEmpty() || CompareKeysForRangeCheck(currKey, inclusiveRange.End()) <= 0) {
			iter.skipListIter = elem
			break
		}
	}

	return iter
}

func (memtable *Memtable) AllEntries(callback func(key kv.Key, value kv.Value)) {
	elem := memtable.entries.Front()
	for elem != nil {
		key := elem.Key().(kv.Key)
		value := elem.Value.(kv.Value)
		callback(key, value)
		elem = elem.Next()
	}
}

func (memtable *Memtable) Sync() error {
	if err := memtable.wal.Sync(); err != nil {
		return fmt.Errorf("error syncing WAL: %v", err)
	}
	return nil
}

func (memtable *Memtable) DeleteWAL() error {
	if err := memtable.wal.Delete(); err != nil {
		return fmt.Errorf("error deleting WAL: %v", err)
	}
	return nil
}

func (memtable *Memtable) IsEmpty() bool {
	return memtable.entries.Len() == 0
}

func (memtable *Memtable) SizeInBytes() uint64 {
	return memtable.sizeInBytes
}

func (memtable *Memtable) Id() uint64 {
	return memtable.id
}

func (memtable *Memtable) WalPath() (string, error) {
	if memtable.wal != nil {
		return memtable.wal.Path()
	}
	return "", nil
}

type MemtableIterator struct {
	skipListIter      *skiplist.Element
	inclusiveKeyRange kv.InclusiveKeyRange
}

func (iter *MemtableIterator) IsValid() bool {
	// 如果迭代器为nil，返回false
	if iter.skipListIter == nil {
		return false
	}

	currKey := iter.skipListIter.Key().(kv.Key)

	if !iter.inclusiveKeyRange.Start().IsRawKeyEmpty() {
		start := iter.inclusiveKeyRange.Start()
		startCmp := CompareKeysForRangeCheck(start, currKey)
		if startCmp > 0 {
			return false // 当前键小于起始键
		}
	}

	if !iter.inclusiveKeyRange.End().IsRawKeyEmpty() {
		end := iter.inclusiveKeyRange.End()
		endCmp := CompareKeysForRangeCheck(currKey, end)
		if endCmp > 0 {
			return false // 当前键大于结束键
		}
	}

	return true
}
func (iter *MemtableIterator) Key() kv.Key {
	if !iter.IsValid() {
		panic("迭代器无效，无法获取键")
	}
	return iter.skipListIter.Key().(kv.Key)
}

// Value 返回当前位置的值
func (iter *MemtableIterator) Value() kv.Value {
	if !iter.IsValid() {
		panic("迭代器无效，无法获取值")
	}
	return iter.skipListIter.Value.(kv.Value)
}

func (iter *MemtableIterator) Next() {
	if !iter.IsValid() {
		return
	}
	iter.skipListIter = iter.skipListIter.Next()

	for iter.skipListIter != nil {
		currKey := iter.skipListIter.Key().(kv.Key)

		if CompareKeysForRangeCheck(iter.inclusiveKeyRange.Start(), currKey) <= 0 &&
			(iter.inclusiveKeyRange.End().IsRawKeyEmpty() || CompareKeysForRangeCheck(currKey, iter.inclusiveKeyRange.End()) <= 0) {
			return
		}
		iter.skipListIter = iter.skipListIter.Next()
	}
}

// 只要key在范围内就可以 只有当key相等时才比较时间戳
func CompareKeysForRangeCheck(keyA, keyB kv.Key) int {
	rawCmp := bytes.Compare(keyA.RawBytes(), keyB.RawBytes())
	if rawCmp != 0 {
		return rawCmp
	}

	if keyA.Timestamp() < keyB.Timestamp() {
		return -1
	} else if keyA.Timestamp() > keyB.Timestamp() {
		return 1
	}

	return 0
}
