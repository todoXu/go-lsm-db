package iterator

import (
	"bytes"
	"container/heap"
	"errors"
	"go-lsm-db/kv"
)

type Iterator interface {
	Key() kv.Key
	Value() kv.Value
	Next() error
	IsValid() bool
	Close()
}

// BoundedMergeIterator 是一个迭代器，它合并多个迭代器并应用时间点边界限制
// 它实现了"时间点查询"语义：
// 1. 只返回在指定时间点存在的数据
// 2. 对于每个键，返回该时间点之前（含）的最新版本
type BoundedMergeIterator struct {
	iterators       *IndexedIteratorMinHeap
	current         IndexedIterator
	endKey          kv.Key
	onCloseCallback OnCloseCallback
	seenKeys        map[string]bool // 用于跟踪已经返回过的用户键
}

func NewBoundedMergeIterator(iterators []Iterator, endKey kv.Key, onCloseCallback OnCloseCallback) *BoundedMergeIterator {
	priorityQueue := &IndexedIteratorMinHeap{}
	heap.Init(priorityQueue)

	for i, iter := range iterators {
		if !iter.IsValid() {
			continue
		}

		for iter.IsValid() && isKeyBeyondBoundary(iter.Key(), endKey) {
			if err := iter.Next(); err != nil {
				break
			}
		}
		heap.Push(priorityQueue, IndexedIterator{index: i, iter: iter})
	}

	bmIter := &BoundedMergeIterator{
		iterators:       priorityQueue,
		endKey:          endKey,
		onCloseCallback: onCloseCallback,
		seenKeys:        make(map[string]bool),
	}

	if priorityQueue.Len() > 0 {
		bmIter.current = heap.Pop(priorityQueue).(IndexedIterator)
		bmIter.skipDuplicateKeys()
	} else {
		bmIter.current = IndexedIterator{index: 0, iter: nothingIterator}
	}

	return bmIter
}

func isKeyBeyondBoundary(key kv.Key, endKey kv.Key) bool {
	if key.Timestamp() > endKey.Timestamp() {
		return true
	}

	if bytes.Compare(key.RawBytes(), endKey.RawBytes()) > 0 {
		return true
	}

	return false
}

func (iter *BoundedMergeIterator) Key() kv.Key {
	return iter.current.iter.Key()
}

func (iter *BoundedMergeIterator) Value() kv.Value {
	return iter.current.iter.Value()
}

func (iter *BoundedMergeIterator) IsValid() bool {
	return iter.iterators != nil && iter.current.iter != nil && iter.current.iter.IsValid()
}

func (iter *BoundedMergeIterator) skipDuplicateKeys() {
	if !iter.IsValid() {
		return
	}

	currentUserKey := string(iter.current.iter.Key().RawBytes())
	iter.seenKeys[currentUserKey] = true

	for iter.iterators.Len() > 0 {
		topIter := (*iter.iterators)[0]
		topUserKey := string(topIter.iter.Key().RawBytes())

		if topUserKey != currentUserKey {
			break
		}

		popped := heap.Pop(iter.iterators).(IndexedIterator)
		if err := popped.iter.Next(); err != nil {
			popped.iter.Close()
			continue
		}

		if !popped.iter.IsValid() {
			popped.iter.Close()
			continue
		}

		if isKeyBeyondBoundary(popped.iter.Key(), iter.endKey) {
			popped.iter.Close()
			continue
		}

		heap.Push(iter.iterators, popped)
	}
}

func (iter *BoundedMergeIterator) Next() error {
	if !iter.IsValid() {
		return errors.New("iterator is not valid")
	}

	var err error

	for {
		if err = iter.current.iter.Next(); err != nil {
			return err
		}

		if !iter.current.iter.IsValid() {
			iter.current.iter.Close()
		} else if isKeyBeyondBoundary(iter.current.iter.Key(), iter.endKey) {
			iter.current.iter.Close()
		} else {
			heap.Push(iter.iterators, iter.current)
		}

		if iter.iterators.Len() == 0 {
			iter.current = IndexedIterator{index: 0, iter: nothingIterator}
			break
		}

		iter.current = heap.Pop(iter.iterators).(IndexedIterator)

		currentUserKey := string(iter.current.iter.Key().RawBytes())
		if !iter.seenKeys[currentUserKey] {
			iter.seenKeys[currentUserKey] = true
			iter.skipDuplicateKeys()
			break
		}

	}

	return nil
}

func (iter *BoundedMergeIterator) Close() {
	if iter.current.iter != nil {
		iter.current.iter.Close()
	}

	for iter.iterators.Len() > 0 {
		popped := heap.Pop(iter.iterators).(IndexedIterator)
		popped.iter.Close()
	}
	iter.current = IndexedIterator{index: 0, iter: nothingIterator}
	iter.iterators = nil

	iter.onCloseCallback()
}
