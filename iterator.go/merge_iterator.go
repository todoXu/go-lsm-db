package iterator

import (
	"bytes"
	"container/heap"
	"go-lsm-db/kv"
)

type IndexedIteratorMinHeap []IndexedIterator

func (h *IndexedIteratorMinHeap) Len() int {
	return len(*h)
}

func (h *IndexedIteratorMinHeap) Less(i, j int) bool {
	return (*h)[i].IsPrioritizedOver((*h)[j])
}

func (heap *IndexedIteratorMinHeap) Swap(i, j int) {
	tmp := (*heap)[i]
	(*heap)[i] = (*heap)[j]
	(*heap)[j] = tmp
}

func (heap *IndexedIteratorMinHeap) Push(element any) {
	*heap = append(*heap, element.(IndexedIterator))
}

func (heap *IndexedIteratorMinHeap) Pop() any {
	n := len(*heap)
	elem := (*heap)[n-1]
	*heap = (*heap)[0 : n-1]
	return elem
}

type IndexedIterator struct {
	index int
	iter  Iterator
}

func NewIndexedIterator(index int, iterator Iterator) IndexedIterator {
	return IndexedIterator{
		index: index,
		iter:  iterator,
	}
}
func (indexedIterator IndexedIterator) IsPrioritizedOver(other IndexedIterator) bool {
	cmp := indexedIterator.iter.Key().CompareKeysWithDescendingTimestamp(other.iter.Key())
	if cmp == 0 {
		return indexedIterator.index < other.index
	}
	return cmp < 0
}

type OnCloseCallback = func()

var NoOperationOnCloseCallback = func() {}

type MergeIterator struct {
	current         IndexedIterator
	iterators       *IndexedIteratorMinHeap
	onCloseCallback OnCloseCallback
}

func NewMergeIterator(iterators []Iterator, onCloseCallback OnCloseCallback) *MergeIterator {
	prioritizedIterators := &IndexedIteratorMinHeap{}
	heap.Init(prioritizedIterators)

	for index, iterator := range iterators {
		if iterator.IsValid() {
			heap.Push(prioritizedIterators, IndexedIterator{index: index, iter: iterator})
		}
	}

	if prioritizedIterators.Len() > 0 {
		return &MergeIterator{
			current:         heap.Pop(prioritizedIterators).(IndexedIterator),
			iterators:       prioritizedIterators,
			onCloseCallback: onCloseCallback,
		}
	}

	return &MergeIterator{
		current:         NewIndexedIterator(0, nothingIterator),
		onCloseCallback: onCloseCallback,
	}
}

func (mergeIterator *MergeIterator) Key() kv.Key {
	return mergeIterator.current.iter.Key()
}

func (mergeIterator *MergeIterator) Value() kv.Value {
	return mergeIterator.current.iter.Value()
}

func (mergeIterator *MergeIterator) IsValid() bool {
	return mergeIterator.current.iter.IsValid()
}

// 跳过所有key相同 时间戳不同的键 例如 apple@300 apple@200 banana@100
// apple@300的next是banana@100
func (mergeIterator *MergeIterator) Next() error {
	mergeIterator.advanceOtherIteratorsOnSameKey()
	return nil
}

func (mergeIterator *MergeIterator) advanceOtherIteratorsOnSameKey() error {
	current := mergeIterator.current

	for mergeIterator.iterators.Len() > 0 {
		topIterator := (*mergeIterator.iterators)[0]
		if !bytes.Equal(current.iter.Key().RawBytes(), topIterator.iter.Key().RawBytes()) {
			break
		}

		if err := mergeIterator.advance(topIterator); err != nil {
			heap.Pop(mergeIterator.iterators).(IndexedIterator).iter.Close()
			return err
		}

		if !topIterator.iter.IsValid() {
			heap.Pop(mergeIterator.iterators).(IndexedIterator).iter.Close()
		} else {
			heap.Fix(mergeIterator.iterators, 0)
		}
	}

	return nil
}

func (mergeIterator *MergeIterator) advance(indexedIterator IndexedIterator) error {
	return indexedIterator.iter.Next()
}
