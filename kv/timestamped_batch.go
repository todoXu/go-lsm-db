package kv

type Kind int

const (
	EntryKindPut    = 1
	EntryKindDelete = 2
)

type Entry struct {
	key  Key
	val  Value
	kind Kind
}

func (entry Entry) IsKindPut() bool {
	return entry.kind == EntryKindPut
}

func (entry Entry) IsKindDelete() bool {
	return entry.kind == EntryKindDelete
}
func (entry Entry) SizeInBytes() int {
	return entry.key.EncodedSizeInBytes() + entry.val.SizeInBytes()
}

type TimestampedBatch struct {
	entries []Entry
}

func NewTimestampedBatchFrom(batch Batch, commitTimestamp uint64) *TimestampedBatch {
	timestampedBatch := &TimestampedBatch{}
	for _, pair := range batch.pairs {
		if pair.kind == EntryKindPut {
			timestampedBatch.put(NewKey(pair.key, commitTimestamp), pair.value)
		} else if pair.kind == EntryKindDelete {
			timestampedBatch.delete(NewKey(pair.key, commitTimestamp))
		} else {
			panic("unsupported entry kind while converting the Batch to TimestampedBatch")
		}
	}
	return timestampedBatch
}

func (batch *TimestampedBatch) put(key Key, value []byte) {
	batch.entries = append(batch.entries, Entry{
		key:  key,
		val:  NewValue(value),
		kind: EntryKindPut,
	})
}

func (batch *TimestampedBatch) delete(key Key) {
	batch.entries = append(batch.entries, Entry{
		key:  key,
		val:  Value{val: nil},
		kind: EntryKindDelete,
	})
}

func (batch *TimestampedBatch) AllEntries() []Entry {
	return batch.entries
}

func (batch *TimestampedBatch) SizeInBytes() int {
	size := 0
	for _, entry := range batch.entries {
		size += entry.SizeInBytes()
	}
	return size
}
