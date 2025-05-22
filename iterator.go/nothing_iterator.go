package iterator

import (
	"errors"
	"go-lsm-db/kv"
)

type NothingIterator struct{}

var errNoNextSupposedByNothingIterator = errors.New("no support for Next() by NothingIterator")

var nothingIterator = &NothingIterator{}

func (iterator NothingIterator) Key() kv.Key {
	return kv.NewKey(nil, 0)
}

func (iterator *NothingIterator) Value() kv.Value {
	return kv.NewValue(nil)
}

func (iterator *NothingIterator) Next() error {
	return errNoNextSupposedByNothingIterator
}

// IsValid returns false.
func (iterator *NothingIterator) IsValid() bool {
	return false
}

// Close does nothing.
func (iterator NothingIterator) Close() {}
