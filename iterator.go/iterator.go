package iterator

import "go-lsm-db/kv"

type Iterator interface {
	Key() kv.Key
	Value() kv.Value
	Next() error
	IsValid() bool
	Close()
}
