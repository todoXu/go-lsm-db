package wal

import (
	"fmt"
	"os"
	"path/filepath"
)

type WAL struct {
	fp *os.File
}

func NewWAL(id uint32, walDirectoryPath string) (*WAL, error) {
	walPath := filepath.Join(walDirectoryPath, fmt.Sprintf("%v.wal", id))
	if _, err := os.Create(walPath); err != nil {
		return nil, err
	}
	fp, err := os.OpenFile(walPath, os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}
	return &WAL{fp: fp}, nil
}

func (wal *WAL) Write(key, value []byte) error {
	return nil
}
