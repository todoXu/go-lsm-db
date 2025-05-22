package wal

import (
	"encoding/binary"
	"fmt"
	"go-lsm-db/kv"
	"go-lsm-db/sstable/block"
	"io"
	"os"
	"path/filepath"
)

type WAL struct {
	fp *os.File
}

func NewWALPath(rootPath string) string {
	walDirectoryPath := filepath.Join(rootPath, "wal")
	if _, err := os.Stat(walDirectoryPath); os.IsNotExist(err) {
		_ = os.MkdirAll(walDirectoryPath, os.ModePerm)
	}
	return walDirectoryPath
}

func NewWAL(id uint64, walDirectoryPath string) (*WAL, error) {
	if err := os.MkdirAll(walDirectoryPath, os.ModePerm); err != nil {
		return nil, fmt.Errorf("创建WAL目录失败: %v", err)
	}

	walPath := filepath.Join(walDirectoryPath, fmt.Sprintf("%v.wal", id))

	fp, err := os.OpenFile(walPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return nil, fmt.Errorf("打开WAL文件失败: %v", err)
	}

	return &WAL{fp: fp}, nil
}

func Recover(path string, callback func(key kv.Key, value kv.Value)) (*WAL, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}
	fileStat, _ := file.Stat()

	bytes1 := make([]byte, fileStat.Size())
	file.ReadAt(bytes1, 0)

	file.Seek(0, 0)
	bytes, _ := io.ReadAll(file)

	for len(bytes) > 0 {
		keySize := binary.BigEndian.Uint32(bytes[0:block.ReservedKeySize])
		bytes = bytes[block.ReservedKeySize:]
		key := kv.DecodeFromByte(bytes[0:keySize])
		bytes = bytes[keySize:]

		valSize := binary.BigEndian.Uint32(bytes[0:block.ReservedValueSize])
		bytes = bytes[block.ReservedValueSize:]
		value := kv.NewValue(bytes[0:valSize])
		bytes = bytes[valSize:]

		callback(key, value)
	}

	return &WAL{fp: file}, nil
}

// | 4 bytes key size | kv.Key | 4 bytes value size | Value |
func (wal *WAL) Append(key kv.Key, value kv.Value) error {
	buffer := make([]byte, block.ReservedKeySize+key.EncodedSizeInBytes()+block.ReservedValueSize+value.SizeInBytes())

	binary.BigEndian.PutUint32(buffer[0:block.ReservedKeySize], uint32(key.EncodedSizeInBytes()))
	copy(buffer[block.ReservedKeySize:], key.EncodedBytes())

	binary.BigEndian.PutUint32(buffer[block.ReservedKeySize+key.EncodedSizeInBytes():], uint32(value.SizeInBytes()))
	copy(buffer[block.ReservedKeySize+key.EncodedSizeInBytes()+block.ReservedValueSize:], value.Bytes())

	_, err := wal.fp.Write(buffer)

	return err
}

func (wal *WAL) Sync() error {
	err := wal.fp.Sync()
	return err
}

func (wal *WAL) Delete() error {
	err := wal.fp.Close()
	if err != nil {
		return err
	}
	err = os.Remove(wal.fp.Name())
	if err != nil {
		return err
	}
	return nil
}

func (wal *WAL) Close() error {
	err := wal.fp.Close()
	return err
}

func (wal *WAL) Path() (string, error) {
	return filepath.Abs(wal.fp.Name())
}
