package sstable

import "os"

type SSTableFile struct {
	fp   *os.File
	size uint64
}

func CreateNewFile(path string, data []byte) *SSTableFile {
	//sstable文件创建后就不可修改
	fp, _ := os.Create(path)
	fp.Write(data)
	fp.Sync()
	fp.Close()
	//只读打开
	fp, _ = os.Open(path)
	return &SSTableFile{
		fp:   fp,
		size: uint64(len(data)),
	}
}

func Open(filePath string) (*SSTableFile, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}
	return &SSTableFile{
		fp:   file,
		size: uint64(stat.Size()),
	}, nil
}

func (f *SSTableFile) Read(offset int64, size uint32) ([]byte, error) {
	buf := make([]byte, size)
	_, err := f.fp.ReadAt(buf, offset)
	if err != nil {
		return nil, err
	}
	return buf, nil
}

func (f *SSTableFile) Size() uint64 {
	return f.size
}

func (f *SSTableFile) Close() error {
	return f.fp.Close()
}
