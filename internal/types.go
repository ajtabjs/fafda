package internal

import (
	"io"
	"time"
)

type MetaFileSystem interface {
	Name() string
	Create(path string, isDir bool) (*Node, error)
	Stat(path string) (*Node, error)
	Ls(path string, limit int, offset int) ([]Node, error)
	Chtimes(path string, mtime time.Time) error
	Touch(path string) error
	Mkdir(path string) error
	MkdirAll(path string) error
	Remove(path string) error
	RemoveAll(path string) error
	Rename(oldpath, newpath string) error
	Close() error

	// Sync - update node's
	Sync(path string, size int64) error
}

type StorageDriver interface {
	GetReader(fileId string, pos int64) (io.ReadCloser, error)
	GetWriter(fileId string) (io.WriteCloser, error)
	GetSize(fileId string) (int64, error)
	Truncate(fileId string) error
}
