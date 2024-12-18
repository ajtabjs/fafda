package filesystem

import (
	"io"
	"os"

	"fafda/internal"
)

type File struct {
	*internal.Node

	flag int

	off      int64
	dirCount int
	written  int64
	writer   io.WriteCloser
	reader   io.ReadCloser

	driver internal.StorageDriver
	meta   internal.MetaFileSystem
}

func NewFile(
	flag int,
	node *internal.Node,
	metafs internal.MetaFileSystem,
	driver internal.StorageDriver,
) *File {
	return &File{
		Node: node,
		flag: flag,

		off:      0,
		dirCount: 0,
		written:  0,
		writer:   nil,
		reader:   nil,

		driver: driver,
		meta:   metafs,
	}
}

func (f *File) Truncate(_ int64) error                 { return internal.ErrNotSupported }
func (f *File) WriteAt(_ []byte, _ int64) (int, error) { return 0, internal.ErrNotSupported }
func (f *File) Sync() error                            { return nil }

func (f *File) Readdirnames(n int) ([]string, error) {
	if !f.IsDir() {
		return nil, internal.ErrIsNotDir
	}
	fi, err := f.Readdir(n)
	names := make([]string, len(fi))
	for i, f := range fi {
		names[i] = f.Name()
	}

	return names, err
}

func (f *File) Readdir(n int) ([]os.FileInfo, error) {
	if !f.IsDir() {
		return nil, internal.ErrIsNotDir
	}

	// If n > 0, return at most n entries
	// If n <= 0, return all remaining entries
	files, err := f.meta.Ls(f.Path(), n, f.dirCount)
	if err != nil {
		return nil, err
	}

	entries := make([]os.FileInfo, len(files))
	for i, file := range files {
		entries[i] = &file
	}

	f.dirCount += len(entries)

	if n > 0 && len(entries) == 0 {
		return entries, io.EOF
	}

	return entries, err
}

func (f *File) Read(p []byte) (n int, err error) {
	if f.IsDir() {
		return 0, internal.ErrIsDir
	}
	if f.reader == nil {
		if err = f.openReadStream(0); err != nil {
			return 0, err
		}
	}
	n, err = f.reader.Read(p)
	// Do not increment n on failed read
	if err != nil && err != io.EOF {
		return n, err
	}
	f.off += int64(n)
	return n, err
}

func (f *File) ReadAt(p []byte, off int64) (n int, err error) {
	if f.IsDir() {
		return 0, internal.ErrIsDir
	}
	if _, err := f.Seek(off, io.SeekCurrent); err != nil {
		return 0, err
	}
	return f.Read(p)
}

func (f *File) WriteString(s string) (ret int, err error) {
	if f.IsDir() {
		return 0, internal.ErrIsDir
	}
	return f.Write([]byte(s))
}

func (f *File) Write(p []byte) (int, error) {
	if f.IsDir() {
		return 0, internal.ErrIsDir
	}

	if !checkFlags(os.O_WRONLY, f.flag) {
		return 0, internal.ErrNotSupported
	}

	if f.writer == nil {
		f.written = 0
		if err := f.driver.Truncate(f.Id()); err != nil {
			return 0, err
		}
		if writer, err := f.driver.GetWriter(f.Id()); err != nil {
			return 0, err
		} else {
			f.writer = writer
		}
	}
	n, err := f.writer.Write(p)
	f.written += int64(n)

	return n, err
}

func (f *File) Seek(offset int64, whence int) (int64, error) {
	if f.IsDir() {
		return 0, internal.ErrIsDir
	}

	pos := int64(0)

	switch whence {
	case io.SeekStart:
		pos = offset
	case io.SeekCurrent:
		pos = f.off + offset
	case io.SeekEnd:
		pos = f.Size() - offset
	}
	if pos < 0 {
		return 0, internal.ErrInvalidSeek
	}
	if f.reader != nil {
		if err := f.reader.Close(); err != nil {
			return 0, err
		}
	}
	f.reader = nil
	if err := f.openReadStream(pos); err != nil {
		return 0, err
	}

	return pos, nil
}

func (f *File) Close() error {
	if f.writer != nil {
		if err := f.writer.Close(); err != nil {
			return err
		}
		if err := f.meta.Sync(f.Path(), f.written); err != nil {
			return err
		}
		f.writer = nil
	}
	if f.reader != nil {
		if err := f.reader.Close(); err != nil {
			return err
		}
		f.reader = nil
	}

	return nil
}

func (f *File) openReadStream(startAt int64) error {
	if reader, err := f.driver.GetReader(f.Id(), startAt); err != nil {
		return err
	} else {
		f.reader = reader
	}
	return nil
}
