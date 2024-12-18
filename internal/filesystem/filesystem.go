package filesystem

import (
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/spf13/afero"

	"fafda/internal"
	"fafda/pkg"
)

type Fs struct {
	meta   internal.MetaFileSystem
	driver internal.StorageDriver
}

func New(driver internal.StorageDriver, dp internal.MetaFileSystem) afero.Fs {
	return pkg.NewLogFS(&Fs{driver: driver, meta: dp})
}

func (fs *Fs) Name() string                                  { return "WhyAreYouGayFs" }
func (fs *Fs) Chown(_ string, _, _ int) error                { return internal.ErrNotSupported }
func (fs *Fs) Chmod(_ string, _ os.FileMode) error           { return internal.ErrNotSupported }
func (fs *Fs) Remove(path string) error                      { return fs.meta.Remove(path) }
func (fs *Fs) RemoveAll(path string) error                   { return fs.meta.RemoveAll(path) }
func (fs *Fs) Rename(oldname, newname string) error          { return fs.meta.Rename(oldname, newname) }
func (fs *Fs) Stat(path string) (os.FileInfo, error)         { return fs.meta.Stat(path) }
func (fs *Fs) Chtimes(path string, _, mtime time.Time) error { return fs.meta.Chtimes(path, mtime) }
func (fs *Fs) Mkdir(path string, _ os.FileMode) error        { return fs.meta.Mkdir(path) }
func (fs *Fs) MkdirAll(path string, _ os.FileMode) error     { return fs.meta.MkdirAll(path) }

func (fs *Fs) Create(path string) (afero.File, error) {
	if err := fs.meta.Touch(path); err != nil {
		return nil, err
	}
	return fs.OpenFile(path, os.O_WRONLY, 0666)
}

func (fs *Fs) Open(path string) (afero.File, error) {
	f, err := fs.meta.Stat(path)
	if err != nil {
		return nil, err
	}
	file := NewFile(os.O_RDONLY, f, fs.meta, fs.driver)

	return file, nil
}

func (fs *Fs) OpenFile(name string, flag int, _ os.FileMode) (afero.File, error) {
	allowedFlags := os.O_WRONLY | os.O_RDONLY | os.O_CREATE | os.O_TRUNC

	if !checkFlags(flag, allowedFlags) {
		return nil, fmt.Errorf("flag not supported")
	}

	f, err := fs.meta.Stat(name)
	if err != nil {
		if errors.Is(err, internal.ErrNotFound) && checkFlags(os.O_CREATE, flag) {
			return fs.Create(name)
		}
		return nil, err
	}

	if checkFlags(os.O_TRUNC, flag) {
		if err = fs.driver.Truncate(f.Id()); err != nil {
			return nil, err
		}
		if err = fs.meta.Sync(f.Path(), 0); err != nil {
			return nil, err
		}
	}

	file := NewFile(flag, f, fs.meta, fs.driver)

	return file, nil
}

func checkFlags(flag int, allowedFlags int) bool {
	return flag == (flag & allowedFlags)
}
