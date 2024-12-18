package pkg

import (
	"io"
	"os"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/afero"
)

type LogFile struct {
	src           afero.File
	lengthRead    int64
	lengthWritten int64
	name          string
	logger        *zerolog.Logger
}

type LogFS struct {
	src    afero.Fs
	logger *zerolog.Logger
}

func NewLogFS(src afero.Fs) afero.Fs {
	logger := log.With().Str("component", "filesystem").Logger()
	return &LogFS{src: src, logger: &logger}
}

func (lf *LogFS) logOperation(err error, operation string, fields map[string]interface{}) {
	event := lf.logger.Debug()
	if err != nil {
		event = lf.logger.Error().Err(err)
	}

	for key, value := range fields {
		event = event.Interface(key, value)
	}

	event.Str("operation", operation).Send()
}

func (lff *LogFile) logOperation(err error, operation string, fields map[string]interface{}) {
	event := lff.logger.Debug()
	if err != nil {
		event = lff.logger.Error().Err(err)
	}

	fields["name"] = lff.name
	for key, value := range fields {
		event = event.Interface(key, value)
	}

	event.Str("operation", operation).Send()
}

func (lf *LogFS) newLogFile(file afero.File, err error) (afero.File, error) {
	if err != nil {
		return file, err
	}
	return &LogFile{
		src:    file,
		name:   file.Name(),
		logger: lf.logger,
	}, nil
}

func (lf *LogFS) Create(name string) (afero.File, error) {
	file, err := lf.src.Create(name)
	lf.logOperation(err, "CREATE", map[string]interface{}{"name": name})
	return lf.newLogFile(file, err)
}

func (lf *LogFS) Mkdir(name string, perm os.FileMode) error {
	err := lf.src.Mkdir(name, perm)
	lf.logOperation(err, "MKDIR", map[string]interface{}{
		"name": name,
		"perm": perm,
	})
	return err
}

func (lf *LogFS) MkdirAll(path string, perm os.FileMode) error {
	err := lf.src.MkdirAll(path, perm)
	lf.logOperation(err, "MKDIR_ALL", map[string]interface{}{
		"path": path,
		"perm": perm,
	})
	return err
}

func (lf *LogFS) Open(name string) (afero.File, error) {
	file, err := lf.src.Open(name)
	lf.logOperation(err, "OPEN", map[string]interface{}{"name": name})
	return lf.newLogFile(file, err)
}

func (lf *LogFS) OpenFile(name string, flag int, perm os.FileMode) (afero.File, error) {
	file, err := lf.src.OpenFile(name, flag, perm)
	lf.logOperation(err, "OPEN_FILE", map[string]interface{}{
		"name": name,
		"flag": flag,
		"perm": perm,
	})
	return lf.newLogFile(file, err)
}

func (lf *LogFS) Remove(name string) error {
	err := lf.src.Remove(name)
	lf.logOperation(err, "REMOVE", map[string]interface{}{"name": name})
	return err
}

func (lf *LogFS) RemoveAll(path string) error {
	err := lf.src.RemoveAll(path)
	lf.logOperation(err, "REMOVE_ALL", map[string]interface{}{"path": path})
	return err
}

func (lf *LogFS) Rename(oldname, newname string) error {
	err := lf.src.Rename(oldname, newname)
	lf.logOperation(err, "RENAME", map[string]interface{}{
		"oldname": oldname,
		"newname": newname,
	})
	return err
}

func (lff *LogFile) Close() error {
	err := lff.src.Close()
	lff.logOperation(err, "CLOSE", map[string]interface{}{
		"bytes_read":    lff.lengthRead,
		"bytes_written": lff.lengthWritten,
	})
	return err
}

func (lff *LogFile) Read(p []byte) (int, error) {
	n, err := lff.src.Read(p)
	if err == nil {
		lff.lengthRead += int64(n)
	} else if err != io.EOF {
		lff.logOperation(err, "READ", nil)
	}
	return n, err
}

func (lff *LogFile) ReadAt(p []byte, off int64) (int, error) {
	n, err := lff.src.ReadAt(p, off)
	if err == nil {
		lff.lengthRead += int64(n)
	} else if err != io.EOF {
		lff.logOperation(err, "READ_AT", map[string]interface{}{"offset": off})
	}
	return n, err
}

func (lff *LogFile) Seek(offset int64, whence int) (int64, error) {
	n, err := lff.src.Seek(offset, whence)
	if err != nil {
		lff.logOperation(err, "SEEK", map[string]interface{}{
			"offset": offset,
			"whence": whence,
		})
	}
	return n, err
}

func (lff *LogFile) Write(p []byte) (int, error) {
	n, err := lff.src.Write(p)
	if err == nil {
		lff.lengthWritten += int64(n)
	} else {
		lff.logOperation(err, "WRITE", nil)
	}
	return n, err
}

func (lff *LogFile) WriteAt(p []byte, off int64) (int, error) {
	n, err := lff.src.WriteAt(p, off)
	if err == nil {
		lff.lengthWritten += int64(n)
	} else {
		lff.logOperation(err, "WRITE_AT", map[string]interface{}{"offset": off})
	}
	return n, err
}

func (lff *LogFile) WriteString(s string) (int, error) {
	n, err := lff.src.WriteString(s)
	if err == nil {
		lff.lengthWritten += int64(n)
	} else {
		lff.logOperation(err, "WRITE_STRING", nil)
	}
	return n, err
}

func (lff *LogFile) Readdir(count int) ([]os.FileInfo, error) {
	info, err := lff.src.Readdir(count)
	lff.logOperation(err, "READ_DIR", map[string]interface{}{"count": count})
	return info, err
}

func (lff *LogFile) Readdirnames(n int) ([]string, error) {
	names, err := lff.src.Readdirnames(n)
	lff.logOperation(err, "READ_DIR_NAMES", map[string]interface{}{"count": n})
	return names, err
}

func (lff *LogFile) Sync() error {
	err := lff.src.Sync()
	lff.logOperation(err, "SYNC", nil)
	return err
}

func (lff *LogFile) Truncate(size int64) error {
	err := lff.src.Truncate(size)
	lff.logOperation(err, "TRUNCATE", map[string]interface{}{"size": size})
	return err
}

func (lf *LogFS) Name() string {
	return lf.src.Name()
}

func (lf *LogFS) Chmod(name string, mode os.FileMode) error {
	return lf.src.Chmod(name, mode)
}

func (lf *LogFS) Chtimes(name string, atime time.Time, mtime time.Time) error {
	return lf.src.Chtimes(name, atime, mtime)
}

func (lf *LogFS) Chown(name string, uid, gid int) error {
	return lf.src.Chown(name, uid, gid)
}

func (lf *LogFS) Stat(name string) (os.FileInfo, error) {
	return lf.src.Stat(name)
}

func (lff *LogFile) Name() string {
	return lff.name
}

func (lff *LogFile) Stat() (os.FileInfo, error) {
	return lff.src.Stat()
}
