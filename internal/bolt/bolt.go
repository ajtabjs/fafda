package bolt

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"os"
	"path"
	"strings"
	"time"

	nanoid "github.com/matoous/go-nanoid/v2"
	"go.etcd.io/bbolt"

	"fafda/internal"
)

var fileBucket = []byte("files")

type MetaFs struct {
	db *bbolt.DB
}

func NewMetaFs(db *bbolt.DB) (internal.MetaFileSystem, error) {
	metafs := &MetaFs{db: db}

	err := db.Update(func(tx *bbolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(fileBucket)
		if err != nil {
			return fmt.Errorf("failed to create file bucket %w", err)
		}

		if data := bucket.Get([]byte("/")); data == nil {
			root := NewFile("/", true)
			if err := metafs.put(bucket, "/", root); err != nil {
				return err
			}
		}

		return nil
	})

	if err != nil {
		_ = db.Close()
		return nil, err
	}

	return metafs, nil
}

func NewFile(path string, isDir bool) *internal.Node {
	now := time.Now()
	mode := os.FileMode(0644)
	if isDir {
		mode = os.FileMode(0755) | os.ModeDir
	}

	node := &internal.Node{}

	if !isDir {
		node.SetId(nanoid.Must())
	}

	return node.
		SetPath(path).
		SetIsDir(isDir).
		SetSize(0).
		SetMode(mode).
		SetCreatedAt(now).
		SetModTime(now)
}

func (mf *MetaFs) Name() string {
	return "boltdb"
}

func (mf *MetaFs) get(bucket *bbolt.Bucket, path string) (*internal.Node, error) {
	data := bucket.Get([]byte(path))
	if data == nil {
		return nil, internal.ErrNotFound
	}
	return decodeNode(data)
}

func (mf *MetaFs) put(bucket *bbolt.Bucket, path string, node *internal.Node) error {
	data, err := encodeNode(node)
	if err != nil {
		return err
	}
	return bucket.Put([]byte(path), data)
}

func (mf *MetaFs) checkParentDir(bucket *bbolt.Bucket, pathStr string) error {
	parent := path.Dir(pathStr)
	if parent == "/" {
		return nil
	}

	parentNode, err := mf.get(bucket, parent)
	if err != nil {
		return internal.ErrNotFound
	}

	if !parentNode.IsDir() {
		return internal.ErrNotFound
	}
	return nil
}

func (mf *MetaFs) rename(tx *bbolt.Tx, b *bbolt.Bucket, data []byte, oldpath, newpath string) error {
	node, err := decodeNode(data)
	if err != nil {
		return err
	}

	node.SetPath(newpath)
	if err := b.Delete([]byte(oldpath)); err != nil {
		return err
	}

	return mf.put(b, newpath, node)
}

func (mf *MetaFs) Create(pathStr string, isDir bool) (*internal.Node, error) {
	file := NewFile(pathStr, isDir)

	err := mf.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(fileBucket)

		if bucket.Get([]byte(pathStr)) != nil {
			return internal.ErrAlreadyExist
		}

		if err := mf.checkParentDir(bucket, pathStr); err != nil {
			return err
		}

		return mf.put(bucket, pathStr, file)
	})

	if err != nil {
		return nil, err
	}

	return file, nil
}

func (mf *MetaFs) Stat(path string) (*internal.Node, error) {
	if path == "" || path == "/" {
		return NewFile("/", true), nil
	}

	var file *internal.Node
	err := mf.db.View(func(tx *bbolt.Tx) error {
		var err error
		file, err = mf.get(tx.Bucket(fileBucket), path)
		return err
	})

	return file, err
}

func (mf *MetaFs) Ls(pathStr string, limit int, offset int) ([]internal.Node, error) {
	info, err := mf.Stat(pathStr)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return nil, internal.ErrIsNotDir
	}

	var files []internal.Node
	cleanPath := path.Clean(pathStr)

	var prefix []byte
	if cleanPath == "/" {
		prefix = []byte("/")
	} else {
		prefix = []byte(cleanPath + "/")
	}

	err = mf.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(fileBucket)
		c := bucket.Cursor()

		skipped := 0
		for k, v := c.Seek(prefix); k != nil; k, v = c.Next() {
			if cleanPath == "/" {
				if string(k) == "/" {
					continue
				}
				parts := strings.Split(strings.TrimPrefix(string(k), "/"), "/")
				if len(parts) > 1 {
					continue
				}
			} else {
				if !strings.HasPrefix(string(k), string(prefix)) {
					break
				}
				relPath := strings.TrimPrefix(string(k), string(prefix))
				if strings.Contains(relPath, "/") {
					continue
				}
			}

			if skipped < offset {
				skipped++
				continue
			}

			if limit != -1 && limit > 0 && len(files) >= limit {
				break
			}

			file, err := decodeNode(v)
			if err != nil {
				return err
			}
			files = append(files, *file)
		}
		return nil
	})

	return files, err
}

func (mf *MetaFs) Chtimes(path string, mtime time.Time) error {
	return mf.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(fileBucket)

		node, err := mf.get(bucket, path)
		if err != nil {
			return err
		}

		node.SetModTime(mtime)
		return mf.put(bucket, path, node)
	})
}

func (mf *MetaFs) Touch(path string) error {
	_, err := mf.Stat(path)
	if errors.Is(err, internal.ErrNotFound) {
		_, err = mf.Create(path, false)
	}
	return err
}

func (mf *MetaFs) Mkdir(path string) error {
	_, err := mf.Create(path, true)
	return err
}

func (mf *MetaFs) MkdirAll(pathStr string) error {
	pathStr = path.Clean(pathStr)
	if pathStr == "/" {
		return nil
	}

	if _, err := mf.Stat(pathStr); err == nil {
		return nil
	}

	parent := path.Dir(pathStr)
	if parent != "/" {
		if err := mf.MkdirAll(parent); err != nil {
			return err
		}
	}

	_, err := mf.Create(pathStr, true)
	if err != nil && !errors.Is(err, internal.ErrAlreadyExist) {
		return err
	}
	return nil
}

func (mf *MetaFs) Rename(oldpath, newpath string) error {
	oldpath = path.Clean(oldpath)
	newpath = path.Clean(newpath)

	if oldpath == "/" {
		return internal.ErrInvalidRootOperation
	}

	if strings.HasPrefix(newpath, oldpath+"/") {
		return internal.ErrInvalidOperation
	}

	return mf.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(fileBucket)
		if exist := bucket.Get([]byte(newpath)); exist != nil {
			return internal.ErrAlreadyExist
		}

		data := bucket.Get([]byte(oldpath))
		if data == nil {
			return internal.ErrNotFound
		}

		if err := mf.checkParentDir(bucket, newpath); err != nil {
			return err
		}

		if err := mf.rename(tx, bucket, data, oldpath, newpath); err != nil {
			return err
		}

		// Handle renaming of child nodes
		prefix := []byte(oldpath + "/")
		newPrefix := []byte(newpath + "/")
		c := bucket.Cursor()
		var filesToMove [][]byte
		for k, _ := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = c.Next() {
			filesToMove = append(filesToMove, k)
		}
		for _, f := range filesToMove {
			newKey := append(newPrefix, f[len(prefix):]...)
			if err := mf.rename(tx, bucket, bucket.Get(f), string(f), string(newKey)); err != nil {
				return err
			}
		}
		return nil
	})
}

func (mf *MetaFs) Remove(path string) error {
	return mf.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(fileBucket)

		node, err := mf.get(bucket, path)
		if err != nil {
			return err
		}

		if node.IsDir() {
			prefix := path + "/"
			c := bucket.Cursor()
			k, _ := c.Seek([]byte(prefix))

			if k != nil && strings.HasPrefix(string(k), prefix) {
				return internal.ErrNotEmpty
			}
		}

		return bucket.Delete([]byte(path))
	})
}

func (mf *MetaFs) RemoveAll(pathStr string) error {
	pathStr = path.Clean(pathStr)
	if pathStr == "/" {
		return internal.ErrInvalidRootOperation
	}

	return mf.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(fileBucket)

		if data := bucket.Get([]byte(pathStr)); data == nil {
			return nil
		}

		prefix := pathStr + "/"
		c := bucket.Cursor()

		for k, _ := c.Seek([]byte(prefix)); k != nil && strings.HasPrefix(string(k), prefix); k, _ = c.Next() {
			if err := bucket.Delete(k); err != nil {
				return err
			}
		}

		return bucket.Delete([]byte(pathStr))
	})
}

func (mf *MetaFs) Sync(path string, size int64) error {
	return mf.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(fileBucket)

		node, err := mf.get(bucket, path)
		if err != nil {
			return err
		}

		if !node.IsDir() {
			node.SetSize(size)
		}
		node.SetModTime(time.Now())

		return mf.put(bucket, path, node)
	})
}

func (mf *MetaFs) Close() error {
	return mf.db.Close()
}

func encodeNode(node *internal.Node) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(node); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decodeNode(data []byte) (*internal.Node, error) {
	var node internal.Node
	buf := bytes.NewBuffer(data)
	if err := gob.NewDecoder(buf).Decode(&node); err != nil {
		return nil, err
	}
	return &node, nil
}
