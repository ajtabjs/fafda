package internal

import (
	"bytes"
	"encoding/gob"
	"os"
	"path"
	"time"
)

type Node struct {
	id        string
	path      string
	name      string
	isDir     bool
	size      int64
	mode      os.FileMode
	createdAt time.Time
	modTime   time.Time
}

func (n *Node) Id() string                 { return n.id }
func (n *Node) Name() string               { return path.Base(n.path) }
func (n *Node) Size() int64                { return n.size }
func (n *Node) Mode() os.FileMode          { return n.mode }
func (n *Node) ModTime() time.Time         { return n.modTime }
func (n *Node) IsDir() bool                { return n.isDir }
func (n *Node) Sys() interface{}           { return nil }
func (n *Node) Stat() (os.FileInfo, error) { return n, nil }
func (n *Node) Path() string               { return n.path }

func (n *Node) SetId(id string) *Node          { n.id = id; return n }
func (n *Node) SetPath(path string) *Node      { n.path = path; return n }
func (n *Node) SetIsDir(isDir bool) *Node      { n.isDir = isDir; return n }
func (n *Node) SetSize(size int64) *Node       { n.size = size; return n }
func (n *Node) SetMode(mode os.FileMode) *Node { n.mode = mode; return n }
func (n *Node) SetCreatedAt(t time.Time) *Node { n.createdAt = t; return n }
func (n *Node) SetModTime(t time.Time) *Node   { n.modTime = t; return n }

type nodeAlias struct {
	Id        string
	Path      string
	Name      string
	IsDir     bool
	Size      int64
	Mode      os.FileMode
	CreatedAt time.Time
	ModTime   time.Time
}

func (n *Node) GobEncode() ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(nodeAlias{
		Id:        n.id,
		Path:      n.path,
		Name:      n.name,
		IsDir:     n.isDir,
		Size:      n.size,
		Mode:      n.mode,
		CreatedAt: n.createdAt,
		ModTime:   n.modTime,
	}); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (n *Node) GobDecode(data []byte) error {
	var alias nodeAlias
	if err := gob.NewDecoder(bytes.NewBuffer(data)).Decode(&alias); err != nil {
		return err
	}

	n.id = alias.Id
	n.path = alias.Path
	n.name = alias.Name
	n.isDir = alias.IsDir
	n.size = alias.Size
	n.mode = alias.Mode
	n.createdAt = alias.CreatedAt
	n.modTime = alias.ModTime

	return nil
}
