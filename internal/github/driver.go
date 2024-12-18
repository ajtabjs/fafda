package github

import (
	"fmt"
	"io"

	"go.etcd.io/bbolt"

	"fafda/config"
)

const MaxPartSize = (2 * 1024 * 1024 * 1024) - 429496729 // 2GB - 20%

type Driver struct {
	client *Client
	ass    *AssetStore

	partSize    int64
	concurrency int
}

func NewDriver(cfg config.GitHub, db *bbolt.DB) (*Driver, error) {

	if cfg.PartSize <= 0 || cfg.PartSize > MaxPartSize {
		return nil, fmt.Errorf("partSize must be positive and under ")
	}

	client, err := NewClient(cfg)
	if err != nil {
		return nil, err
	}
	ass, err := NewAssetStore(db)
	if err != nil {
		return nil, err
	}

	return &Driver{
		ass:         ass,
		client:      client,
		partSize:    cfg.PartSize,
		concurrency: cfg.Concurrency,
	}, nil
}

func (d *Driver) GetReader(fileId string, pos int64) (io.ReadCloser, error) {
	return NewReader(fileId, pos, d)
}

func (d *Driver) GetWriter(fileId string) (io.WriteCloser, error) {
	return NewWriter(fileId, d)
}

func (d *Driver) GetSize(fileId string) (int64, error) {
	return d.ass.Size(fileId)
}

func (d *Driver) Truncate(fileId string) error {
	return d.ass.Delete(fileId)
}
