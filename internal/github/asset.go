package github

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"sort"

	"go.etcd.io/bbolt"
)

type Asset struct {
	Id         int
	Name       string
	Username   string
	Repository string
	ReleaseId  int
	ReleaseTag string
	Size       int
	Number     int

	client *Client
}

func (a *Asset) GetSize() int {
	return a.Size
}

func (a *Asset) GetReader(start, end int) (io.ReadCloser, error) {
	return a.client.DownloadAsset(a, start, end)
}

func (a *Asset) url() string {
	return fmt.Sprintf(
		"%s/repos/%s/%s/releases/assets/%d",
		apiURL, a.Username, a.Repository, a.Id,
	)
}

func (a *Asset) publicURL() string {
	return fmt.Sprintf(
		"https://github.com/%s/%s/releases/download/%s/%s",
		a.Username, a.Repository, a.ReleaseTag, a.Name,
	)
}

type AssetStore struct {
	db         *bbolt.DB
	bucketName []byte
}

func NewAssetStore(db *bbolt.DB) (*AssetStore, error) {

	err := db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(assetBucket)
		if err != nil {
			return fmt.Errorf("failed to create file bucket %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return &AssetStore{
		db:         db,
		bucketName: assetBucket,
	}, nil
}

func (ass *AssetStore) Write(fileId string, assets []*Asset) error {
	return ass.db.Update(func(tx *bbolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(ass.bucketName)
		if err != nil {
			return err
		}

		var buf bytes.Buffer
		if err := gob.NewEncoder(&buf).Encode(assets); err != nil {
			return err
		}

		key := []byte(fileId)
		return bucket.Put(key, buf.Bytes())
	})
}

func (ass *AssetStore) Get(fileId string) ([]Asset, error) {
	var assets []Asset

	err := ass.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(ass.bucketName)
		if bucket == nil {
			return nil
		}

		data := bucket.Get([]byte(fileId))
		if data == nil {
			return nil
		}

		buf := bytes.NewBuffer(data)
		return gob.NewDecoder(buf).Decode(&assets)
	})
	sort.Slice(assets, func(i, j int) bool {
		return assets[i].Number < assets[j].Number
	})
	return assets, err
}

func (ass *AssetStore) Size(fileId string) (int64, error) {
	assets, err := ass.Get(fileId)
	if err != nil {
		return 0, err
	}
	size := int64(0)
	for _, asset := range assets {
		size += int64(asset.Size)
	}
	return size, err
}

func (ass *AssetStore) Delete(fileId string) error {
	return ass.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(ass.bucketName)
		if bucket == nil {
			return nil
		}

		return bucket.Delete([]byte(fileId))
	})
}
