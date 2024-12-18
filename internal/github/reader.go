package github

import (
	"fmt"
	"io"

	"fafda/internal/partedio"
)

type Reader struct {
	drvr   *Driver
	reader io.ReadCloser
}

func NewReader(fileId string, pos int64, drvr *Driver) (*Reader, error) {
	assets, err := drvr.ass.Get(fileId)
	if err != nil {
		return nil, err
	}
	if len(assets) == 0 {
		return nil, fmt.Errorf("assets len is zero")
	}
	partReaders := make([]partedio.PartReader, len(assets))
	for i, asset := range assets {
		asset.client = drvr.client
		partReaders[i] = &asset
	}
	reader, err := partedio.NewReader(partReaders, pos)
	if err != nil {
		return nil, err
	}
	return &Reader{reader: reader}, nil
}

func (r *Reader) Read(p []byte) (int, error) {
	return r.reader.Read(p)
}

func (r *Reader) Close() error {
	return r.reader.Close()
}
