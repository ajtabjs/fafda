package github

import (
	"io"
	"math/rand"

	"fafda/internal/partedio"
)

type Writer struct {
	fileId string
	drvr   *Driver
	writer io.WriteCloser
	assets []*Asset
}

func NewWriter(fileId string, drvr *Driver) (*Writer, error) {
	writer := &Writer{
		fileId: fileId,
		drvr:   drvr,
		assets: make([]*Asset, 0),
	}

	partSize := randomPartSize(drvr.partSize, 20)
	w, err := partedio.NewNWriter(partSize, drvr.concurrency, writer.processor)
	if err != nil {
		return nil, err
	}

	writer.writer = w
	return writer, nil
}

func (w *Writer) Assets() []*Asset {
	return w.assets
}

func (w *Writer) processor(partNum int, partSize int64, data []byte) error {
	assetName := getRandomAssetName()
	asset, err := w.drvr.client.UploadAsset(assetName, partSize, data)
	if err != nil {
		return err
	}
	asset.Number = partNum
	w.assets = append(w.assets, asset)
	return nil
}

func (w *Writer) Write(p []byte) (int, error) {
	return w.writer.Write(p)
}

func (w *Writer) Close() error {
	if err := w.writer.Close(); err != nil {
		return err
	}
	return w.drvr.ass.Write(w.fileId, w.assets)
}

func randomPartSize(baseNumber int64, percentageRange int) int64 {
	minValue := float64(baseNumber) * (1 - float64(percentageRange)/100)
	maxValue := float64(baseNumber) * (1 + float64(percentageRange)/100)

	return int64(minValue + rand.Float64()*(maxValue-minValue))
}
