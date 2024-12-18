package partedio

import (
	"io"
	"sync"
)

type SyncReader struct {
	reader io.Reader
	mu     sync.Mutex
}

func NewSyncReader(r io.Reader) io.Reader {
	return &SyncReader{r, sync.Mutex{}}
}

func (br *SyncReader) Read(p []byte) (int, error) {
	br.mu.Lock()
	defer br.mu.Unlock()

	currReadIdx := 0
	// Loop until p is full
	for currReadIdx < len(p) {
		n, err := br.reader.Read(p[currReadIdx:])
		currReadIdx += n
		if err != nil {
			return currReadIdx, err
		}
	}

	return currReadIdx, nil
}
