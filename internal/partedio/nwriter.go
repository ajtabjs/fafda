package partedio

import (
	"fmt"
	"io"
	"sync"
	"sync/atomic"
)

type PartHandler func(partNum int, contentLength int64, data []byte) error

var (
	pool     *sync.Pool
	initOnce sync.Once
)

type NWriter struct {
	partSize    int64
	concurrency int
	handler     PartHandler
	closed      bool
	pwriter     *io.PipeWriter
	partCount   int64
	err         error

	mu sync.Mutex
	wg sync.WaitGroup
}

func NewNWriter(partSize int64, concurrency int, handler PartHandler) (io.WriteCloser, error) {
	if partSize <= 0 || concurrency <= 0 {
		return nil, fmt.Errorf("part size and concurrency must be positive")
	}
	if handler == nil {
		return nil, fmt.Errorf("handler function cannot be nil")
	}

	initOnce.Do(func() {
		pool = &sync.Pool{
			New: func() interface{} {
				return make([]byte, partSize)
			},
		}
	})

	reader, writer := io.Pipe()
	w := &NWriter{
		handler:     handler,
		partSize:    partSize,
		pwriter:     writer,
		concurrency: concurrency,
	}
	go w.startWriting(NewSyncReader(reader))

	return w, nil
}

func (nw *NWriter) Write(p []byte) (int, error) {
	if nw.closed {
		return 0, ErrClosed
	}
	if nw.err != nil {
		return 0, nw.err
	}
	return nw.pwriter.Write(p)
}

func (nw *NWriter) Close() error {
	if nw.closed {
		return ErrClosed
	}
	nw.closed = true
	if nw.pwriter != nil {
		if err := nw.pwriter.Close(); err != nil {
			return err
		}
	}
	nw.wg.Wait()
	return nw.getErr()
}

func (nw *NWriter) startWriting(src io.Reader) {
	reader := NewSyncReader(src)
	nw.wg.Add(nw.concurrency)

	for i := 0; i < nw.concurrency; i++ {
		go func() {
			defer nw.wg.Done()

			buffer := pool.Get().([]byte)
			defer pool.Put(buffer)

			for nw.getErr() == nil {
				n, err := reader.Read(buffer)
				if err != nil && err != io.EOF {
					nw.setErr(err)
					return
				}
				if n > 0 {
					partNum := atomic.AddInt64(&nw.partCount, 1)
					if perr := nw.handler(int(partNum), int64(n), buffer[:n]); perr != nil {
						nw.setErr(perr)
						return
					}
				}
				if err == io.EOF {
					return
				}
			}
		}()
	}
}

func (nw *NWriter) setErr(err error) {
	nw.mu.Lock()
	if nw.err == nil {
		nw.err = err
	}
	nw.mu.Unlock()
}

func (nw *NWriter) getErr() error {
	nw.mu.Lock()
	defer nw.mu.Unlock()
	return nw.err
}
