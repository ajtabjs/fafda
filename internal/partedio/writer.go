package partedio

import "io"

type Processor func(size int64, reader io.Reader) error

type Writer struct {
	partSize     int64
	totalSize    int64
	totalWritten int64 // total bytes written
	processed    int64 // bytes in current chunk
	closed       bool
	errCh        chan error
	pwriter      *io.PipeWriter
	processor    Processor
	err          error
}

func NewWriter(totalSize int64, partSize int64, processor Processor) *Writer {
	return &Writer{
		totalSize: totalSize,
		partSize:  partSize,
		processor: processor,
		errCh:     make(chan error),
	}
}

func (w *Writer) Write(p []byte) (int, error) {
	if w.closed {
		return 0, ErrClosed
	}

	if w.err != nil {
		return 0, w.err
	}

	if w.pwriter == nil {
		w.startNextPart()
	}

	total := len(p)
	for len(p) > 0 {
		if w.processed+int64(len(p)) > w.partSize {
			n, err := w.pwriter.Write(p[:w.partSize-w.processed])
			if err != nil {
				return total - len(p), err
			}
			w.totalWritten += int64(n)
			if err = w.finishPart(true); err != nil {
				return total - len(p), err
			}
			p = p[n:]
		} else {
			n, err := w.pwriter.Write(p)
			if err != nil {
				return total - len(p), err
			}
			w.processed += int64(n)
			w.totalWritten += int64(n)
			p = p[n:]
		}
	}
	return total, nil
}

func (w *Writer) Close() error {
	if w.closed {
		return ErrClosed
	}
	w.closed = true
	if w.pwriter != nil {
		return w.finishPart(false)
	}
	return nil
}

func (w *Writer) finishPart(startNew bool) error {
	if err := w.pwriter.Close(); err != nil {
		return err
	}
	if err := <-w.errCh; err != nil {
		return err
	}
	if startNew {
		w.startNextPart()
	}
	return nil
}

func (w *Writer) startNextPart() {
	if !w.closed {
		reader, writer := io.Pipe()
		w.pwriter = writer
		w.processed = 0

		remainingSize := w.totalSize - w.totalWritten
		partSize := w.partSize
		if remainingSize < w.partSize {
			partSize = remainingSize
		}

		go func() {
			err := w.processor(partSize, reader)
			if err != nil {
				w.err = err
				_ = reader.CloseWithError(err)
			}
			// Sometime processor is dumbfuck
			// neither read all the data nor return error
			_, _ = io.Copy(io.Discard, reader)
			_ = reader.Close()
			w.errCh <- w.err
		}()
	}
}
