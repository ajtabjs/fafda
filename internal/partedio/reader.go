package partedio

import "io"

type PartReader interface {
	GetSize() int
	GetReader(start, end int) (io.ReadCloser, error)
}

type PartReaders []PartReader

type Reader struct {
	parts  []PartReader
	pos    int64
	curIdx int
	closed bool
	reader io.ReadCloser
	size   int64

	partStarts []int64
	partEnds   []int64
}

func NewReader(parts PartReaders, pos int64) (*Reader, error) {
	if len(parts) == 0 {
		return nil, ErrNoParts
	}

	partStarts := make([]int64, len(parts))
	partEnds := make([]int64, len(parts))

	var offset int64
	for i, part := range parts {
		partStarts[i] = offset
		partEnds[i] = offset + int64(part.GetSize()) - 1
		offset = partEnds[i] + 1
	}

	if pos > offset {
		return nil, io.EOF
	}

	startIdx := 0
	for i := range parts {
		if pos <= partEnds[i] {
			startIdx = i
			break
		}
	}

	return &Reader{
		parts:      parts[startIdx:],
		partStarts: partStarts[startIdx:],
		partEnds:   partEnds[startIdx:],
		pos:        pos,
		size:       offset,
		curIdx:     0,
	}, nil
}

func (r *Reader) Read(p []byte) (int, error) {
	if r.closed {
		return 0, ErrClosed
	}

	if len(p) == 0 {
		return 0, nil
	}

	if r.reader == nil {
		if err := r.readNextPart(); err != nil {
			return 0, err
		}
	}

	var totalRead int
	for totalRead < len(p) {
		nr, err := r.reader.Read(p[totalRead:])
		totalRead += nr
		r.pos += int64(nr)

		if err == nil {
			continue
		}

		if err == io.EOF {
			r.curIdx++
			if r.curIdx >= len(r.parts) {
				return totalRead, io.EOF
			}

			if err = r.readNextPart(); err != nil {
				return totalRead, err
			}
			continue
		}

		return totalRead, err
	}

	return totalRead, nil
}

func (r *Reader) Close() error {
	if r.closed {
		return ErrClosed
	}

	var err error
	if r.reader != nil {
		err = r.reader.Close()
	}

	r.closed = true
	r.reader = nil
	r.parts = nil // Help GC
	r.partStarts = nil
	r.partEnds = nil
	return err
}

func (r *Reader) readNextPart() error {
	if r.reader != nil {
		if err := r.reader.Close(); err != nil {
			return err
		}
	}

	start := 0
	if r.pos > r.partStarts[r.curIdx] {
		start = int(r.pos - r.partStarts[r.curIdx])
	}

	reader, err := r.parts[r.curIdx].GetReader(start, r.parts[r.curIdx].GetSize()-1)
	if err != nil {
		return err
	}

	r.reader = reader
	return nil
}
