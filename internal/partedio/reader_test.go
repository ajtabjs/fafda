package partedio

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

type mockReader struct {
	data   []byte
	offset int
}

func (m *mockReader) Read(p []byte) (n int, err error) {
	if m.offset >= len(m.data) {
		return 0, io.EOF
	}
	n = copy(p, m.data[m.offset:])
	m.offset += n
	return n, nil
}

func (m *mockReader) Close() error {
	return nil
}

// mockPart implements PartReader interface
type mockPart struct {
	size   int
	reader func(start, end int) (io.ReadCloser, error)
}

func (m *mockPart) GetSize() int {
	return m.size
}

func (m *mockPart) GetReader(start, end int) (io.ReadCloser, error) {
	return m.reader(start, end)
}

func TestNewReader(t *testing.T) {
	tests := []struct {
		name    string
		parts   []PartReader
		pos     int64
		wantErr error
	}{
		{
			name:    "empty parts",
			parts:   []PartReader{},
			pos:     0,
			wantErr: ErrNoParts,
		},
		{
			name: "position beyond end",
			parts: []PartReader{
				&mockPart{size: 100},
				&mockPart{size: 50},
			},
			pos:     200,
			wantErr: io.EOF,
		},
		{
			name: "valid parts and position",
			parts: []PartReader{
				&mockPart{size: 100},
				&mockPart{size: 50},
			},
			pos:     0,
			wantErr: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewReader(tt.parts, tt.pos)
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("NewReader() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestReadAcrossParts(t *testing.T) {
	part1Data := bytes.Repeat([]byte("a"), 100)
	part2Data := bytes.Repeat([]byte("b"), 50)
	part3Data := bytes.Repeat([]byte("c"), 75)

	parts := []PartReader{
		&mockPart{
			size: 100,
			reader: func(start, end int) (io.ReadCloser, error) {
				return &mockReader{data: part1Data[start : end+1], offset: 0}, nil
			},
		},
		&mockPart{
			size: 50,
			reader: func(start, end int) (io.ReadCloser, error) {
				return &mockReader{data: part2Data[start : end+1], offset: 0}, nil
			},
		},
		&mockPart{
			size: 75,
			reader: func(start, end int) (io.ReadCloser, error) {
				return &mockReader{data: part3Data[start : end+1], offset: 0}, nil
			},
		},
	}

	tests := []struct {
		name     string
		pos      int64
		readSize int
		want     string
		wantErr  error
	}{
		{
			name:     "read within first part",
			pos:      0,
			readSize: 50,
			want:     string(bytes.Repeat([]byte("a"), 50)),
			wantErr:  nil,
		},
		{
			name:     "read across first and second parts",
			pos:      90,
			readSize: 30,
			want:     string(append(bytes.Repeat([]byte("a"), 10), bytes.Repeat([]byte("b"), 20)...)),
			wantErr:  nil,
		},
		{
			name:     "read to EOF",
			pos:      200,
			readSize: 30,
			want:     string(bytes.Repeat([]byte("c"), 25)),
			wantErr:  io.EOF,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, err := NewReader(parts, tt.pos)
			if err != nil {
				t.Fatalf("NewReader() error = %v", err)
			}
			defer r.Close()

			buf := make([]byte, tt.readSize)
			n, err := r.Read(buf)
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("Read() error = %v, wantErr %v", err, tt.wantErr)
			}

			got := string(buf[:n])
			if got != tt.want {
				t.Errorf("Read() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestReaderClose(t *testing.T) {
	part := &mockPart{
		size: 100,
		reader: func(start, end int) (io.ReadCloser, error) {
			return &mockReader{data: make([]byte, end-start+1), offset: 0}, nil
		},
	}

	r, err := NewReader([]PartReader{part}, 0)
	if err != nil {
		t.Fatalf("NewReader() error = %v", err)
	}

	buf := make([]byte, 10)
	_, err = r.Read(buf)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}

	if err := r.Close(); err != nil {
		t.Errorf("First Close() error = %v", err)
	}

	if err := r.Close(); !errors.Is(err, ErrClosed) {
		t.Errorf("Second Close() error = %v, want %v", err, ErrClosed)
	}

	if _, err := r.Read(buf); !errors.Is(err, ErrClosed) {
		t.Errorf("Read() after Close error = %v, want %v", err, ErrClosed)
	}
}

func TestEmptyRead(t *testing.T) {
	part := &mockPart{
		size: 100,
		reader: func(start, end int) (io.ReadCloser, error) {
			return &mockReader{data: make([]byte, end-start+1), offset: 0}, nil
		},
	}

	r, err := NewReader([]PartReader{part}, 0)
	if err != nil {
		t.Fatalf("NewReader() error = %v", err)
	}
	defer r.Close()

	n, err := r.Read([]byte{})
	if n != 0 || err != nil {
		t.Errorf("Read() empty buffer got = %v, %v, want 0, nil", n, err)
	}
}
