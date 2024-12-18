package partedio

import (
	"errors"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestNewNWriter(t *testing.T) {
	tests := []struct {
		name        string
		partSize    int64
		handler     PartHandler
		concurrency int
		wantErr     bool
	}{
		{
			name:        "valid parameters",
			partSize:    100,
			handler:     func(int, int64, io.Reader) error { return nil },
			concurrency: 5,
			wantErr:     false,
		},
		{
			name:        "zero part size",
			partSize:    0,
			handler:     func(int, int64, io.Reader) error { return nil },
			concurrency: 5,
			wantErr:     true,
		},
		{
			name:        "negative part size",
			partSize:    -1,
			handler:     func(int, int64, io.Reader) error { return nil },
			concurrency: 5,
			wantErr:     true,
		},
		{
			name:        "nil handler",
			partSize:    100,
			handler:     nil,
			concurrency: 5,
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewNWriter(tt.partSize, tt.concurrency, tt.handler)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewNWriter() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestNWriterWrite(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		partSize    int64
		concurrency int
		wantParts   int64
		wantErr     bool
		handler     func(t *testing.T) PartHandler
	}{
		{
			name:        "write empty input",
			input:       "",
			partSize:    4,
			concurrency: 2,
			wantParts:   0,
			wantErr:     false,
			handler: func(t *testing.T) PartHandler {
				return func(partNum int, size int64, r io.Reader) error {
					t.Error("handler should not be called for empty input")
					return nil
				}
			},
		},
		{
			name:        "write single part",
			input:       "test",
			partSize:    4,
			concurrency: 2,
			wantParts:   1,
			wantErr:     false,
			handler: func(t *testing.T) PartHandler {
				return func(partNum int, size int64, r io.Reader) error {
					data, err := io.ReadAll(r)
					if err != nil {
						t.Errorf("failed to read part: %v", err)
					}
					if string(data) != "test" {
						t.Errorf("part data = %s, want %s", string(data), "test")
					}
					return nil
				}
			},
		},
		{
			name:        "write multiple parts",
			input:       "testdata",
			partSize:    4,
			concurrency: 2,
			wantParts:   2,
			wantErr:     false,
			handler: func(t *testing.T) PartHandler {
				var mu sync.Mutex
				parts := make(map[int]string)
				return func(partNum int, size int64, r io.Reader) error {
					data, err := io.ReadAll(r)
					if err != nil {
						t.Errorf("failed to read part: %v", err)
					}
					mu.Lock()
					parts[partNum] = string(data)
					mu.Unlock()
					return nil
				}
			},
		},
		{
			name:        "handler returns error",
			input:       "test",
			partSize:    4,
			concurrency: 2,
			wantParts:   1,
			wantErr:     true,
			handler: func(t *testing.T) PartHandler {
				return func(partNum int, size int64, r io.Reader) error {
					return errors.New("handler error")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var processedParts int64
			handler := func(partNum int, size int64, r io.Reader) error {
				atomic.AddInt64(&processedParts, 1)
				return tt.handler(t)(partNum, size, r)
			}

			w, err := NewNWriter(tt.partSize, tt.concurrency, handler)
			if err != nil {
				t.Fatalf("failed to create writer: %v", err)
			}

			_, err = w.Write([]byte(tt.input))
			if err != nil && !tt.wantErr {
				t.Errorf("Write() unexpected error = %v", err)
			}

			err = w.Close()
			if (err != nil) != tt.wantErr {
				t.Errorf("Close() error = %v, wantErr %v", err, tt.wantErr)
			}

			if processedParts != tt.wantParts {
				t.Errorf("processed %d parts, want %d", processedParts, tt.wantParts)
			}
		})
	}
}

func TestNWriterConcurrentWrites(t *testing.T) {
	input := strings.Repeat("test", 1000)
	partSize := int64(4)
	concurrency := 5

	var maxConcurrent int32
	var currentConcurrent int32
	var mu sync.Mutex
	processed := make(map[int]bool)

	handler := func(partNum int, size int64, r io.Reader) error {
		current := atomic.AddInt32(&currentConcurrent, 1)
		defer atomic.AddInt32(&currentConcurrent, -1)

		for {
			cur := atomic.LoadInt32(&maxConcurrent)
			if current > cur {
				if atomic.CompareAndSwapInt32(&maxConcurrent, cur, current) {
					break
				}
				continue
			}
			break
		}

		time.Sleep(time.Millisecond)

		mu.Lock()
		if processed[partNum] {
			mu.Unlock()
			t.Errorf("part %d processed multiple times", partNum)
			return nil
		}
		processed[partNum] = true
		mu.Unlock()

		return nil
	}

	w, err := NewNWriter(partSize, concurrency, handler)
	if err != nil {
		t.Fatalf("failed to create writer: %v", err)
	}

	_, err = w.Write([]byte(input))
	if err != nil {
		t.Errorf("Write() error = %v", err)
	}

	err = w.Close()
	if err != nil {
		t.Errorf("Close() error = %v", err)
	}

	if maxConcurrent != int32(concurrency) {
		t.Errorf("max concurrent executions = %d, want %d", maxConcurrent, concurrency)
	}

	expectedParts := int64(len(input)) / partSize
	if len(input)%int(partSize) > 0 {
		expectedParts++
	}

	if int64(len(processed)) != expectedParts {
		t.Errorf("processed %d parts, want %d", len(processed), expectedParts)
	}
}
