package partedio

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

func TestWriterSinglePart(t *testing.T) {
	var collected []byte
	var receivedSize int64

	processor := func(size int64, reader io.Reader) error {
		receivedSize = size
		data, err := io.ReadAll(reader)
		if err != nil {
			return err
		}
		collected = append(collected, data...)
		return nil
	}

	w := NewWriter(10, 10, processor)

	input := []byte("helloworld")
	n, err := w.Write(input)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if n != len(input) {
		t.Errorf("Expected to write %d bytes, wrote %d", len(input), n)
	}

	err = w.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	if !bytes.Equal(collected, input) {
		t.Errorf("Expected %q, got %q", input, collected)
	}
	if receivedSize != 10 {
		t.Errorf("Expected size 10, got %d", receivedSize)
	}
}

func TestWriterMultiPart(t *testing.T) {
	var parts [][]byte
	var sizes []int64

	processor := func(size int64, reader io.Reader) error {
		data, err := io.ReadAll(reader)
		if err != nil {
			return err
		}
		parts = append(parts, data)
		sizes = append(sizes, size)
		return nil
	}

	w := NewWriter(10, 4, processor)

	input := []byte("helloworld")
	n, err := w.Write(input)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if n != len(input) {
		t.Errorf("Expected to write %d bytes, wrote %d", len(input), n)
	}

	err = w.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	expectedParts := 3
	if len(parts) != expectedParts {
		t.Fatalf("Expected %d parts, got %d", expectedParts, len(parts))
	}

	expectedParts1 := []byte("hell")
	expectedParts2 := []byte("owor")
	expectedParts3 := []byte("ld")

	if !bytes.Equal(parts[0], expectedParts1) {
		t.Errorf("Part 1: expected %q, got %q", expectedParts1, parts[0])
	}
	if !bytes.Equal(parts[1], expectedParts2) {
		t.Errorf("Part 2: expected %q, got %q", expectedParts2, parts[1])
	}
	if !bytes.Equal(parts[2], expectedParts3) {
		t.Errorf("Part 3: expected %q, got %q", expectedParts3, parts[2])
	}

	if sizes[0] != 4 || sizes[1] != 4 || sizes[2] != 2 {
		t.Errorf("Unexpected sizes: %v", sizes)
	}
}

func TestWriterProcessorError(t *testing.T) {
	expectedError := errors.New("processor error")
	processor := func(size int64, reader io.Reader) error {
		return expectedError
	}

	w := NewWriter(10, 5, processor)

	_, err := w.Write([]byte("hello"))
	if !errors.Is(err, expectedError) {
		t.Errorf("Expected error %v, got %v", expectedError, err)
	}
}

func TestWriterWriteAfterClose(t *testing.T) {
	processor := func(size int64, reader io.Reader) error {
		_, err := io.ReadAll(reader)
		return err
	}

	w := NewWriter(10, 5, processor)

	err := w.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	_, err = w.Write([]byte("hello"))
	if !errors.Is(err, ErrClosed) {
		t.Errorf("Expected ErrClosed, got %v", err)
	}
}

func TestWriterLargeWrite(t *testing.T) {
	var totalReceived int

	processor := func(size int64, reader io.Reader) error {
		data, err := io.ReadAll(reader)
		if err != nil {
			return err
		}
		totalReceived += len(data)
		return nil
	}

	totalSize := int64(1000)
	partSize := int64(100)
	w := NewWriter(totalSize, partSize, processor)

	data := make([]byte, 500)
	for i := range data {
		data[i] = byte(i % 256)
	}

	for i := 0; i < 2; i++ {
		n, err := w.Write(data)
		if err != nil {
			t.Fatalf("Write failed: %v", err)
		}
		if n != len(data) {
			t.Errorf("Expected to write %d bytes, wrote %d", len(data), n)
		}
	}

	err := w.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	if totalReceived != 1000 {
		t.Errorf("Expected to receive 1000 bytes, got %d", totalReceived)
	}
}

func TestWriterEarlyClose(t *testing.T) {
	var parts [][]byte
	var sizes []int64

	processor := func(size int64, reader io.Reader) error {
		data, err := io.ReadAll(reader)
		if err != nil {
			return err
		}
		parts = append(parts, data)
		sizes = append(sizes, size)
		return nil
	}

	w := NewWriter(10, 4, processor)

	input := []byte("hello")
	n, err := w.Write(input)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if n != len(input) {
		t.Errorf("Expected to write %d bytes, wrote %d", len(input), n)
	}

	err = w.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	expectedParts := 2
	if len(parts) != expectedParts {
		t.Fatalf("Expected %d parts, got %d", expectedParts, len(parts))
	}

	expectedPart1 := []byte("hell")
	expectedPart2 := []byte("o")
	if !bytes.Equal(parts[0], expectedPart1) {
		t.Errorf("Part 1: expected %q, got %q", expectedPart1, parts[0])
	}
	if !bytes.Equal(parts[1], expectedPart2) {
		t.Errorf("Part 2: expected %q, got %q", expectedPart2, parts[1])
	}

	if sizes[0] != 4 || sizes[1] != 4 {
		t.Errorf("Unexpected sizes: %v", sizes)
	}

	_, err = w.Write([]byte("world"))
	if !errors.Is(err, ErrClosed) {
		t.Errorf("Expected ErrClosed, got %v", err)
	}
}

func TestWriterSizeMismatch(t *testing.T) {
	w := NewWriter(5, 10, func(size int64, r io.Reader) error {
		_, _ = io.ReadAll(r)
		return nil
	})

	data := []byte("hello")
	_, err := w.Write(data)
	if err != nil {
		t.Fatal(err)
	}

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestWriterProcessorNotReading(t *testing.T) {
	w := NewWriter(5, 10, func(size int64, r io.Reader) error {
		return nil
	})

	data := []byte("hello")
	_, err := w.Write(data)
	if err != nil {
		t.Fatal(err)
	}

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}
