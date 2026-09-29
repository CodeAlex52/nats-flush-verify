package nats

import (
	"bytes"
	"errors"
	"testing"
)

// A writer that accepts only a bounded number of bytes on its first call and
// fails that call with a timeout, then behaves normally — the exact sequence
// from #2158: a socket write timeout after the kernel accepted a partial
// buffer.
type partialWriteWriter struct {
	data       bytes.Buffer
	firstLimit int
	calls      int
}

func (w *partialWriteWriter) Write(p []byte) (int, error) {
	w.calls++
	if w.calls == 1 {
		n := w.firstLimit
		if n > len(p) {
			n = len(p)
		}
		w.data.Write(p[:n])
		return n, errPartialWrite
	}
	n, _ := w.data.Write(p)
	return n, nil
}

var errPartialWrite = errors.New("i/o timeout")

func TestFlushRetriesPartialWrite(t *testing.T) {
	calls := &partialWriteWriter{firstLimit: 10}
	w := &natsWriter{w: calls}

	frame := []byte("PUB foo 5\r\nhello\r\n")
	w.bufs = append(w.bufs, frame...)

	if err := w.flush(); err != nil {
		t.Fatalf("flush() returned an error after retrying the partial write: %v", err)
	}
	if got := calls.data.String(); got != string(frame) {
		t.Fatalf("protocol frame is torn: got %q, want %q", got, string(frame))
	}
	if len(w.bufs) != 0 {
		t.Fatalf("flush() left %d buffered bytes behind", len(w.bufs))
	}
}
