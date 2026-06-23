package progress

import (
	"bytes"
	"io"
	"os"
	"strings"
	"testing"
)

// TestWriter_LogBufferedAndFlushedOnStop asserts that Log calls do not write
// to the output immediately and that the messages appear only after Stop.
// The render goroutine is intentionally not started here — the buffering logic
// is independent of the render cycle and testing it without Start avoids a
// race between go-pretty's initForRender reading os.Stdout and the other test
// replacing it.
func TestWriter_LogBufferedAndFlushedOnStop(t *testing.T) {
	var buf bytes.Buffer
	pw := NewWriter(1, false)
	pw.SetOutputWriter(&buf)

	pw.Log("sentinel %d", 42)

	// Nothing flushed yet.
	if buf.Len() != 0 {
		t.Errorf("Log wrote to output before Stop: %q", buf.String())
	}

	pw.Stop()

	if !strings.Contains(buf.String(), "sentinel 42") {
		t.Errorf("flushed output does not contain log message; got: %q", buf.String())
	}
}

// TestWriter_StopAndClear_emitsOneEscapePerTracker captures stdout and
// asserts the count of "\033[K" escape sequences matches the numTrackers
// passed to NewWriter. The escape erases one terminal line; one per tracker
// is what visually clears the rendered bars.
func TestWriter_StopAndClear_emitsOneEscapePerTracker(t *testing.T) {
	const want = 3

	orig := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	os.Stdout = w
	defer func() { os.Stdout = orig }()

	pw := NewWriter(want, true)
	pw.Start()
	pw.StopAndClear()

	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, r); err != nil {
		t.Fatalf("copy: %v", err)
	}

	got := strings.Count(buf.String(), "\033[K")
	if got != want {
		t.Errorf("escape sequences = %d, want %d (output: %q)", got, want, buf.String())
	}
}
