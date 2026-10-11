package obs

import (
	"bytes"
	"io"
	"log/slog"
	"os"
	"sync"
)

// LogRing keeps the most recent log lines in memory so they can be served at
// GET /logs, so logs are readable without access to the host.
type LogRing struct {
	mu    sync.Mutex
	lines [][]byte
	next  int
	full  bool
}

func NewLogRing(size int) *LogRing {
	return &LogRing{lines: make([][]byte, size)}
}

// Write stores one JSON log line. slog's JSON handler emits one line per call.
func (r *LogRing) Write(p []byte) (int, error) {
	line := bytes.TrimRight(p, "\n")
	cp := make([]byte, len(line))
	copy(cp, line)
	r.mu.Lock()
	r.lines[r.next] = cp
	r.next = (r.next + 1) % len(r.lines)
	if r.next == 0 {
		r.full = true
	}
	r.mu.Unlock()
	return len(p), nil
}

// Tail returns up to n most recent lines, oldest first, keeping only lines
// that contain filter (if non-empty).
func (r *LogRing) Tail(n int, filter string) [][]byte {
	r.mu.Lock()
	defer r.mu.Unlock()
	size := r.next
	if r.full {
		size = len(r.lines)
	}
	out := make([][]byte, 0, min(n, size))
	for i := 0; i < size && len(out) < n; i++ {
		idx := (r.next - 1 - i + len(r.lines)) % len(r.lines)
		line := r.lines[idx]
		if filter == "" || bytes.Contains(line, []byte(filter)) {
			out = append(out, line)
		}
	}
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return out
}

// NewLogger writes JSON logs to stdout and to the ring.
func NewLogger(ring *LogRing, level slog.Level) *slog.Logger {
	w := io.MultiWriter(os.Stdout, ring)
	return slog.New(slog.NewJSONHandler(w, &slog.HandlerOptions{Level: level}))
}
