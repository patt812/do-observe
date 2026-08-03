package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// EventWriter appends JSONL events to daily-rotated files.
// これが一次データ。可視化レイヤー（Grafana等）が何であれ、
// ここのファイルさえ残っていれば後から何度でも再集計できる。
type EventWriter struct {
	mu   sync.Mutex
	dir  string
	site string
	day  string
	f    *os.File
}

func NewEventWriter(dir, site string) (*EventWriter, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &EventWriter{dir: dir, site: site}, nil
}

func (w *EventWriter) Emit(typ string, fields map[string]any) {
	w.mu.Lock()
	defer w.mu.Unlock()

	now := time.Now().UTC()
	day := now.Format("2006-01-02")
	if w.f == nil || day != w.day {
		if w.f != nil {
			w.f.Close()
		}
		path := filepath.Join(w.dir, "events-"+day+".jsonl")
		f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
		if err != nil {
			fmt.Fprintf(os.Stderr, "event writer: open %s: %v\n", path, err)
			return
		}
		w.f, w.day = f, day
	}

	ev := map[string]any{"ts": now.Format(time.RFC3339Nano), "type": typ, "site": w.site}
	for k, v := range fields {
		ev[k] = v
	}
	b, err := json.Marshal(ev)
	if err != nil {
		fmt.Fprintf(os.Stderr, "event writer: marshal: %v\n", err)
		return
	}
	if _, err := w.f.Write(append(b, '\n')); err != nil {
		fmt.Fprintf(os.Stderr, "event writer: write: %v\n", err)
	}
	// DO死イベントは取りこぼしたくないので毎回 fsync する（イベントはレアなので負荷は無視できる）
	w.f.Sync()
}

func (w *EventWriter) Close() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f != nil {
		w.f.Close()
		w.f = nil
	}
}
