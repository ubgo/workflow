package middleware_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"os"
	"strings"
	"testing"

	"github.com/ubgo/workflow/middleware"
)

// TestCaptureTeeKeepsAttrsAndGroups: attributes and groups added with
// With and WithGroup reach both the captured records and the tee, with the
// same keys; an empty group changes neither.
func TestCaptureTeeKeepsAttrsAndGroups(t *testing.T) {
	var o outputs
	var teeBuf bytes.Buffer
	tee := slog.NewTextHandler(&teeBuf, &slog.HandlerOptions{
		ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
			if a.Key == slog.TimeKey {
				return slog.Attr{}
			}
			return a
		},
	})
	h := middleware.CaptureOutput(o.sink(), middleware.CaptureTee(tee))(func(ctx context.Context, _ []byte) ([]byte, error) {
		l := middleware.OutputLogger(ctx, nil).With("job", "nightly").WithGroup("g").WithGroup("").With("b", 3)
		l.Info("first", "n", 2)
		l.WithGroup("h").Info("second", "m", 4)
		// slog.Logger drops an empty group itself; the handler must too.
		if hd := l.Handler(); hd.WithGroup("") != hd {
			t.Error("WithGroup(\"\") on the capture handler returned a new handler")
		}
		return nil, nil
	})
	if _, err := h(stepCtx(), nil); err != nil {
		t.Fatal(err)
	}
	got := o.all()
	if len(got) != 1 || len(got[0].Records) != 2 {
		t.Fatalf("outputs = %+v, want one with two records", got)
	}
	for i, want := range []string{"job,g.b,g.n", "job,g.b,g.h.m"} {
		var keys []string
		for _, a := range got[0].Records[i].Attrs {
			keys = append(keys, a.Key)
		}
		if strings.Join(keys, ",") != want {
			t.Errorf("record %d keys = %v, want %s", i, keys, want)
		}
	}
	wantTee := "level=INFO msg=first job=nightly g.b=3 g.n=2\nlevel=INFO msg=second job=nightly g.b=3 g.h.m=4\n"
	if teeBuf.String() != wantTee {
		t.Fatalf("tee =\n%q\nwant\n%q", teeBuf.String(), wantTee)
	}
}

// failWriter fails every write.
type failWriter struct{ err error }

func (w failWriter) Write([]byte) (int, error) { return 0, w.err }

func TestFormatOutputWithoutInfoAndWriteError(t *testing.T) {
	var buf bytes.Buffer
	if err := middleware.FormatOutput(&buf, middleware.Output{Finished: outT0}); err != nil {
		t.Fatal(err)
	}
	if want := "== 2026-10-13T09:00:00Z ok\n"; buf.String() != want {
		t.Fatalf("format = %q, want %q", buf.String(), want)
	}
	errDisk := errors.New("disk full")
	if err := middleware.FormatOutput(failWriter{errDisk}, middleware.Output{Finished: outT0}); !errors.Is(err, errDisk) {
		t.Fatalf("FormatOutput on a failing writer = %v, want %v", err, errDisk)
	}
	if err := middleware.WriterSink(failWriter{errDisk}).Deliver(context.Background(), middleware.Output{}); !errors.Is(err, errDisk) {
		t.Fatalf("WriterSink on a failing writer = %v, want %v", err, errDisk)
	}
}

// TestFileSinkWriteError: a write that fails after the file opened is
// returned. /dev/full accepts the open and fails every write with ENOSPC.
func TestFileSinkWriteError(t *testing.T) {
	if _, err := os.Stat("/dev/full"); err != nil {
		t.Skip("no /dev/full on this system")
	}
	for _, mode := range []middleware.FileMode{middleware.FileReplace, middleware.FileAppend} {
		if err := middleware.FileSink("/dev/full", mode).Deliver(context.Background(), middleware.Output{Finished: outT0}); err == nil {
			t.Errorf("mode %d: FileSink into /dev/full succeeded", mode)
		}
	}
}
