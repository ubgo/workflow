package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

// ChunkOptions configures ProcessChunks.
type ChunkOptions struct {
	// Total is the number of items in all, when known; it turns progress
	// into a percentage. Zero means unknown: Percent stays 0 until the
	// end and Message counts items only.
	Total int64

	// Size is the number of items per chunk; at least 1.
	Size int64

	// MaxChunks bounds the loop, so a source that never runs dry cannot
	// keep the step busy for ever. Zero means Total/Size + 1 when Total is
	// set, and is required when it is not.
	MaxChunks int
}

// ChunkProgress is the Data ProcessChunks puts in each Progress report,
// as JSON, for a page that wants the numbers rather than the message.
type ChunkProgress struct {
	Done  int64 `json:"done"`
	Total int64 `json:"total,omitempty"`
}

// ProcessChunks runs an export or import as chunks of Size items with
// progress (SA-05): it calls fn with each chunk's offset and size until
// fn handles fewer items than size (the source ran dry), reporting
// progress after every chunk with ReportProgress, so a page can show how
// far the job has got. It returns the number of items handled.
//
// Each chunk is a Loop iteration, memoised in the journal, so a retried
// step resumes at the first chunk that did not finish instead of starting
// over; it needs the Journal plugin. fn must therefore be idempotent per
// chunk (an upsert, or a file part named by its offset), since a crash
// after fn and before the journal's save runs that chunk again.
//
// The caller asked for progress, so a driver that keeps none is not
// silently ignored: the first report's UnsupportedError fails the step
// (Permanent). Reaching MaxChunks is a Permanent ErrLoopLimit.
func ProcessChunks(ctx context.Context, name string, opts ChunkOptions, fn func(ctx context.Context, offset, size int64) (handled int64, err error)) (int64, error) {
	if opts.Size < 1 || opts.Total < 0 || opts.MaxChunks < 0 {
		return 0, Permanent(invalidf("chunks %q: Size must be at least 1, Total and MaxChunks not negative", name))
	}
	limit := opts.MaxChunks
	if limit == 0 {
		if opts.Total == 0 {
			return 0, Permanent(invalidf("chunks %q: MaxChunks is required when Total is unknown", name))
		}
		limit = int(opts.Total/opts.Size) + 1
	}
	return Loop(ctx, name, int64(0), limit, func(ctx context.Context, _ int, offset int64) (int64, bool, error) {
		n, err := fn(ctx, offset, opts.Size)
		if err != nil {
			return offset, false, err
		}
		if n < 0 || n > opts.Size {
			return offset, false, Permanent(invalidf("chunks %q: fn handled %d items of a chunk of %d", name, n, opts.Size))
		}
		done := offset + n
		last := n < opts.Size || (opts.Total > 0 && done >= opts.Total)
		if err := reportChunk(ctx, opts.Total, done, last); err != nil {
			return offset, false, err
		}
		return done, last, nil
	})
}

// reportChunk reports done of total; an UnsupportedError is Permanent.
func reportChunk(ctx context.Context, total, done int64, last bool) error {
	p := Progress{Message: fmt.Sprintf("%d items", done)}
	switch {
	case last:
		p.Percent = 100
	case total > 0:
		p.Percent = min(float64(done)/float64(total)*100, 100)
	}
	if total > 0 {
		p.Message = fmt.Sprintf("%d of %d items", done, total)
	}
	p.Data, _ = json.Marshal(ChunkProgress{Done: done, Total: total}) // two int64 fields always encode
	err := ReportProgress(ctx, p)
	if errors.Is(err, ErrUnsupported) {
		return Permanent(err)
	}
	return err
}
