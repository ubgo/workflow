package workflow

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ubgo/workflow/clock"
)

// ErrStreamFull means a MemoryStream already holds MemoryStreamOptions.MaxRuns
// open runs and none can be evicted, so a chunk for a new run is refused
// rather than pushing out a run someone may still be reading.
//
//ds:def id=errstreamfull-yqqf883d owner=@khanakia stability=stable
var ErrStreamFull = errors.New("workflow: stream store is full")

// StreamChunk is one piece of output a step sent with Emit (a token of a
// model's answer, a line of a long export), as a StreamReader returns it.
//
//ds:def id=streamchunk-p96pj84v owner=@khanakia stability=stable
type StreamChunk struct {
	// Pos is the chunk's position in its run, from 1, given by the store
	// in the order chunks arrived. Read's after and the SSE event ID of
	// the httprun package are a Pos, so a reader that reconnects asks for
	// what came after the last chunk it saw. A gap means the store
	// dropped older chunks to stay within its bounds.
	Pos int64

	// Step is the step that emitted it.
	Step string

	// Attempt is the step attempt that emitted it (StepInfo.Attempt);
	// zero when the store does not know it.
	Attempt int

	// Seq is the chunk's number within its step attempt, from 1, as Emit
	// numbered it. A retried attempt starts again at 1, so a reader
	// showing one answer can start over when Attempt changes.
	Seq int64

	// Data is the chunk as the step emitted it.
	Data []byte

	// At is when the store received it, on the store's clock.
	At time.Time
}

// StreamSink receives the chunks steps emit. The Stream plugin puts one on
// every step's ctx; Emit hands each chunk to it.
//
// ctx is the step attempt's ctx, so a sink can read StepInfoFrom(ctx)
// (the attempt, labels, tenant). seq counts chunks within one step
// attempt from 1; together with the attempt it lets a durable sink make a
// repeated write idempotent. chunk belongs to the sink (Emit passes a
// copy).
type StreamSink interface {
	Write(ctx context.Context, run RunID, step string, seq int64, chunk []byte) error
}

// StreamReader reads a run's chunks back, for a live view (the httprun
// SSE route) that shows output while the run is still going.
type StreamReader interface {
	// Read returns the run's chunks with a Pos greater than after, oldest
	// first; after zero means from the start. A run the store does not
	// hold (never written, or already dropped) returns no chunks and no
	// error, since "no output yet" and "no output" look the same to a
	// reader.
	Read(ctx context.Context, run RunID, after int64) ([]StreamChunk, error)

	// Subscribe returns a channel that receives the run's chunks as they
	// arrive, and a function that ends the subscription (safe to call
	// more than once). The channel is closed when the subscription ends:
	// by that function, when ctx ends, when the run's stream is closed,
	// or when the reader falls too far behind. A closed channel therefore
	// means "resynchronise": call Read with the last Pos seen to catch up,
	// and Subscribe again if the run is still going. Subscribe first and
	// Read second, skipping chunks already seen, to miss nothing between
	// the two calls.
	Subscribe(ctx context.Context, run RunID) (<-chan StreamChunk, func())
}

// StreamCloser is an optional interface of a StreamSink: Close marks a
// run's stream finished, so its subscribers' channels close and the store
// may drop the run. The Stream plugin calls it when a run ends if it was
// built with StreamCloseOnFinish.
type StreamCloser interface {
	Close(ctx context.Context, run RunID) error
}

// streamBinding is what StreamMiddleware puts on a step attempt's ctx.
type streamBinding struct {
	sink StreamSink
	run  RunID
	step string
	seq  atomic.Int64
}

type streamKey struct{}

// Emit sends chunk, a piece of the step's output, to the StreamSink the
// Stream plugin bound to this step, so a reader (the httprun SSE route, a
// test) sees partial output before the step returns: the tokens of a
// model's answer, the lines of a long report. The step's return value is
// still its output; Emit is a side channel and never changes the run.
//
// chunk is copied, so the caller may reuse its buffer. Chunks of one step
// attempt are numbered from 1 (StreamChunk.Seq). Without the Stream plugin
// (or StreamMiddleware) on the client, or outside a step, it returns an
// UnsupportedError, since the caller asked for output to be delivered and
// dropping it silently would be a lie; a step that streams only as a
// nicety can ignore that error. Any error from the sink (a MemoryStream
// bound, ErrStreamFull) is returned as is.
//
// It runs on every driver: the plugin is step middleware. With
// MemoryStream the chunks live in the process that ran the step, so a
// reader in another process sees nothing; a fleet needs a shared store
// (a table, a Redis stream) implementing StreamSink and StreamReader.
//
//ds:def id=emit-zsknwfun owner=@khanakia stability=stable
func Emit(ctx context.Context, chunk []byte) error {
	b, ok := ctx.Value(streamKey{}).(*streamBinding)
	if !ok {
		return Unsupported("Emit", "no StreamSink is bound to this ctx: add the workflow.Stream plugin (or StreamMiddleware) and call Emit from a step")
	}
	return b.sink.Write(ctx, b.run, b.step, b.seq.Add(1), slices.Clone(chunk))
}

// StreamMiddleware returns step middleware that binds sink to every step
// attempt's ctx, for Emit. A ctx without StepInfo (a handler called
// outside a driver) and a nil sink pass through unchanged, so Emit there
// returns its UnsupportedError. Use it directly with RunInline
// (InlineMiddleware); on a client, the Stream plugin adds it.
func StreamMiddleware(sink StreamSink) Middleware {
	return func(next Handler) Handler {
		return func(ctx context.Context, in []byte) ([]byte, error) {
			info, ok := StepInfoFrom(ctx)
			if !ok || sink == nil {
				return next(ctx, in)
			}
			return next(context.WithValue(ctx, streamKey{}, &streamBinding{sink: sink, run: info.RunID, step: info.Step}), in)
		}
	}
}

// StreamOption configures the Stream plugin.
type StreamOption func(*streamConfig)

type streamConfig struct{ closeOnFinish bool }

// StreamCloseOnFinish makes the plugin close a run's stream when the run
// ends (StreamCloser.Close, when the sink implements it), so live readers
// stop at once and the store can drop the run. It adds a run-finished
// observer, so Use then needs a client with RunNotifier; without this
// option the plugin is step middleware only and works on every client,
// and MemoryStream drops runs by its TTL.
func StreamCloseOnFinish() StreamOption {
	return func(c *streamConfig) { c.closeOnFinish = true }
}

// Stream returns the plugin that lets steps call Emit: it binds sink to
// every step's ctx (StreamMiddleware). Pass it to Use before Register and
// Start. A nil sink adds nothing, so Emit keeps returning its
// UnsupportedError. With StreamCloseOnFinish it also closes each run's
// stream when the run ends.
func Stream(sink StreamSink, opts ...StreamOption) Plugin {
	var cfg streamConfig
	for _, o := range opts {
		if o != nil {
			o(&cfg)
		}
	}
	p := &streamPlugin{sink: sink}
	if cfg.closeOnFinish {
		return &streamClosePlugin{streamPlugin: p}
	}
	return p
}

type streamPlugin struct{ sink StreamSink }

// Name names the plugin in refusals.
func (p *streamPlugin) Name() string { return "stream" }

// StepMiddleware binds the sink to every step's ctx.
func (p *streamPlugin) StepMiddleware() []Middleware {
	if p.sink == nil {
		return nil
	}
	return []Middleware{StreamMiddleware(p.sink)}
}

// streamClosePlugin is the plugin with StreamCloseOnFinish: a separate
// type, so only this variant is a RunFinishedProvider and needs
// RunNotifier.
type streamClosePlugin struct{ *streamPlugin }

// RunFinishedObservers closes a finished run's stream. A Close error is
// dropped: an observer has no caller, and the store's TTL drops the run
// anyway.
func (p *streamClosePlugin) RunFinishedObservers() []RunFinishedFunc {
	c, ok := p.sink.(StreamCloser)
	if !ok {
		return nil
	}
	return []RunFinishedFunc{func(ctx context.Context, ev RunEvent) {
		if !ev.Ref.RunID.IsZero() {
			_ = c.Close(ctx, ev.Ref.RunID) // see the doc comment
		}
	}}
}

// The MemoryStream defaults.
const (
	//ds:def id=defaultstreammaxchunk-x32wk6hd owner=@khanakia stability=stable
	DefaultStreamMaxChunk = 64 << 10
	//ds:def id=defaultstreammaxrunbytes-5k2knh9n owner=@khanakia stability=stable
	DefaultStreamMaxRunBytes = 1 << 20
	//ds:def id=defaultstreammaxrunchunks-7nh94m5f owner=@khanakia stability=stable
	DefaultStreamMaxRunChunks = 4096
	//ds:def id=defaultstreammaxruns-e3k8y7xk owner=@khanakia stability=stable
	DefaultStreamMaxRuns = 1000
	//ds:def id=defaultstreamttl-vmnd7ey7 owner=@khanakia stability=stable
	DefaultStreamTTL = time.Hour
	//ds:def id=defaultstreamretain-z5dfhqra owner=@khanakia stability=stable
	DefaultStreamRetain = time.Minute
	//ds:def id=defaultstreambuffer-7xcbfcjg owner=@khanakia stability=stable
	DefaultStreamBuffer = 256
)

// MemoryStreamOptions bounds a MemoryStream. Zero fields take the
// defaults named on each.
//
//ds:def id=memorystreamoptions-5bawxsf7 owner=@khanakia stability=stable
type MemoryStreamOptions struct {
	// MaxChunk is the largest chunk Write accepts (DefaultStreamMaxChunk,
	// 64 KiB); a larger one is refused with ErrInvalid.
	MaxChunk int

	// MaxRunBytes and MaxRunChunks bound what one run keeps
	// (DefaultStreamMaxRunBytes, 1 MiB; DefaultStreamMaxRunChunks, 4096).
	// Past either, the oldest chunks are dropped: a live reader keeps
	// up, and a late one sees a gap in Pos.
	MaxRunBytes  int
	MaxRunChunks int

	// MaxRuns bounds how many runs are held at once
	// (DefaultStreamMaxRuns). When full, the closed run closed longest
	// ago is dropped for a new one; with no closed run, the new run's
	// first chunk is refused with ErrStreamFull.
	MaxRuns int

	// TTL drops a run no chunk was written to for this long, closed or
	// not (DefaultStreamTTL, one hour), so a run whose Close never came
	// (a crash, no StreamCloseOnFinish) does not stay forever.
	TTL time.Duration

	// Retain is how long a closed run's chunks stay readable after
	// Close (DefaultStreamRetain, one minute), so a reader that connects
	// just after the run ended still gets its output.
	Retain time.Duration

	// Buffer is each subscriber's channel size (DefaultStreamBuffer).
	// A subscriber that falls this far behind has its channel closed and
	// resynchronises with Read.
	Buffer int

	// Clock is read for TTL, Retain and StreamChunk.At; nil means
	// clock.Real.
	Clock clock.Clock
}

// MemoryStream is the in-process StreamSink and StreamReader: chunks live
// in this process's memory, bounded per run and in number of runs
// (MemoryStreamOptions), and are lost when it exits. It runs no
// goroutine: expired runs are dropped on the next call. Every method is
// safe for concurrent use. It serves one process (a worker that also
// answers the httprun routes); a fleet needs a shared store implementing
// the same interfaces.
//
//ds:def id=memorystream-z4prqky7 owner=@khanakia stability=stable
type MemoryStream struct {
	opt  MemoryStreamOptions
	mu   sync.Mutex
	runs map[RunID]*memRunStream
}

type memRunStream struct {
	chunks   []StreamChunk
	bytes    int
	next     int64 // the Pos of the next chunk
	lastAt   time.Time
	closedAt time.Time // zero while open
	subs     map[*memStreamSub]struct{}
}

type memStreamSub struct {
	ch   chan StreamChunk
	done chan struct{} // closed with ch, so the ctx watcher ends too
	once sync.Once
}

func (s *memStreamSub) close() {
	s.once.Do(func() {
		close(s.ch)
		close(s.done)
	})
}

var (
	_ StreamSink   = (*MemoryStream)(nil)
	_ StreamReader = (*MemoryStream)(nil)
	_ StreamCloser = (*MemoryStream)(nil)
)

// NewMemoryStream returns an empty MemoryStream with opt's bounds.
//
//ds:def id=newmemorystream-mk959v4h owner=@khanakia stability=stable
func NewMemoryStream(opt MemoryStreamOptions) *MemoryStream {
	if opt.MaxChunk <= 0 {
		opt.MaxChunk = DefaultStreamMaxChunk
	}
	if opt.MaxRunBytes <= 0 {
		opt.MaxRunBytes = DefaultStreamMaxRunBytes
	}
	if opt.MaxRunChunks <= 0 {
		opt.MaxRunChunks = DefaultStreamMaxRunChunks
	}
	if opt.MaxRuns <= 0 {
		opt.MaxRuns = DefaultStreamMaxRuns
	}
	if opt.TTL <= 0 {
		opt.TTL = DefaultStreamTTL
	}
	if opt.Retain <= 0 {
		opt.Retain = DefaultStreamRetain
	}
	if opt.Buffer <= 0 {
		opt.Buffer = DefaultStreamBuffer
	}
	if opt.Clock == nil {
		opt.Clock = clock.Real{}
	}
	return &MemoryStream{opt: opt, runs: map[RunID]*memRunStream{}}
}

// Write appends chunk to run's stream and hands it to every subscriber.
// It refuses an empty run ID or a chunk over MaxChunk (ErrInvalid), a run
// already closed (ErrWrongState), and a new run when the store is full
// (ErrStreamFull). A subscriber whose buffer is full is dropped (its
// channel closed), never waited for, so a slow reader cannot stall a step.
//
//ds:def id=memorystream-write-9us37kub owner=@khanakia stability=stable
func (m *MemoryStream) Write(ctx context.Context, run RunID, step string, seq int64, chunk []byte) error {
	if run.IsZero() {
		return invalidf("stream: empty run ID")
	}
	if len(chunk) > m.opt.MaxChunk {
		return invalidf("stream: chunk is %d bytes, more than %d", len(chunk), m.opt.MaxChunk)
	}
	attempt := 0
	if info, ok := StepInfoFrom(ctx); ok {
		attempt = info.Attempt
	}
	now := m.opt.Clock.Now()
	m.mu.Lock()
	defer m.mu.Unlock()
	m.prune(now)
	rs := m.runs[run]
	if rs == nil {
		if len(m.runs) >= m.opt.MaxRuns && !m.evictClosed() {
			return ErrStreamFull
		}
		rs = &memRunStream{next: 1, subs: map[*memStreamSub]struct{}{}}
		m.runs[run] = rs
	}
	if !rs.closedAt.IsZero() {
		return fmt.Errorf("%w: stream: run %s is closed", ErrWrongState, run)
	}
	c := StreamChunk{Pos: rs.next, Step: step, Attempt: attempt, Seq: seq, Data: slices.Clone(chunk), At: now}
	rs.next++
	rs.lastAt = now
	rs.chunks = append(rs.chunks, c)
	rs.bytes += len(c.Data)
	for len(rs.chunks) > 0 && (len(rs.chunks) > m.opt.MaxRunChunks || rs.bytes > m.opt.MaxRunBytes) {
		rs.bytes -= len(rs.chunks[0].Data)
		rs.chunks[0] = StreamChunk{}
		rs.chunks = rs.chunks[1:]
	}
	for sub := range rs.subs {
		select {
		case sub.ch <- cloneStreamChunk(c):
		default: // too far behind: drop it, it resynchronises with Read
			delete(rs.subs, sub)
			sub.close()
		}
	}
	return nil
}

// Read returns copies of run's chunks after the given Pos.
func (m *MemoryStream) Read(_ context.Context, run RunID, after int64) ([]StreamChunk, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.prune(m.opt.Clock.Now())
	rs := m.runs[run]
	if rs == nil {
		return nil, nil
	}
	var out []StreamChunk
	for _, c := range rs.chunks {
		if c.Pos > after {
			out = append(out, cloneStreamChunk(c))
		}
	}
	return out, nil
}

// Subscribe follows run's new chunks. On a run already closed the channel
// comes back closed, so the reader goes straight to Read.
func (m *MemoryStream) Subscribe(ctx context.Context, run RunID) (<-chan StreamChunk, func()) {
	sub := &memStreamSub{ch: make(chan StreamChunk, m.opt.Buffer), done: make(chan struct{})}
	m.mu.Lock()
	m.prune(m.opt.Clock.Now())
	rs := m.runs[run]
	switch {
	case rs != nil && !rs.closedAt.IsZero():
		sub.close()
	case rs == nil:
		// Nothing written yet: hold an empty, open stream so the first
		// chunk reaches this subscriber. It counts against MaxRuns only
		// when there is room, and the TTL drops it if nothing comes.
		if len(m.runs) >= m.opt.MaxRuns && !m.evictClosed() {
			sub.close()
			break
		}
		rs = &memRunStream{next: 1, lastAt: m.opt.Clock.Now(), subs: map[*memStreamSub]struct{}{}}
		m.runs[run] = rs
		rs.subs[sub] = struct{}{}
	default:
		rs.subs[sub] = struct{}{}
	}
	m.mu.Unlock()
	stop := func() {
		m.mu.Lock()
		if rs := m.runs[run]; rs != nil {
			delete(rs.subs, sub)
		}
		m.mu.Unlock()
		sub.close()
	}
	if ctx.Done() != nil {
		go func() {
			select {
			case <-ctx.Done():
				stop()
			case <-sub.done:
			}
		}()
	}
	return sub.ch, stop
}

// Close marks run's stream finished: subscribers' channels close, later
// writes are refused, and the chunks stay readable for Retain. Closing an
// unknown or closed run is not an error.
func (m *MemoryStream) Close(_ context.Context, run RunID) error {
	now := m.opt.Clock.Now()
	m.mu.Lock()
	defer m.mu.Unlock()
	rs := m.runs[run]
	if rs == nil || !rs.closedAt.IsZero() {
		return nil
	}
	rs.closedAt = now
	for sub := range rs.subs {
		sub.close()
	}
	rs.subs = nil
	return nil
}

// Runs returns the runs the store holds now, sorted, for tests and for
// checking that finished runs are dropped.
func (m *MemoryStream) Runs() []RunID {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.prune(m.opt.Clock.Now())
	out := make([]RunID, 0, len(m.runs))
	for id := range m.runs {
		out = append(out, id)
	}
	slices.Sort(out)
	return out
}

// prune drops runs past their TTL or Retain. The caller holds m.mu.
//
//ds:def id=memorystream-prune-bn39fqan owner=@khanakia stability=stable
func (m *MemoryStream) prune(now time.Time) {
	for id, rs := range m.runs {
		expired := now.Sub(rs.lastAt) >= m.opt.TTL
		retained := !rs.closedAt.IsZero() && now.Sub(rs.closedAt) >= m.opt.Retain
		if expired || retained {
			for sub := range rs.subs {
				sub.close()
			}
			delete(m.runs, id)
		}
	}
}

// evictClosed drops the run closed longest ago and reports whether there
// was one. The caller holds m.mu.
//
//ds:def id=memorystream-evictclosed-r393dw88 owner=@khanakia stability=stable
func (m *MemoryStream) evictClosed() bool {
	var oldest RunID
	var at time.Time
	for id, rs := range m.runs {
		if !rs.closedAt.IsZero() && (oldest.IsZero() || rs.closedAt.Before(at)) {
			oldest, at = id, rs.closedAt
		}
	}
	if oldest.IsZero() {
		return false
	}
	delete(m.runs, oldest)
	return true
}

func cloneStreamChunk(c StreamChunk) StreamChunk {
	c.Data = slices.Clone(c.Data)
	return c
}
