package cli

import (
	"fmt"
	"io"
)

// stickyWriter wraps the command output and keeps the first write error.
// Run checks it once the command ends, so a broken pipe or a full disk
// turns a command that otherwise succeeded into ExitError instead of a
// silent partial output. After the first failure every write fails fast.
type stickyWriter struct {
	w   io.Writer
	err error
}

// Write implements io.Writer, recording the first error.
func (s *stickyWriter) Write(p []byte) (int, error) {
	if s.err != nil {
		return 0, s.err
	}
	n, err := s.w.Write(p)
	if err != nil {
		s.err = err
	}
	return n, err
}

// fprintf, fprintln and fprint print to an output that Run wrapped in a
// stickyWriter (or to stderr, where a failed write has nowhere left to be
// reported). The stickyWriter keeps a write error and Run reports it once,
// so the call sites do not check each print.
func fprintf(w io.Writer, format string, a ...any) { _, _ = fmt.Fprintf(w, format, a...) }

// fprintln is fmt.Fprintln for stickyWriter outputs; see fprintf.
func fprintln(w io.Writer, a ...any) { _, _ = fmt.Fprintln(w, a...) }

// fprint is fmt.Fprint for stickyWriter outputs; see fprintf.
func fprint(w io.Writer, a ...any) { _, _ = fmt.Fprint(w, a...) }
