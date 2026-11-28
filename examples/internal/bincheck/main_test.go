package main

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// write puts content at name under dir and returns the path.
func write(t *testing.T, dir, name string, content []byte) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, content, 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

// peFile is the smallest file isPE accepts: a DOS header whose offset at
// 0x3c points at a "PE\0\0" signature.
func peFile() []byte {
	b := make([]byte, 0x84)
	copy(b, "MZ")
	binary.LittleEndian.PutUint32(b[peOffsetAt:], 0x80)
	copy(b[0x80:], "PE\x00\x00")
	return b
}

// TestRunReportsEveryExecutableFormat pins what the gate catches: each
// format Go builds is reported with its kind, and source, text, a text
// file that happens to start with "MZ", an empty file and a tracked file
// deleted from the working tree are not.
func TestRunReportsEveryExecutableFormat(t *testing.T) {
	dir := t.TempDir()
	bins := map[string][]byte{
		"linux":     append([]byte{0x7f, 'E', 'L', 'F', 2, 1, 1}, make([]byte, 100)...),
		"darwin":    append([]byte{0xcf, 0xfa, 0xed, 0xfe, 0x0c, 0, 0, 1}, make([]byte, 100)...),
		"universal": append([]byte{0xca, 0xfe, 0xba, 0xbe, 0, 0, 0, 2}, make([]byte, 100)...),
		"prog.exe":  peFile(),
		"prog.wasm": append([]byte{0, 'a', 's', 'm', 1, 0, 0, 0}, make([]byte, 8)...),
	}
	clean := map[string][]byte{
		"main.go":   []byte("package main\n\nfunc main() {}\n"),
		"README.md": []byte("# demo\n"),
		"mz.txt":    []byte("MZ is how this note starts, and it is plain text that runs past sixty-four bytes.\n"),
		"empty":     nil,
	}
	var paths []string
	for name, b := range bins {
		paths = append(paths, write(t, dir, name, b))
	}
	for name, b := range clean {
		paths = append(paths, write(t, dir, name, b))
	}
	paths = append(paths, filepath.Join(dir, "deleted"))

	var out bytes.Buffer
	n, err := run(&out, paths)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(bins) {
		t.Errorf("reported %d, want %d:\n%s", n, len(bins), out.String())
	}
	want := map[string]string{"linux": "ELF", "darwin": "Mach-O", "universal": "Mach-O universal", "prog.exe": "PE", "prog.wasm": "WebAssembly"}
	for name, kind := range want {
		line := filepath.Join(dir, name) + ": " + kind + " executable"
		if !strings.Contains(out.String(), line+"\n") {
			t.Errorf("missing %q in:\n%s", line, out.String())
		}
	}
	for name := range clean {
		if strings.Contains(out.String(), filepath.Join(dir, name)+":") {
			t.Errorf("%s reported as a program:\n%s", name, out.String())
		}
	}
}

// TestRunCatchesARealBuild checks a real Go build of this platform is
// reported: the running test binary, linked the way a stray `go build` in a
// main folder links its program.
func TestRunCatchesARealBuild(t *testing.T) {
	exe, err := os.Executable() // the test binary is a program of this platform
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	n, err := run(&out, []string{exe})
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("the test binary %s was not reported: %q", exe, out.String())
	}
}

// TestRunNeedsPaths keeps an empty file list from passing silently.
func TestRunNeedsPaths(t *testing.T) {
	if _, err := run(&bytes.Buffer{}, nil); err == nil {
		t.Error("no paths: want an error")
	}
}
