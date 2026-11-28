// Command bincheck reports files that are compiled programs: ELF, Mach-O,
// Windows PE and WebAssembly binaries, by their first bytes.
//
// Why it exists: `go build ./...` inside a folder with a main package writes
// a binary named after the folder, next to the source. .gitignore lists
// every such folder, but a new one is easy to miss, and `git add .` then
// commits megabytes of machine code that no review reads and that stays in
// the history for good. A gate check catches it before it is proposed.
//
// Usage: go run ./internal/bincheck FILE...
// `task bin:check` passes every file git tracks or would track
// (git ls-files --cached --others --exclude-standard), so ignored build
// output is skipped and an unignored one is caught before it is committed.
// It exits 1 and prints the path and kind of every compiled program.
package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
)

// headerLen is how much of a file the check reads: enough for the DOS
// header of a PE file, whose offset to the PE signature sits at peOffsetAt.
const headerLen = 64

// peOffsetAt is where a DOS header keeps the offset of the "PE\0\0"
// signature.
const peOffsetAt = 0x3c

// magic is one compiled-program signature at the start of a file.
type magic struct {
	kind   string
	prefix []byte
}

// magics are the executable formats Go can build, by their first bytes.
// Mach-O is listed in both byte orders and as a universal (fat) binary.
var magics = []magic{
	{"ELF", []byte{0x7f, 'E', 'L', 'F'}},
	{"Mach-O", []byte{0xfe, 0xed, 0xfa, 0xce}},
	{"Mach-O", []byte{0xfe, 0xed, 0xfa, 0xcf}},
	{"Mach-O", []byte{0xce, 0xfa, 0xed, 0xfe}},
	{"Mach-O", []byte{0xcf, 0xfa, 0xed, 0xfe}},
	{"Mach-O universal", []byte{0xca, 0xfe, 0xba, 0xbe}},
	{"WebAssembly", []byte{0x00, 'a', 's', 'm'}},
}

func main() {
	n, err := run(os.Stdout, os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, "bincheck:", err)
		os.Exit(1)
	}
	if n > 0 {
		fmt.Fprintf(os.Stderr, "bincheck: %d compiled program(s); delete them and list their folder in .gitignore\n", n)
		os.Exit(1)
	}
}

// run checks every given file and returns how many compiled programs it
// reported. A path that is gone (deleted in the working tree but still
// tracked) or is not a regular file (a symlink, a submodule) is skipped.
func run(w io.Writer, paths []string) (int, error) {
	if len(paths) == 0 {
		return 0, errors.New("no paths given")
	}
	found := 0
	for _, p := range paths {
		kind, err := kindOf(p)
		if err != nil {
			return found, err
		}
		if kind != "" {
			fmt.Fprintf(w, "%s: %s executable\n", p, kind)
			found++
		}
	}
	return found, nil
}

// kindOf names the executable format of the file at p, or returns "" when
// it is not a compiled program.
func kindOf(p string) (string, error) {
	fi, err := os.Lstat(p)
	if errors.Is(err, fs.ErrNotExist) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	if !fi.Mode().IsRegular() {
		return "", nil
	}
	f, err := os.Open(p)
	if err != nil {
		return "", err
	}
	defer f.Close()
	head := make([]byte, headerLen)
	n, err := io.ReadFull(f, head)
	if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, io.EOF) {
		return "", err
	}
	head = head[:n]
	for _, m := range magics {
		if bytes.HasPrefix(head, m.prefix) {
			return m.kind, nil
		}
	}
	if isPE(f, head) {
		return "PE", nil
	}
	return "", nil
}

// isPE reports whether a file that starts with head is a Windows PE
// program. "MZ" alone is too weak a sign for a text file, so the DOS
// header's offset must lead to the "PE\0\0" signature.
func isPE(f io.ReaderAt, head []byte) bool {
	if len(head) < headerLen || head[0] != 'M' || head[1] != 'Z' {
		return false
	}
	off := int64(binary.LittleEndian.Uint32(head[peOffsetAt:]))
	sig := make([]byte, 4)
	if _, err := f.ReadAt(sig, off); err != nil {
		return false
	}
	return bytes.Equal(sig, []byte{'P', 'E', 0, 0})
}
