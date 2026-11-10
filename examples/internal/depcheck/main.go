// Command depcheck fails when a released module would make an app download
// a module it does not import: its go.mod must be tidy on its own (no
// go.work), and every module it requires, direct or indirect, must provide
// a package its library code builds with on at least one of the platforms
// in goosList. A require that only tests use belongs in a never-released
// test module beside it (see sqlstore/enginetest).
//
// Usage: depcheck <module dir>...
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"sort"
	"strings"
)

// goosList are the platforms a library package is built for; a module only
// one of them needs (golang.org/x/sys on Windows) still counts as imported.
var goosList = []string{"linux", "darwin", "windows"}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: depcheck <module dir>...")
		os.Exit(2)
	}
	bad := 0
	for _, dir := range os.Args[1:] {
		problems, err := check(dir)
		if err != nil {
			fmt.Fprintf(os.Stderr, "depcheck: %s: %v\n", dir, err)
			os.Exit(2)
		}
		for _, p := range problems {
			fmt.Printf("%s: %s\n", dir, p)
		}
		bad += len(problems)
	}
	if bad > 0 {
		fmt.Fprintf(os.Stderr, "depcheck: %d problem(s); move what only tests need into a never-released test module\n", bad)
		os.Exit(1)
	}
}

// check returns one line per problem in the module at dir.
func check(dir string) ([]string, error) {
	var problems []string
	if diff, err := goCmd(dir, "", "mod", "tidy", "-diff"); err != nil {
		if diff == "" {
			return nil, err
		}
		problems = append(problems, "go.mod is not tidy on its own (GOWORK=off go mod tidy -diff):\n"+diff)
	}
	requires, err := requiresOf(dir)
	if err != nil {
		return nil, err
	}
	used := map[string]bool{}
	for _, goos := range goosList {
		out, err := goCmd(dir, goos, "list", "-deps", "-f", "{{with .Module}}{{.Path}}{{end}}", "./...")
		if err != nil {
			return nil, fmt.Errorf("go list -deps (GOOS=%s): %w\n%s", goos, err, out)
		}
		for _, m := range strings.Fields(out) {
			used[m] = true
		}
	}
	for _, r := range requires {
		if !used[r] {
			problems = append(problems, "requires "+r+", which no library package imports")
		}
	}
	return problems, nil
}

// requiresOf lists every module dir's go.mod requires.
func requiresOf(dir string) ([]string, error) {
	out, err := goCmd(dir, "", "mod", "edit", "-json")
	if err != nil {
		return nil, err
	}
	var mod struct{ Require []struct{ Path string } }
	if err := json.Unmarshal([]byte(out), &mod); err != nil {
		return nil, err
	}
	paths := make([]string, 0, len(mod.Require))
	for _, r := range mod.Require {
		paths = append(paths, r.Path)
	}
	sort.Strings(paths)
	return paths, nil
}

// goCmd runs the go command in dir with no workspace, so each module is
// judged by its own go.mod as a consumer sees it, and returns its output.
func goCmd(dir, goos string, args ...string) (string, error) {
	cmd := exec.Command("go", args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GOWORK=off")
	if goos != "" {
		cmd.Env = append(cmd.Env, "GOOS="+goos)
	}
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	err := cmd.Run()
	var exit *exec.ExitError
	if errors.As(err, &exit) {
		return out.String(), fmt.Errorf("go %s: %w", strings.Join(args, " "), err)
	}
	return out.String(), err
}
