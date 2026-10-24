package cli

import (
	"fmt"
	"io"
	"slices"
	"strings"
	"text/tabwriter"
)

// table writes aligned columns: a header row (none for a key/value
// list), then rows.
type table struct{ tw *tabwriter.Writer }

func newTable(w io.Writer, header ...string) *table {
	t := &table{tw: tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)}
	if len(header) > 0 {
		t.row(header...)
	}
	return t
}

// row writes one row; a tab or newline inside a cell is replaced by a
// space so it cannot break the columns.
func (t *table) row(cells ...string) {
	for i, c := range cells {
		cells[i] = strings.NewReplacer("\t", " ", "\n", " ", "\r", " ").Replace(c)
	}
	fmt.Fprintln(t.tw, strings.Join(cells, "\t"))
}

func (t *table) flush() error { return t.tw.Flush() }

// sortedKeys is m's keys in order, for stable label output.
func sortedKeys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}
