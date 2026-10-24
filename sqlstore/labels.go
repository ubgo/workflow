package sqlstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"unicode/utf8"
)

// labelIndexWidth is how many characters of a label key or value the
// run_labels index keeps: the width of a name column (VARCHAR(191)), the
// widest that fits MySQL's utf8mb4 index limit beside the other key
// columns. A longer key or value is indexed by its first labelIndexWidth
// characters, so the index narrows a label filter to candidates and
// RunQuery.Match then compares the whole stored labels exactly.
const labelIndexWidth = 191

// labelsLockName is the locks row whose n is the label backfill's
// watermark: every run with seq <= n has its run_labels rows. Runs this
// release writes index themselves at enqueue; the watermark is for runs
// written before migration 005, or by an older client still running
// during a rolling deploy.
const labelsLockName = "labels:indexed"

// labelBackfillChunk is how many runs one backfill transaction indexes, so
// a large table is indexed in short transactions that never hold the
// watermark row for long.
const labelBackfillChunk = 500

// labelIndexText is s as the index stores it: invalid UTF-8 bytes and NUL
// each become U+FFFD (as encoding/json writes the stored labels, and NUL
// because Postgres text refuses it), cut to labelIndexWidth characters.
// The index and the query both go through it, so a run whose labels match
// a filter exactly is always among the index's candidates.
func labelIndexText(s string) string {
	var b strings.Builder
	n := 0
	for _, r := range s { // an invalid byte ranges as utf8.RuneError, width 1
		if n == labelIndexWidth {
			break
		}
		if r == 0 {
			r = utf8.RuneError
		}
		b.WriteRune(r)
		n++
	}
	return b.String()
}

// labelPairs is labels as the distinct (key, value) index rows, sorted so
// a run's rows are written in a stable order. Two long keys that share
// their first labelIndexWidth characters can produce the same pair; it is
// written once.
func labelPairs(labels map[string]string) [][2]string {
	seen := map[[2]string]bool{}
	out := make([][2]string, 0, len(labels))
	for k, v := range labels {
		p := [2]string{labelIndexText(k), labelIndexText(v)}
		if !seen[p] {
			seen[p] = true
			out = append(out, p)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i][0] != out[j][0] {
			return out[i][0] < out[j][0]
		}
		return out[i][1] < out[j][1]
	})
	return out
}

// insertLabels writes the run_labels rows of run id in tx.
func (c *Client) insertLabels(ctx context.Context, tx *sql.Tx, id string, labels map[string]string) error {
	for _, p := range labelPairs(labels) {
		if _, err := c.st.exec(ctx, tx, "INSERT INTO {run_labels} (run_id, lkey, lval) VALUES (?, ?, ?)", id, p[0], p[1]); err != nil {
			return fmt.Errorf("sqlstore: index the labels of run %s: %w", id, err)
		}
	}
	return nil
}

// labelFilter is the SQL half of a label filter over runs r: one indexed
// subquery per exact pair (RunQuery.Labels) and per key prefix
// (RunQuery.LabelPrefixes), so the engine reads the (lkey, lval) index
// instead of decoding every run's labels. It narrows to candidates only;
// Runs checks the exact labels in Go (RunQuery.Match).
//
// A prefix is a LIKE pattern with its wildcards escaped, not a range
// (lval >= p AND lval < p+max): a range is prefix-correct only under a
// byte-order collation, and a Postgres database may use a linguistic one.
// LIKE may match more than the prefix (SQLite's is case-insensitive for
// ASCII), never less, and Match drops the extras. The key equality still
// seeks the index. A prefix cut to labelIndexWidth stays a prefix of the
// cut value, so long prefixes narrow correctly too.
func labelFilter(labels, prefixes map[string]string) (string, []any) {
	var where string
	var args []any
	for _, p := range labelPairs(labels) {
		where += " AND r.id IN (SELECT lb.run_id FROM {run_labels} lb WHERE lb.lkey = ? AND lb.lval = ?)"
		args = append(args, p[0], p[1])
	}
	for _, p := range labelPairs(prefixes) {
		if p[1] == "" {
			where += " AND r.id IN (SELECT lb.run_id FROM {run_labels} lb WHERE lb.lkey = ?)"
			args = append(args, p[0])
			continue
		}
		where += " AND r.id IN (SELECT lb.run_id FROM {run_labels} lb WHERE lb.lkey = ? AND lb.lval LIKE ? ESCAPE '" + likeEscape + "')"
		args = append(args, p[0], likePrefix(p[1]))
	}
	return where, args
}

// likeEscape is the ESCAPE character of labelFilter's LIKE patterns. Not a
// backslash, whose meaning inside a string literal differs by engine and
// setting (MySQL's NO_BACKSLASH_ESCAPES).
const likeEscape = "!"

// likePrefix is the LIKE pattern that matches every value starting with p:
// p with %, _ and the escape character escaped, then %.
func likePrefix(p string) string {
	var b strings.Builder
	for _, r := range p {
		if r == '%' || r == '_' || string(r) == likeEscape {
			b.WriteString(likeEscape)
		}
		b.WriteRune(r)
	}
	b.WriteByte('%')
	return b.String()
}

// IndexLabels writes the label index rows (migration 005's run_labels) of
// every run past the backfill watermark (rewriting rows a run already has), in transactions
// of a few hundred runs, and returns how many runs it indexed. Migrate
// calls it after applying the migrations, so most apps never call it.
// Call it after applying MigrationScripts with another tool, and once more
// after a rolling deploy in which a client older than 005 kept enqueueing:
// until then a label filter in Runs or CancelAll does not see those runs.
//
// It is safe to run twice and from several processes at once: each chunk
// first takes the watermark row's lock, re-reads the watermark, rewrites
// the rows of the runs past it and moves it on, so a second caller waits
// and then finds the work done.
func (c *Client) IndexLabels(ctx context.Context) (int, error) {
	if err := c.ensureLock(ctx, labelsLockName); err != nil {
		return 0, fmt.Errorf("sqlstore: index labels: %w", err)
	}
	total := 0
	for {
		var n int
		var more bool
		err := c.st.tx(ctx, func(tx *sql.Tx) error {
			var err error
			n, more, err = c.indexLabelChunk(ctx, tx)
			return err
		})
		if err != nil {
			return total, fmt.Errorf("sqlstore: index labels: %w", err)
		}
		total += n
		if !more {
			return total, nil
		}
	}
}

// labelRun is one run the backfill reads.
type labelRun struct {
	seq    int64
	id     string
	labels string
}

// indexLabelChunk indexes the next labelBackfillChunk runs past the
// watermark in tx and moves the watermark past them. It reports how many
// of them had labels and whether more runs may follow.
func (c *Client) indexLabelChunk(ctx context.Context, tx *sql.Tx) (int, bool, error) {
	// A write first, so the transaction holds the watermark row (and
	// SQLite's write lock) before it reads: a concurrent backfill waits here
	// rather than indexing the same runs.
	if _, err := c.st.exec(ctx, tx, "UPDATE {locks} SET n = n WHERE name = ?", labelsLockName); err != nil {
		return 0, false, err
	}
	var mark int64
	if err := c.st.one(ctx, tx, "SELECT n FROM {locks} WHERE name = ?", []any{labelsLockName}, &mark); err != nil {
		return 0, false, err
	}
	var runs []labelRun
	var r labelRun
	err := c.st.each(ctx, tx, "SELECT seq, id, labels FROM {runs} WHERE seq > ? ORDER BY seq LIMIT ?", []any{mark, labelBackfillChunk},
		func() error { runs = append(runs, r); return nil }, &r.seq, &r.id, &r.labels)
	if err != nil {
		return 0, false, err
	}
	if len(runs) == 0 {
		return 0, false, nil
	}
	indexed := 0
	for _, run := range runs {
		var labels map[string]string
		if err := json.Unmarshal([]byte(run.labels), &labels); err != nil {
			// The same stored labels that labelMap reports; the run keeps no
			// index rows, as it has no labels a filter could match.
			c.log.Error("sqlstore: label backfill reads a run without labels", "run", run.id, "error", err)
			continue
		}
		if len(labels) == 0 {
			continue
		}
		// Rewrite rather than insert, so a run a newer client already
		// indexed at enqueue is left with the same rows.
		if _, err := c.st.exec(ctx, tx, "DELETE FROM {run_labels} WHERE run_id = ?", run.id); err != nil {
			return 0, false, err
		}
		if err := c.insertLabels(ctx, tx, run.id, labels); err != nil {
			return 0, false, err
		}
		indexed++
	}
	last := runs[len(runs)-1].seq
	if _, err := c.st.exec(ctx, tx, "UPDATE {locks} SET n = ? WHERE name = ?", last, labelsLockName); err != nil {
		return 0, false, err
	}
	return indexed, len(runs) == labelBackfillChunk, nil
}
