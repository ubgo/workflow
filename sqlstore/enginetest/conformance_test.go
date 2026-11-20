package enginetest

import (
	"slices"
	"testing"

	"github.com/ubgo/workflow/workflowtest"
)

// TestConformance runs the shared driver suite (conformanceHarness) on
// every shared server this run has: Postgres with WFTEST_PG_DSN, MySQL with
// WFTEST_MYSQL_DSN, one after the other as always. The SQLite run of the
// same suite lives in the shard packages (shards/conformance*), whose
// TestConformance/sqlite subtests carry the names it had here.
func TestConformance(t *testing.T) {
	for _, e := range engines(t) {
		if e.name == "sqlite" {
			continue // shards/conformance*
		}
		t.Run(e.name, func(t *testing.T) {
			workflowtest.Run(t, conformanceHarness(e))
		})
	}
}

// TestConformanceSharesNameRealGroups: every group a share table names is
// a group of the suite, and no group is named twice, so the shares split
// the suite exactly (the last share takes every group left over).
func TestConformanceSharesNameRealGroups(t *testing.T) {
	all := workflowtest.GroupNames()
	for name, table := range map[string][][]string{"conformanceShares": conformanceShares, "oneConnectionShares": oneConnectionShares} {
		seen := map[string]bool{}
		for i, share := range table {
			for _, g := range share {
				if !slices.Contains(all, g) {
					t.Errorf("%s[%d] names %q, which is not a group of the suite (%v)", name, i, g, all)
				}
				if seen[g] {
					t.Errorf("%s names %q twice", name, g)
				}
				seen[g] = true
			}
		}
	}
}
