package workflow

// Shared concurrency scopes (ConcurrencyLimit.Scope, CapConcurrencyScope).

// CheckConcurrencyScopes reports whether wf may be registered beside
// others, the workflows a client already has: every limit of wf that names
// a Scope must declare the same MaxRuns and Hold as the limit of each other
// workflow naming that scope. A scope is one set of buckets, so its cap and
// how long a run keeps its slot are properties of the shared resource, not
// of the workflow that asks: with different caps, whether a run may start
// would depend on which workflow's runs happen to hold the bucket. The
// error wraps ErrInvalid and names both workflows, the scope and the
// field. A workflow of others with wf's name is skipped (registering it
// again replaces it). Drivers with CapConcurrencyScope call it in Register.
//
//ds:def id=checkconcurrencyscopes-k6juwpv8 owner=@khanakia stability=stable
func CheckConcurrencyScopes(wf Workflow, others []Workflow) error {
	for i, l := range wf.ConcurrencyLimits {
		if l.Scope == "" {
			continue
		}
		for _, o := range others {
			if o.Name == wf.Name {
				continue
			}
			for j, ol := range o.ConcurrencyLimits {
				if ol.Scope != l.Scope {
					continue
				}
				if ol.MaxRuns != l.MaxRuns {
					return invalidf("workflow %q: ConcurrencyLimits[%d] names Scope %q with MaxRuns %d, but workflow %q ConcurrencyLimits[%d] names it with MaxRuns %d: workflows sharing a scope must declare the same MaxRuns",
						wf.Name, i, l.Scope, l.MaxRuns, o.Name, j, ol.MaxRuns)
				}
				if holdName(ol.Hold) != holdName(l.Hold) {
					return invalidf("workflow %q: ConcurrencyLimits[%d] names Scope %q with Hold %q, but workflow %q ConcurrencyLimits[%d] names it with Hold %q: workflows sharing a scope must declare the same Hold",
						wf.Name, i, l.Scope, holdName(l.Hold), o.Name, j, holdName(ol.Hold))
				}
			}
		}
	}
	return nil
}

// ConcurrencyScoped reports whether any limit of wf names a Scope.
//
//ds:def id=concurrencyscoped-n9a3n3bc owner=@khanakia stability=stable
func ConcurrencyScoped(wf Workflow) bool {
	for _, l := range wf.ConcurrencyLimits {
		if l.Scope != "" {
			return true
		}
	}
	return false
}

// holdName is h with the zero value spelled out, so "" and HoldWholeRun
// compare equal and an error names the policy.
func holdName(h ConcurrencyHold) ConcurrencyHold {
	if h == "" {
		return HoldWholeRun
	}
	return h
}
