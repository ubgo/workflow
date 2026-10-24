package workflow

import (
	"context"
	"maps"
	"slices"
	"strings"
)

// Reserved label prefixes. The library's own packages write labels under
// these prefixes onto the jobs they pass down (unique keys, claim-check
// references, batch membership, trace context), so an app label under one
// of them would collide with, or be removed by, the package that owns it.
// The three spellings differ for historical reasons and are kept as they
// are: runs in flight carry them, and renaming one would break those runs.
const (
	// LabelPrefixInternal is owned by the middleware package (unique
	// locks, claim-check references).
	LabelPrefixInternal = "ubgo.workflow/"

	// LabelPrefixBatch is owned by the batch package (batch ID and job
	// index).
	LabelPrefixBatch = "workflow.batch."

	// LabelPrefixOTel is owned by the otel package (trace context carried
	// from Enqueue to the step).
	LabelPrefixOTel = "otel."
)

// reservedLabelPrefixes is the one list every check reads. It is never
// handed out: ReservedLabelPrefixes returns a copy, so no caller can
// change what another sees.
var reservedLabelPrefixes = [...]string{LabelPrefixInternal, LabelPrefixBatch, LabelPrefixOTel}

// ReservedLabelPrefixes returns every label prefix the library reserves
// for itself, as a fresh slice.
func ReservedLabelPrefixes() []string { return slices.Clone(reservedLabelPrefixes[:]) }

// ReservedLabel reports whether key falls under a reserved prefix, and
// which.
func ReservedLabel(key string) (prefix string, ok bool) {
	for _, p := range reservedLabelPrefixes {
		if strings.HasPrefix(key, p) {
			return p, true
		}
	}
	return "", false
}

// ValidateLabels refuses app labels under a reserved prefix with an error
// wrapping ErrInvalid that names the key and the prefix, instead of
// letting the owning package drop or overwrite them silently. Keys are
// checked in sorted order, so the error is the same on every call.
//
// It is for labels an app wrote. The library's own packages set reserved
// labels on the jobs they pass down, so a driver's Enqueue calls
// CheckJobLabels instead, which lets those through; ValidateSchedule does (schedules are written only by apps), and each
// package that owns a prefix refuses incoming labels under its own.
func ValidateLabels(labels map[string]string) error {
	for _, k := range slices.Sorted(maps.Keys(labels)) {
		if p, ok := ReservedLabel(k); ok {
			return invalidf("label %q uses the reserved prefix %q", k, p)
		}
	}
	return nil
}

// reservedAllow is what a ctx lets an Enqueue carry under the reserved
// prefixes: exact keys and whole prefixes, stamped by the package that
// owns them.
type reservedAllow struct {
	keys     map[string]bool
	prefixes []string
}

type reservedAllowKey struct{}

func allowFrom(ctx context.Context) reservedAllow {
	a, _ := ctx.Value(reservedAllowKey{}).(reservedAllow)
	return a
}

// AllowReservedLabels returns a ctx under which an Enqueue may carry the
// given reserved label keys, adding to what ctx already allows. The
// package that owns a key calls it around the Enqueue that carries the
// label it stamped (Spawn for its parent labels, middleware.Unique for
// its lock labels), so a label it wrote passes CheckJobLabels while the
// same key typed by an app, or copied in from a request, is refused.
//
// It is a guard against labels arriving by accident or from untrusted
// input, not a sandbox: code in the same process that calls it can set
// any label it names, as it could call any other function. Only the
// owning package should.
func AllowReservedLabels(ctx context.Context, keys ...string) context.Context {
	old := allowFrom(ctx)
	a := reservedAllow{keys: make(map[string]bool, len(old.keys)+len(keys)), prefixes: old.prefixes}
	for k := range old.keys {
		a.keys[k] = true
	}
	for _, k := range keys {
		a.keys[k] = true
	}
	return context.WithValue(ctx, reservedAllowKey{}, a)
}

// AllowReservedPrefix is AllowReservedLabels for every key under prefix,
// for the package that owns a whole prefix (batch for LabelPrefixBatch,
// otel for LabelPrefixOTel).
func AllowReservedPrefix(ctx context.Context, prefix string) context.Context {
	old := allowFrom(ctx)
	a := reservedAllow{keys: old.keys, prefixes: append(slices.Clone(old.prefixes), prefix)}
	return context.WithValue(ctx, reservedAllowKey{}, a)
}

// CheckJobLabels refuses, with an error wrapping ErrInvalid that names the
// key, a label of job (or of a job in its Chain) under a reserved prefix
// that ctx does not allow (AllowReservedLabels, AllowReservedPrefix).
// Every driver calls it first in its public Enqueue (and any other public
// entry that takes a Job from an app), never on its internal paths (a
// fired schedule, a chained job, a retry), which carry labels the library
// wrote. Keys are checked in sorted order, so the error is stable.
func CheckJobLabels(ctx context.Context, job Job) error {
	a := allowFrom(ctx)
	for _, k := range slices.Sorted(maps.Keys(job.Labels)) {
		p, ok := ReservedLabel(k)
		if !ok || a.keys[k] {
			continue
		}
		if slices.ContainsFunc(a.prefixes, func(ap string) bool { return strings.HasPrefix(k, ap) }) {
			continue
		}
		return invalidf("job for %q: label %q uses the reserved prefix %q; only the package owning it may set it", job.Workflow, k, p)
	}
	for _, next := range job.Chain {
		if err := CheckJobLabels(ctx, next); err != nil {
			return err
		}
	}
	return nil
}
