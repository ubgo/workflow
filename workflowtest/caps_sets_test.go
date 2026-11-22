package workflowtest

import (
	"testing"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/memory"
)

// TestFakeDefaultCapsFollowMemory: a default Fake claims exactly what the
// memory driver claims minus fakeUnserved, so a capability added to
// memory fails here until it is put in one of the two lists, and a test
// on a default Fake never refuses a field memory accepts.
func TestFakeDefaultCapsFollowMemory(t *testing.T) {
	mem := memory.New().Capabilities()
	if want := mem.Without(fakeUnserved); fakeDefaultCaps != want {
		t.Errorf("fakeDefaultCaps lacks %v and has %v beyond memory's set minus fakeUnserved",
			want.Without(fakeDefaultCaps), fakeDefaultCaps.Without(want))
	}
	if both := fakeDefaultCaps.Intersect(fakeUnserved); !both.IsZero() {
		t.Errorf("fakeDefaultCaps and fakeUnserved both hold %v", both)
	}
	if got := NewFake().Capabilities(); got != fakeDefaultCaps {
		t.Errorf("NewFake().Capabilities() = %v, want fakeDefaultCaps %v", got, fakeDefaultCaps)
	}
}

// TestRecorderFakeClaimsEveryCapButRunFinished: the Recorder's fake mode
// claims every capability the core defines except CapRunFinished, the
// ones added after it was written included.
func TestRecorderFakeClaimsEveryCapButRunFinished(t *testing.T) {
	want := workflow.AllCaps().Without(workflow.CapRunFinished)
	if got := NewRecorder(nil).Capabilities(); got != want {
		t.Errorf("NewRecorder(nil).Capabilities() lacks %v and has %v", want.Without(got), got.Without(want))
	}
}
