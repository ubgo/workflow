package sqlstore

import (
	"encoding/json"
	"strings"
	"testing"
	"unicode/utf8"
)

// TestLabelIndexText: the index form of a label cuts at labelIndexWidth
// characters (not bytes), turns NUL into U+FFFD and agrees with the labels
// encoding/json stores, so a filter value and the stored labels of a run
// it matches always index to the same text.
func TestLabelIndexText(t *testing.T) {
	wide := strings.Repeat("é", labelIndexWidth+10)
	if got := labelIndexText(wide); utf8.RuneCountInString(got) != labelIndexWidth || !strings.HasPrefix(wide, got) {
		t.Fatalf("a long value indexes to %d characters, want the first %d", utf8.RuneCountInString(got), labelIndexWidth)
	}
	if got := labelIndexText("a\x00b"); got != "a�b" {
		t.Fatalf("NUL indexes to %q, want U+FFFD", got)
	}
	for _, v := range []string{"plain", "bad\xff\xfebytes", "nul\x00", wide} {
		raw, err := json.Marshal(map[string]string{"k": v})
		if err != nil {
			t.Fatal(err)
		}
		var back map[string]string
		if err := json.Unmarshal(raw, &back); err != nil {
			t.Fatal(err)
		}
		if labelIndexText(back["k"]) != labelIndexText(v) {
			t.Fatalf("%q: the stored form indexes to %q, the given form to %q", v, labelIndexText(back["k"]), labelIndexText(v))
		}
	}
	long := strings.Repeat("k", labelIndexWidth)
	if got := labelPairs(map[string]string{long + "1": "v", long + "2": "v"}); len(got) != 1 {
		t.Fatalf("two keys sharing the indexed prefix with one value give %d rows, want 1", len(got))
	}
}

// TestLikePrefix: LIKE wildcards and the escape character in a prefix are
// escaped, so they match only themselves.
func TestLikePrefix(t *testing.T) {
	for in, want := range map[string]string{"abc": "abc%", "a_b%c": "a!_b!%c%", "x!y": "x!!y%", "": "%"} {
		if got := likePrefix(in); got != want {
			t.Errorf("likePrefix(%q) = %q, want %q", in, got, want)
		}
	}
}
