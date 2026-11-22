package workflow

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/bits"
	"strconv"
	"strings"
)

// capWords is how many 64-bit words a Caps holds. Widening it is an
// internal change: callers never see the words, only the methods, so a
// later release can make room again without breaking anyone.
const capWords = 4

// capBits is the number of capability numbers a Caps can hold: 0 to
// capBits-1.
//
//ds:def id=capbits-tqwzzyzd owner=@khanakia stability=stable
const capBits = capWords * 64

// Caps is a set of capabilities a driver really has. Drivers declare it
// through Client.Capabilities; Start compares it with Requires for every
// registered workflow and refuses what cannot be honoured, so a missing
// feature is an error at start-up rather than a silent no-op at run time.
//
// A Caps has room for 256 capabilities. It is a value: the zero Caps is
// the empty set, two sets compare with == and != (so
// ue.Missing == CapRunQuery works), and every method returns a new set.
// Combine capabilities with CapsOf or With (where an integer set would use
// |), drop them with Without (&^), test with Has, HasAny and IsZero, and
// intersect with Intersect (&).
//
// Its text form (String, MarshalText) is the capability names joined by
// "|", such as "Durable|Cron", and "none" for the empty set; JSON uses the
// same form as a string, and also reads the numeric form of releases where
// Caps was a uint64 (see UnmarshalJSON).
//
//ds:def id=caps-ssth52ga owner=@khanakia stability=api
type Caps struct {
	w [capWords]uint64
}

// capNames maps a capability number to its name; "" is a free number.
var capNames [capBits]string

// capClashes records every defineCap call that reused a taken number, so
// TestCapsDistinct can name both capabilities instead of the second one
// silently aliasing the first.
var capClashes []string

// defineCap declares capability number n, named name (the identifier
// without its "Cap" prefix), and returns the one-capability set. Only the
// var block in caps.go calls it.
func defineCap(n int, name string) Caps {
	if n < 0 || n >= capBits {
		panic(fmt.Sprintf("workflow: capability %s has number %d, outside 0..%d", name, n, capBits-1))
	}
	if prev := capNames[n]; prev != "" {
		capClashes = append(capClashes, fmt.Sprintf("number %d: %s and %s", n, prev, name))
	} else {
		capNames[n] = name
	}
	return capBit(n)
}

// capBit is the set holding only number n.
func capBit(n int) Caps {
	var c Caps
	c.w[n/64] = 1 << (n % 64)
	return c
}

// CapsOf returns the set holding every capability in cs (their union).
// CapsOf() is the empty set.
//
//ds:def id=capsof-mmp8bkmj owner=@khanakia stability=stable
func CapsOf(cs ...Caps) Caps {
	var c Caps
	return c.With(cs...)
}

// With returns c plus every capability in more.
//
//ds:def id=caps-with-jwfj42qg owner=@khanakia stability=stable
func (c Caps) With(more ...Caps) Caps {
	for _, m := range more {
		for i := range c.w {
			c.w[i] |= m.w[i]
		}
	}
	return c
}

// Without returns c minus every capability in drop.
//
//ds:def id=caps-without-bkp9zq5y owner=@khanakia stability=stable
func (c Caps) Without(drop ...Caps) Caps {
	for _, d := range drop {
		for i := range c.w {
			c.w[i] &^= d.w[i]
		}
	}
	return c
}

// Intersect returns the capabilities in both c and o.
//
//ds:def id=caps-intersect-5vawyy93 owner=@khanakia stability=stable
func (c Caps) Intersect(o Caps) Caps {
	for i := range c.w {
		c.w[i] &= o.w[i]
	}
	return c
}

// AllCaps returns every capability this release declares, such as a
// test double that should claim everything (FakeCaps(AllCaps())). It is
// the set of the Cap variables, so it grows when a release adds one.
//
//ds:def id=allcaps-umeq9egq owner=@khanakia stability=stable
func AllCaps() Caps {
	var c Caps
	for n, name := range capNames {
		if name != "" {
			c.w[n/64] |= 1 << (n % 64)
		}
	}
	return c
}

// Has reports whether c holds every capability in want. Every set has
// the empty set.
//
//ds:def id=caps-has-z5rv5wur owner=@khanakia stability=stable
func (c Caps) Has(want Caps) bool {
	for i := range c.w {
		if c.w[i]&want.w[i] != want.w[i] {
			return false
		}
	}
	return true
}

// HasAny reports whether c holds at least one capability in o.
//
//ds:def id=caps-hasany-n3gjxxvw owner=@khanakia stability=stable
func (c Caps) HasAny(o Caps) bool {
	for i := range c.w {
		if c.w[i]&o.w[i] != 0 {
			return true
		}
	}
	return false
}

// IsZero reports whether c is the empty set.
//
//ds:def id=caps-iszero-pmummwj2 owner=@khanakia stability=stable
func (c Caps) IsZero() bool { return c == Caps{} }

// Missing returns the capabilities in required that c lacks; the empty
// set means c covers them all.
//
//ds:def id=caps-missing-yt996zkr owner=@khanakia stability=stable
func (c Caps) Missing(required Caps) Caps { return required.Without(c) }

// each calls f with every capability number in c, lowest first.
func (c Caps) each(f func(n int)) {
	for i, w := range c.w {
		for w != 0 {
			b := bits.TrailingZeros64(w)
			f(i*64 + b)
			w &^= 1 << b
		}
	}
}

// String lists the capabilities joined by "|", such as "Durable|Cron",
// in number order; the empty set is "none". A number no capability of
// this release declares (read from a newer release's numeric JSON) prints
// as "#" and its number, such as "#200", so the form still round-trips
// through ParseCaps.
//
//ds:def id=caps-string-rrz2ge6p owner=@khanakia stability=stable
func (c Caps) String() string {
	if c.IsZero() {
		return "none"
	}
	var sb strings.Builder
	c.each(func(n int) {
		if sb.Len() > 0 {
			sb.WriteByte('|')
		}
		if name := capNames[n]; name != "" {
			sb.WriteString(name)
		} else {
			sb.WriteByte('#')
			sb.WriteString(strconv.Itoa(n))
		}
	})
	return sb.String()
}

// ParseCaps reads the form String writes: names joined by "|" (spaces
// around a name are ignored), "#" and a number for a capability by number,
// and "none" or "" for the empty set. An unknown name or an out-of-range
// number is an error matching ErrInvalid.
//
//ds:def id=parsecaps-vryjfj24 owner=@khanakia stability=stable
func ParseCaps(s string) (Caps, error) {
	var c Caps
	s = strings.TrimSpace(s)
	if s == "" || s == "none" {
		return c, nil
	}
	for _, part := range strings.Split(s, "|") {
		part = strings.TrimSpace(part)
		if num, ok := strings.CutPrefix(part, "#"); ok {
			n, err := strconv.Atoi(num)
			if err != nil || n < 0 || n >= capBits {
				return Caps{}, invalidf("capability number %q out of range 0..%d", part, capBits-1)
			}
			c = c.With(capBit(n))
			continue
		}
		n := capNumber(part)
		if n < 0 {
			return Caps{}, invalidf("unknown capability %q", part)
		}
		c = c.With(capBit(n))
	}
	return c, nil
}

// capNumber is the number of the capability called name, or -1.
func capNumber(name string) int {
	if name == "" {
		return -1
	}
	for n, have := range capNames {
		if have == name {
			return n
		}
	}
	return -1
}

// MarshalText writes String's form.
func (c Caps) MarshalText() ([]byte, error) { return []byte(c.String()), nil }

// UnmarshalText reads String's form (see ParseCaps).
func (c *Caps) UnmarshalText(text []byte) error {
	got, err := ParseCaps(string(text))
	if err != nil {
		return err
	}
	*c = got
	return nil
}

// UnmarshalJSON reads a JSON string in String's form, or a JSON number:
// the bits of the uint64 Caps of v0.1.x, whose capability numbers are the
// numbers kept here, so a stored or sent old value reads as the same set.
// null leaves c unchanged.
//
//ds:def id=caps-unmarshaljson-dngyx9tw owner=@khanakia stability=stable
func (c *Caps) UnmarshalJSON(data []byte) error {
	data = bytes.TrimSpace(data)
	switch {
	case bytes.Equal(data, []byte("null")):
		return nil
	case len(data) > 0 && data[0] == '"':
		var s string
		if err := json.Unmarshal(data, &s); err != nil {
			return invalidf("capabilities %s: %v", data, err)
		}
		return c.UnmarshalText([]byte(s))
	}
	v, err := strconv.ParseUint(string(data), 10, 64)
	if err != nil {
		return invalidf("capabilities %s: want a string such as \"Durable|Cron\" or a v0.1 number", data)
	}
	*c = Caps{}
	c.w[0] = v
	return nil
}
