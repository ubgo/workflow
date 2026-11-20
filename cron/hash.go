package cron

import (
	"hash/fnv"
	"strconv"
	"strings"
)

// hashDOMMax is the top of a day-of-month "H": days 1-28 exist in every
// month, so a hashed day never lands on a 30th that February lacks.
const hashDOMMax = 28

// Hash rewrites the hashed items of a five-field expression into fixed
// values derived from seed, and returns a standard expression that Parse
// accepts, so the result runs on every driver, engines with their own
// dialect included. It spreads many schedules written alike ("once an
// hour") across the field's range instead of firing them all at minute 0.
//
// The hashed items, allowed in any of the five fields and inside lists:
//   - "H": one value in the field's range (day-of-month 1-28, so every
//     month has it; day-of-week 0-6).
//   - "H(a-b)": one value in a-b ("H(0-29)" is a minute in the first half
//     hour).
//   - "H/n": every n-th value, starting at an offset in [min, min+n) of
//     the range ("H/15" in minutes is "7-59/15" for one seed, "11-59/15"
//     for another).
//   - "H(a-b)/n": the same within a-b.
//
// The values come from FNV-1a of seed, the field and the item's position,
// so one seed gives the same expression on every call, process and node
// (pass a stable name: the schedule's or the job's), different fields of
// one seed move independently, and different seeds spread evenly. Hash it
// once where the schedule is declared and store the result; an
// expression without "H" comes back trimmed and otherwise unchanged.
// Macros are returned as they are. Every error is a *ParseError naming
// the field.
func Hash(expr, seed string) (string, error) {
	trimmed := strings.TrimSpace(expr)
	if trimmed == "" {
		return "", &ParseError{Expr: expr, Field: "expression", Reason: "empty expression"}
	}
	if strings.HasPrefix(trimmed, "@") {
		if _, err := Parse(trimmed); err != nil {
			return "", err
		}
		return trimmed, nil
	}
	parts := strings.Fields(trimmed)
	if len(parts) != len(fields) {
		_, err := Parse(expr)
		return "", err
	}
	for i, f := range fields {
		out, err := hashField(parts[i], f, i, seed)
		if err != nil {
			err.Expr = expr
			return "", err
		}
		parts[i] = out
	}
	result := strings.Join(parts, " ")
	if _, err := Parse(result); err != nil {
		if pe, ok := err.(*ParseError); ok {
			pe.Expr = expr // name what the caller wrote, not the rewrite
		}
		return "", err
	}
	return result, nil
}

// hashField rewrites each hashed item of one field.
func hashField(text string, f field, index int, seed string) (string, *ParseError) {
	if !strings.Contains(strings.ToUpper(text), "H") {
		return text, nil
	}
	lo, hi := f.min, f.max
	switch f.name {
	case "day-of-month":
		hi = hashDOMMax
	case "day-of-week":
		hi = 6
	}
	items := strings.Split(text, ",")
	for n, item := range items {
		if !strings.HasPrefix(strings.ToUpper(item), "H") {
			continue
		}
		fail := func(reason string) *ParseError {
			return &ParseError{Field: f.name, Value: item, Reason: reason}
		}
		rest := item[1:]
		a, b := lo, hi
		if strings.HasPrefix(rest, "(") {
			end := strings.Index(rest, ")")
			if end < 0 {
				return "", fail("H( has no closing )")
			}
			ra, rb, ok := strings.Cut(rest[1:end], "-")
			if !ok {
				return "", fail("want H(a-b)")
			}
			var err error
			if a, err = parseValue(ra, f); err != nil {
				return "", fail(err.Error())
			}
			if b, err = parseValue(rb, f); err != nil {
				return "", fail(err.Error())
			}
			if a > b {
				return "", fail("range start " + ra + " is after end " + rb)
			}
			rest = rest[end+1:]
		}
		h := hashValue(seed, index, n)
		switch {
		case rest == "":
			items[n] = strconv.Itoa(a + int(h%uint64(b-a+1)))
		case strings.HasPrefix(rest, "/"):
			step, err := strconv.Atoi(rest[1:])
			if err != nil || step < 1 {
				return "", fail("step must be a positive integer")
			}
			start := a + int(h%uint64(min(step, b-a+1)))
			items[n] = strconv.Itoa(start) + "-" + strconv.Itoa(b) + "/" + strconv.Itoa(step)
		default:
			return "", fail("want H, H(a-b), H/n or H(a-b)/n")
		}
	}
	return strings.Join(items, ","), nil
}

// hashValue is FNV-1a of the seed, the field index and the item's position.
func hashValue(seed string, field, item int) uint64 {
	h := fnv.New64a()
	h.Write([]byte(seed))
	h.Write([]byte{0, byte(field), byte(item)})
	return h.Sum64()
}
