package workflow

// DefineCapForTest declares capability number n for one test, the way the
// var block in caps.go does, and returns it with an undo that frees the
// number again. It lets the external tests prove a capability above 64
// end to end without the release declaring one.
func DefineCapForTest(n int, name string) (Caps, func()) {
	if capNames[n] != "" {
		panic("DefineCapForTest: number taken by " + capNames[n])
	}
	c := defineCap(n, name)
	return c, func() { capNames[n] = "" }
}

// CapBitsForTest is how many capability numbers a Caps holds.
const CapBitsForTest = capBits
