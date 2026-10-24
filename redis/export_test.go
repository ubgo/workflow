package redis

// Crash makes c behave like a killed process: from now on it writes
// nothing (no lease renewal, no hand-back, no result) and claims nothing,
// so the runs it held are taken over only once their leases expire.
func Crash(c *Client) {
	c.mu.Lock()
	c.crashed = true
	c.mu.Unlock()
}
