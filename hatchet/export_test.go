package hatchet

import "context"

// SetAfterChunk makes RunGroup call f after each chunk it started, so a
// test can kill the process between two bulk triggers.
func SetAfterChunk(c *Client, f func()) { c.afterChunk = f }

// PauseWithTTL pauses a workflow with a chosen queue TTL, so the live
// control test can show the engine cancelling held runs once a short TTL
// passes, the trap pauseQueueTTL exists to avoid.
func PauseWithTTL(ctx context.Context, c *Client, name, ttl string) error {
	req, err := pauseRequest(ttl)
	if err != nil {
		return err
	}
	return c.updateWorkflow(ctx, name, req)
}
