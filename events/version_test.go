package events_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/events"
)

// v0 had "amount" in cents as a number; v1 renamed it "cents"; v2 added
// a currency with a default.
var orderUpcasters = map[int]workflow.Upcaster{
	0: func(raw []byte) ([]byte, error) {
		var v map[string]any
		if err := json.Unmarshal(raw, &v); err != nil {
			return nil, err
		}
		v["cents"] = v["amount"]
		delete(v, "amount")
		return json.Marshal(v)
	},
	1: func(raw []byte) ([]byte, error) {
		var v map[string]any
		if err := json.Unmarshal(raw, &v); err != nil {
			return nil, err
		}
		if _, ok := v["currency"]; !ok {
			v["currency"] = "EUR"
		}
		return json.Marshal(v)
	},
}

// TestUpcastBeforeSubscribers: a message of any older version reaches
// Filter, Job and the run in the current shape; the current version
// passes untouched; a newer version is refused before anything runs; an
// event without upcasters ignores Version.
func TestUpcastBeforeSubscribers(t *testing.T) {
	cases := []struct {
		name    string
		version int
		payload string
		want    string
		err     bool
	}{
		{"version 0", 0, `{"amount":500}`, `{"cents":500,"currency":"EUR"}`, false},
		{"version 1", 1, `{"cents":500}`, `{"cents":500,"currency":"EUR"}`, false},
		{"current version untouched", 2, `{"cents":5,"currency":"USD"}`, `{"cents":5,"currency":"USD"}`, false},
		{"newer than current", 3, `{}`, ``, true},
		{"negative", -1, `{}`, ``, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var rec recorder
			c := started(t, rec.workflow("charge"))
			log := events.NewMemoryLog()
			b, err := events.New(c, events.WithUpcasters("order.paid", 2, orderUpcasters), events.WithLog(log))
			if err != nil {
				t.Fatal(err)
			}
			var filterSaw, jobSaw string
			var filterVersion int
			err = b.Subscribe(events.Subscription{Name: "charge", Event: "order.paid",
				Filter: func(m events.Message) bool { filterSaw, filterVersion = string(m.Payload), m.Version; return true },
				Job: func(m events.Message) (workflow.Job, error) {
					jobSaw = string(m.Payload)
					return workflow.Job{Workflow: "charge", Payload: m.Payload}, nil
				}})
			if err != nil {
				t.Fatal(err)
			}
			p, err := b.Publish(context.Background(), events.Message{ID: "e1", Name: "order.paid", Version: tc.version, Payload: []byte(tc.payload)})
			if tc.err {
				if !errors.Is(err, workflow.ErrInvalid) || len(p.Deliveries) != 0 || log.Len() != 0 {
					t.Fatalf("err = %v, deliveries %d, logged %d; want ErrInvalid, nothing delivered or logged", err, len(p.Deliveries), log.Len())
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			waitAll(t, c, p)
			if filterSaw != tc.want || jobSaw != tc.want || rec.ins[0] != tc.want || filterVersion != 2 {
				t.Fatalf("filter saw %s (v%d), job saw %s, run got %s; want %s at v2", filterSaw, filterVersion, jobSaw, rec.ins[0], tc.want)
			}
		})
	}

	t.Run("event without upcasters ignores Version", func(t *testing.T) {
		var rec recorder
		c := started(t, rec.workflow("w"))
		b, _ := events.New(c, events.WithUpcasters("order.paid", 2, orderUpcasters))
		_ = b.Subscribe(events.Subscription{Name: "s", Event: "other", Workflow: "w"})
		p, err := b.Publish(context.Background(), events.Message{Name: "other", Version: 9, Payload: []byte(`1`)})
		if err != nil {
			t.Fatal(err)
		}
		waitAll(t, c, p)
		if rec.ins[0] != "1" {
			t.Fatalf("payload = %s", rec.ins[0])
		}
	})
}

// TestUpcastFailureIsLoggedThenRefused: an upcaster's error refuses the
// publish with ErrInvalid after the message is logged, so a fixed deploy
// can replay it.
func TestUpcastFailureIsLoggedThenRefused(t *testing.T) {
	c := started(t)
	log := events.NewMemoryLog()
	broken := map[int]workflow.Upcaster{0: func([]byte) ([]byte, error) { return nil, errors.New("bad shape") }}
	b, err := events.New(c, events.WithUpcasters("e", 1, broken), events.WithLog(log))
	if err != nil {
		t.Fatal(err)
	}
	_ = b.Subscribe(events.Subscription{Name: "s", Event: "e", Workflow: "w"})
	p, err := b.Publish(context.Background(), events.Message{ID: "1", Name: "e"})
	if !errors.Is(err, workflow.ErrInvalid) || len(p.Deliveries) != 0 || log.Len() != 1 {
		t.Fatalf("err = %v, deliveries %d, logged %d", err, len(p.Deliveries), log.Len())
	}
}

// TestWithUpcastersRefusals: New refuses an invalid version policy.
func TestWithUpcastersRefusals(t *testing.T) {
	up := func(b []byte) ([]byte, error) { return b, nil }
	cases := map[string][]events.Option{
		"empty event":      {events.WithUpcasters("", 1, map[int]workflow.Upcaster{0: up})},
		"negative current": {events.WithUpcasters("e", -1, nil)},
		"key at current":   {events.WithUpcasters("e", 1, map[int]workflow.Upcaster{1: up})},
		"negative key":     {events.WithUpcasters("e", 1, map[int]workflow.Upcaster{-1: up})},
		"nil upcaster":     {events.WithUpcasters("e", 1, map[int]workflow.Upcaster{0: nil})},
		"same event twice": {events.WithUpcasters("e", 1, map[int]workflow.Upcaster{0: up}), events.WithUpcasters("e", 1, map[int]workflow.Upcaster{0: up})},
	}
	for name, opts := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := events.New(started(t), opts...); !errors.Is(err, workflow.ErrInvalid) {
				t.Fatalf("err = %v, want ErrInvalid", err)
			}
		})
	}
	b, err := events.New(started(t), events.WithUpcasters("e", 1, map[int]workflow.Upcaster{0: up}))
	if err != nil {
		t.Fatal(err)
	}
	if v, ok := b.CurrentVersion("e"); v != 1 || !ok {
		t.Fatalf("CurrentVersion = %d, %v", v, ok)
	}
}
