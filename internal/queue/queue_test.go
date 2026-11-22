package queue

import (
	"errors"
	"testing"
)

func TestNewRejectsUnboundedOrDisabledLimits(t *testing.T) {
	for _, limits := range []Limits{{}, {MaxMessages: 1}, {MaxBytes: 1}, {MaxMessages: -1, MaxBytes: 1}} {
		if _, err := New(limits); !errors.Is(err, ErrInvalidLimits) {
			t.Fatalf("New(%+v) error = %v, want %v", limits, err, ErrInvalidLimits)
		}
	}
}

func TestDropOldestCreatesGapBeforeRetainedMessages(t *testing.T) {
	q := mustQueue(t, Limits{MaxMessages: 2, MaxBytes: 6})
	mustEnqueue(t, q, 1, "one")
	mustEnqueue(t, q, 2, "two")
	result, err := q.Enqueue(Item{Sequence: 3, Payload: []byte("tri")})
	if err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}
	if !result.Accepted || result.DroppedMessages != 1 || result.DroppedBytes != 3 {
		t.Fatalf("result = %+v", result)
	}

	mustGap(t, q, 1, 1)
	mustMessage(t, q, 2, "two")
	mustMessage(t, q, 3, "tri")
	if _, ok := q.Next(); ok {
		t.Fatal("Next() returned an event after queue drained")
	}

	stats := q.Stats()
	if stats.DroppedMessages != 1 || stats.DroppedBytes != 3 || stats.QueuedMessages != 0 || stats.PendingGaps != 0 {
		t.Fatalf("stats = %+v", stats)
	}
}

func TestByteLimitDropsMultipleOldestAndCoalescesGap(t *testing.T) {
	q := mustQueue(t, Limits{MaxMessages: 5, MaxBytes: 4})
	mustEnqueue(t, q, 1, "aa")
	mustEnqueue(t, q, 2, "bb")
	mustEnqueue(t, q, 3, "ccc")
	mustGap(t, q, 1, 2)
	mustMessage(t, q, 3, "ccc")
}

func TestOversizedItemIsDroppedButGapKeepsStreamOrder(t *testing.T) {
	q := mustQueue(t, Limits{MaxMessages: 2, MaxBytes: 3})
	mustEnqueue(t, q, 1, "one")
	result, err := q.Enqueue(Item{Sequence: 2, Payload: []byte("toolong")})
	if err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}
	if result.Accepted || result.DroppedMessages != 1 || result.DroppedBytes != 7 {
		t.Fatalf("result = %+v", result)
	}
	mustMessage(t, q, 1, "one")
	mustGap(t, q, 2, 2)
}

func TestPayloadIsCopiedOnEnqueue(t *testing.T) {
	q := mustQueue(t, Limits{MaxMessages: 1, MaxBytes: 8})
	payload := []byte("event")
	if _, err := q.Enqueue(Item{Sequence: 1, Payload: payload}); err != nil {
		t.Fatal(err)
	}
	payload[0] = 'X'
	mustMessage(t, q, 1, "event")
}

func TestEnqueueRejectsDuplicateOrOlderSequence(t *testing.T) {
	q := mustQueue(t, Limits{MaxMessages: 2, MaxBytes: 8})
	mustEnqueue(t, q, 2, "two")
	if _, err := q.Enqueue(Item{Sequence: 2, Payload: []byte("dup")}); !errors.Is(err, ErrSequenceNotIncreasing) {
		t.Fatalf("duplicate error = %v", err)
	}
	if _, err := q.Enqueue(Item{Sequence: 1, Payload: []byte("old")}); !errors.Is(err, ErrSequenceNotIncreasing) {
		t.Fatalf("older error = %v", err)
	}
	mustMessage(t, q, 2, "two")
}

func mustQueue(t *testing.T, limits Limits) *Queue {
	t.Helper()
	q, err := New(limits)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return q
}

func mustEnqueue(t *testing.T, q *Queue, sequence uint64, payload string) {
	t.Helper()
	result, err := q.Enqueue(Item{Sequence: sequence, Payload: []byte(payload)})
	if err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}
	if !result.Accepted {
		t.Fatalf("Enqueue() result = %+v", result)
	}
}

func mustMessage(t *testing.T, q *Queue, sequence uint64, payload string) {
	t.Helper()
	event, ok := q.Next()
	if !ok {
		t.Fatal("Next() returned no event")
	}
	if event.Kind != EventMessage || event.Item.Sequence != sequence || string(event.Item.Payload) != payload {
		t.Fatalf("event = %+v, want message %d %q", event, sequence, payload)
	}
}

func mustGap(t *testing.T, q *Queue, from, to uint64) {
	t.Helper()
	event, ok := q.Next()
	if !ok {
		t.Fatal("Next() returned no event")
	}
	if event.Kind != EventGap || event.Gap != (Gap{From: from, To: to}) {
		t.Fatalf("event = %+v, want gap %d-%d", event, from, to)
	}
}
