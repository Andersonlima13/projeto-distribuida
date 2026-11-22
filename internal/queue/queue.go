package queue

import (
	"errors"
	"fmt"
	"sync"
)

var (
	// ErrInvalidLimits means that a queue would not have finite positive bounds.
	ErrInvalidLimits = errors.New("queue: maximum messages and bytes must be positive")
	// ErrSequenceNotIncreasing means the producer did not preserve topic order.
	ErrSequenceNotIncreasing = errors.New("queue: sequence must be strictly increasing")
)

// Limits bounds one subscriber queue. Both values are mandatory so a burst
// cannot exhaust either descriptors or memory.
type Limits struct {
	MaxMessages int
	MaxBytes    int
}

// Item is an already sequenced event awaiting delivery to one subscriber.
// Payload is opaque to the queue.
type Item struct {
	Sequence uint64
	Payload  []byte
}

// Gap reports a contiguous sequence interval that was not retained for this
// subscriber. A consumer turns it into the wire-level GAP message.
type Gap struct {
	From uint64
	To   uint64
}

// EventKind identifies the result returned by Next.
type EventKind uint8

const (
	// EventMessage represents one retained Item ready for delivery.
	EventMessage EventKind = 1
	// EventGap represents one pending discarded sequence interval.
	EventGap EventKind = 2
)

// Event is returned by Next. Exactly one of Item or Gap is meaningful,
// according to Kind.
type Event struct {
	Kind EventKind
	Item Item
	Gap  Gap
}

// EnqueueResult describes how one enqueue attempt affected the queue.
// Accepted is false only when the new item is larger than MaxBytes and cannot
// fit even in an empty queue; that item is discarded and represented by a GAP.
type EnqueueResult struct {
	Accepted        bool
	DroppedMessages uint64
	DroppedBytes    uint64
}

// Stats is a point-in-time snapshot of queue pressure and cumulative loss.
type Stats struct {
	QueuedMessages  int
	QueuedBytes     int
	DroppedMessages uint64
	DroppedBytes    uint64
	PendingGaps     int
}

// Queue is a bounded, concurrency-safe buffer for one subscriber. It uses
// drop-oldest overflow: freshness wins over an event that waited too long.
type Queue struct {
	mu sync.Mutex

	limits Limits
	items  []storedItem
	bytes  int
	gaps   []Gap

	hasSeenSequence  bool
	lastSeenSequence uint64
	droppedMessages  uint64
	droppedBytes     uint64
}

type storedItem struct {
	sequence uint64
	payload  []byte
}

// New creates a bounded queue. A zero or negative bound is rejected because
// it either accepts no normal traffic or obscures the intended overload rule.
func New(limits Limits) (*Queue, error) {
	if limits.MaxMessages <= 0 || limits.MaxBytes <= 0 {
		return nil, fmt.Errorf("%w: messages=%d bytes=%d", ErrInvalidLimits, limits.MaxMessages, limits.MaxBytes)
	}
	return &Queue{limits: limits}, nil
}

// Enqueue adds item or applies drop-oldest until it fits. The input payload is
// copied so a caller can safely reuse its receive or encode buffer after this
// method returns.
//
// Items must arrive in strictly increasing sequence order. The broker assigns
// this sequence before fanout, therefore accepting a duplicate or older item
// here would make a later GAP ambiguous.
func (q *Queue) Enqueue(item Item) (EnqueueResult, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.hasSeenSequence && item.Sequence <= q.lastSeenSequence {
		return EnqueueResult{}, fmt.Errorf("%w: got %d after %d", ErrSequenceNotIncreasing, item.Sequence, q.lastSeenSequence)
	}
	q.hasSeenSequence = true
	q.lastSeenSequence = item.Sequence

	itemBytes := len(item.Payload)
	if itemBytes > q.limits.MaxBytes {
		q.recordDrop(item.Sequence)
		q.droppedMessages++
		q.droppedBytes += uint64(itemBytes)
		return EnqueueResult{DroppedMessages: 1, DroppedBytes: uint64(itemBytes)}, nil
	}

	result := EnqueueResult{Accepted: true}
	for len(q.items) >= q.limits.MaxMessages || q.bytes+itemBytes > q.limits.MaxBytes {
		dropped := q.items[0]
		q.items[0] = storedItem{}
		q.items = q.items[1:]
		q.bytes -= len(dropped.payload)
		q.recordDrop(dropped.sequence)
		q.droppedMessages++
		q.droppedBytes += uint64(len(dropped.payload))
		result.DroppedMessages++
		result.DroppedBytes += uint64(len(dropped.payload))
	}

	payload := append([]byte(nil), item.Payload...)
	q.items = append(q.items, storedItem{sequence: item.Sequence, payload: payload})
	q.bytes += itemBytes
	return result, nil
}

// Next returns the next ordered event for this subscriber. A GAP is returned
// before the first still-queued sequence after that interval, while a GAP for
// a later discarded item waits until older retained messages were returned.
// This ordering lets a delivery worker translate Events directly into DELIVER
// and GAP wire frames without reordering a topic stream.
func (q *Queue) Next() (Event, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.gaps) > 0 && (len(q.items) == 0 || q.gaps[0].From < q.items[0].sequence) {
		gap := q.gaps[0]
		q.gaps[0] = Gap{}
		q.gaps = q.gaps[1:]
		return Event{Kind: EventGap, Gap: gap}, true
	}
	if len(q.items) == 0 {
		return Event{}, false
	}
	item := q.items[0]
	q.items[0] = storedItem{}
	q.items = q.items[1:]
	q.bytes -= len(item.payload)
	return Event{Kind: EventMessage, Item: Item{Sequence: item.sequence, Payload: item.payload}}, true
}

// Stats returns a consistent snapshot suitable for degradation decisions and
// metrics collection.
func (q *Queue) Stats() Stats {
	q.mu.Lock()
	defer q.mu.Unlock()
	return Stats{
		QueuedMessages:  len(q.items),
		QueuedBytes:     q.bytes,
		DroppedMessages: q.droppedMessages,
		DroppedBytes:    q.droppedBytes,
		PendingGaps:     len(q.gaps),
	}
}

func (q *Queue) recordDrop(sequence uint64) {
	if count := len(q.gaps); count > 0 && q.gaps[count-1].To+1 == sequence {
		q.gaps[count-1].To = sequence
		return
	}
	q.gaps = append(q.gaps, Gap{From: sequence, To: sequence})
}
