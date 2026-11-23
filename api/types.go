package api

import "time"

// Topic is a flat, case-sensitive public topic identifier.
type Topic string

// ClientID identifies an application client across reconnects.
type ClientID string

// SessionID identifies one resumable client session.
type SessionID string

// SubscriptionID identifies one subscription within the cluster.
type SubscriptionID string

// RequestID identifies a retryable control-plane request.
type RequestID uint64

// Epoch identifies the current primary ownership generation of a partition.
type Epoch uint64

// Role declares how a client uses a session.
type Role uint8

const (
	// RoleConsumer receives deliveries from subscribed topics.
	RoleConsumer Role = 1
	// RolePublisher sends events to topics.
	RolePublisher Role = 2
	// RoleBoth both publishes and consumes on the same session.
	RoleBoth Role = 3
)

// Message is one ephemeral event. Sequence is assigned by the broker primary
// when the publication is accepted; a client leaves it zero while publishing.
// Payload is opaque to the middleware.
type Message struct {
	Topic       Topic
	PublisherID ClientID
	Sequence    uint64
	PublishedAt time.Time
	Payload     []byte
}

// PublishOutcome describes how the broker handled one publication.
type PublishOutcome uint8

const (
	// PublishDelivered means the broker accepted the event and queued it for one
	// or more current subscribers. It does not confirm end-to-end delivery.
	PublishDelivered PublishOutcome = 1
	// PublishNoSubscribers means the topic exists but had no subscribers.
	PublishNoSubscribers PublishOutcome = 2
	// PublishTopicNotFound means the target topic does not exist on the primary.
	PublishTopicNotFound PublishOutcome = 3
)

// PublishResult is returned to a publisher after the broker processes a
// publication. SubscriberCount is meaningful only for PublishDelivered.
type PublishResult struct {
	Outcome         PublishOutcome
	SubscriberCount uint32
}

// Gap identifies a contiguous per-topic sequence interval not delivered to a
// subscriber. Loss is expected under UDP and queue overload.
type Gap struct {
	Topic Topic
	From  uint64
	To    uint64
}
