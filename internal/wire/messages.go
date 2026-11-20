package wire

// MessageType identifies the body carried after a Header. A message type is
// valid only when it is listed below; unknown values are rejected at the wire
// boundary before they reach the broker or client.
type MessageType uint8

const (
	// MessageHello opens a client session and declares its client identity and role.
	MessageHello MessageType = 1
	// MessageWelcome confirms a session and carries the current cluster topology.
	MessageWelcome MessageType = 2
	// MessageCreateTopic requests creation of a dynamic topic.
	MessageCreateTopic MessageType = 3
	// MessageDeleteTopic requests removal of a topic and triggers TopicClosed.
	MessageDeleteTopic MessageType = 4
	// MessageSubscribe adds the current session to a topic.
	MessageSubscribe MessageType = 5
	// MessageUnsubscribe removes the current session from a topic.
	MessageUnsubscribe MessageType = 6
	// MessagePublish carries an ephemeral event from a publisher to a topic.
	MessagePublish MessageType = 7
	// MessagePubResult reports broker acceptance, no subscribers, or topic absence.
	MessagePubResult MessageType = 8
	// MessageDeliver carries a sequenced event from a broker to one subscriber.
	MessageDeliver MessageType = 9
	// MessageGap tells a subscriber which sequence interval was not delivered.
	MessageGap MessageType = 10
	// MessageResume asks a new broker to continue a session from known sequences.
	MessageResume MessageType = 11
	// MessageThrottle tells a publisher that its admission limit was exceeded.
	MessageThrottle MessageType = 12
	// MessageRedirect tells a client to refresh its topology and use another broker.
	MessageRedirect MessageType = 13
	// MessageHeartbeat supports timeout-based liveness detection.
	MessageHeartbeat MessageType = 14
	// MessageTopicClosed informs subscribers that a topic was deleted.
	MessageTopicClosed MessageType = 15
	// MessageError reports a protocol or control-plane error.
	MessageError MessageType = 16
)

// Valid reports whether t is assigned by this protocol version.
func (t MessageType) Valid() bool {
	return t >= MessageHello && t <= MessageError
}

// String returns the stable protocol name used by logs and diagnostics.
func (t MessageType) String() string {
	switch t {
	case MessageHello:
		return "HELLO"
	case MessageWelcome:
		return "WELCOME"
	case MessageCreateTopic:
		return "CREATE_TOPIC"
	case MessageDeleteTopic:
		return "DELETE_TOPIC"
	case MessageSubscribe:
		return "SUBSCRIBE"
	case MessageUnsubscribe:
		return "UNSUBSCRIBE"
	case MessagePublish:
		return "PUBLISH"
	case MessagePubResult:
		return "PUB_RESULT"
	case MessageDeliver:
		return "DELIVER"
	case MessageGap:
		return "GAP"
	case MessageResume:
		return "RESUME"
	case MessageThrottle:
		return "THROTTLE"
	case MessageRedirect:
		return "REDIRECT"
	case MessageHeartbeat:
		return "HEARTBEAT"
	case MessageTopicClosed:
		return "TOPIC_CLOSED"
	case MessageError:
		return "ERROR"
	default:
		return "UNKNOWN"
	}
}
