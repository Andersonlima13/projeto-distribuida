package wire

import (
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestBodyRoundTripForAllMessageTypes(t *testing.T) {
	tests := []struct {
		name        string
		messageType MessageType
		body        any
	}{
		{
			name:        "hello",
			messageType: MessageHello,
			body:        HelloBody{RequestID: 1, ClientID: "client-1", Role: RoleBoth},
		},
		{
			name:        "welcome",
			messageType: MessageWelcome,
			body:        WelcomeBody{RequestID: 2, SessionID: "session-1", Epoch: 3, TopologyVersion: 4, PrimaryAddr: "127.0.0.1:9000"},
		},
		{
			name:        "create topic",
			messageType: MessageCreateTopic,
			body:        CreateTopicBody{RequestID: 10, Topic: "score"},
		},
		{
			name:        "delete topic",
			messageType: MessageDeleteTopic,
			body:        DeleteTopicBody{RequestID: 11, Topic: "score"},
		},
		{
			name:        "subscribe",
			messageType: MessageSubscribe,
			body:        SubscribeBody{RequestID: 12, SubscriptionID: "sub-1", Topic: "score"},
		},
		{
			name:        "unsubscribe",
			messageType: MessageUnsubscribe,
			body:        UnsubscribeBody{RequestID: 13, SubscriptionID: "sub-1", Topic: "score"},
		},
		{
			name:        "publish",
			messageType: MessagePublish,
			body:        PublishBody{RequestID: 14, Topic: "score", PublisherID: "pub-1", PublishedAtUnixNano: 12345, Payload: []byte("goal")},
		},
		{
			name:        "pub result",
			messageType: MessagePubResult,
			body:        PubResultBody{RequestID: 15, Outcome: PublishOutcomeDelivered, SubscriberCount: 9},
		},
		{
			name:        "deliver",
			messageType: MessageDeliver,
			body:        DeliverBody{SubscriptionID: "sub-1", Topic: "score", PublisherID: "pub-1", Sequence: 22, PublishedAtUnixNano: 45678, Payload: []byte("goal")},
		},
		{
			name:        "gap",
			messageType: MessageGap,
			body:        GapBody{SubscriptionID: "sub-1", Topic: "score", From: 10, To: 19},
		},
		{
			name:        "resume",
			messageType: MessageResume,
			body: ResumeBody{
				RequestID: 20,
				SessionID: "session-1",
				Cursors:   []ResumeCursor{{Topic: "score", LastSequence: 77}, {Topic: "chat", LastSequence: 101}},
			},
		},
		{
			name:        "throttle",
			messageType: MessageThrottle,
			body:        ThrottleBody{Topic: "score", RetryAfterMillis: 200, Reason: "rate limit"},
		},
		{
			name:        "redirect",
			messageType: MessageRedirect,
			body:        RedirectBody{Topic: "score", BrokerAddr: "127.0.0.2:9000", Epoch: 8},
		},
		{
			name:        "heartbeat",
			messageType: MessageHeartbeat,
			body:        HeartbeatBody{SessionID: "session-1", SentAtUnixNano: 999},
		},
		{
			name:        "topic closed",
			messageType: MessageTopicClosed,
			body:        TopicClosedBody{Topic: "score", Reason: "deleted"},
		},
		{
			name:        "error",
			messageType: MessageError,
			body:        ErrorBody{RequestID: 30, Code: 9, Message: "internal"},
		},
		{
			name:        "control ack",
			messageType: MessageControlAck,
			body:        ControlAckBody{RequestID: 31, Operation: 4, SessionID: "session-1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded, err := EncodeBody(tt.messageType, tt.body)
			if err != nil {
				t.Fatalf("EncodeBody() error = %v", err)
			}
			decoded, err := DecodeBody(tt.messageType, encoded)
			if err != nil {
				t.Fatalf("DecodeBody() error = %v", err)
			}
			if !reflect.DeepEqual(decoded, tt.body) {
				t.Fatalf("decoded = %#v, want %#v", decoded, tt.body)
			}
		})
	}
}

func TestEncodeBodyRejectsWrongTypeForMessage(t *testing.T) {
	_, err := EncodeBody(MessageHello, PublishBody{})
	if !errors.Is(err, ErrInvalidBodyType) {
		t.Fatalf("EncodeBody() error = %v, want %v", err, ErrInvalidBodyType)
	}
}

func TestDecodeBodyRejectsTrailingBytes(t *testing.T) {
	encoded, err := EncodeBody(MessageHello, HelloBody{RequestID: 1, ClientID: "c", Role: RoleConsumer})
	if err != nil {
		t.Fatal(err)
	}
	encoded = append(encoded, 0)
	_, err = DecodeBody(MessageHello, encoded)
	if !errors.Is(err, ErrBodyTrailingData) {
		t.Fatalf("DecodeBody() error = %v, want %v", err, ErrBodyTrailingData)
	}
}

func TestHelloRoleValidation(t *testing.T) {
	_, err := EncodeBody(MessageHello, HelloBody{RequestID: 1, ClientID: "c", Role: 0})
	if !errors.Is(err, ErrInvalidBody) {
		t.Fatalf("EncodeBody() error = %v, want %v", err, ErrInvalidBody)
	}
}

func TestPubResultOutcomeValidation(t *testing.T) {
	_, err := EncodeBody(MessagePubResult, PubResultBody{RequestID: 1, Outcome: 9})
	if !errors.Is(err, ErrInvalidBody) {
		t.Fatalf("EncodeBody() error = %v, want %v", err, ErrInvalidBody)
	}
}

func TestGapIntervalValidation(t *testing.T) {
	_, err := EncodeBody(MessageGap, GapBody{SubscriptionID: "sub", Topic: "t", From: 9, To: 8})
	if !errors.Is(err, ErrInvalidBody) {
		t.Fatalf("EncodeBody() error = %v, want %v", err, ErrInvalidBody)
	}
}

func TestStringLengthValidation(t *testing.T) {
	tooLong := strings.Repeat("a", maxStringLen+1)
	_, err := EncodeBody(MessageCreateTopic, CreateTopicBody{RequestID: 1, Topic: tooLong})
	if !errors.Is(err, ErrStringTooLong) {
		t.Fatalf("EncodeBody() error = %v, want %v", err, ErrStringTooLong)
	}
}

func TestDecodeBodyRejectsTruncatedInput(t *testing.T) {
	encoded, err := EncodeBody(MessagePublish, PublishBody{RequestID: 1, Topic: "t", PublisherID: "p", PublishedAtUnixNano: 1, Payload: []byte("ok")})
	if err != nil {
		t.Fatal(err)
	}
	truncated := encoded[:len(encoded)-1]
	_, err = DecodeBody(MessagePublish, truncated)
	if !errors.Is(err, ErrInvalidBody) {
		t.Fatalf("DecodeBody() error = %v, want %v", err, ErrInvalidBody)
	}
}
