package broker

import (
	"errors"
	"net"
	"testing"

	"pubsub/api"
	"pubsub/internal/admission"
	"pubsub/internal/queue"
	"pubsub/internal/topology"
	"pubsub/internal/wire"
)

func TestHelloReturnsWelcome(t *testing.T) {
	b := mustBroker(t)
	from := udpAddr(t, "127.0.0.1", 10001)

	outbound := mustHandle(t, b, from, wire.MessageHello, wire.HelloBody{RequestID: 1, ClientID: "client-a", Role: wire.RoleBoth})
	if len(outbound) != 1 {
		t.Fatalf("outbound length = %d, want 1", len(outbound))
	}

	h, body := decodeDatagram(t, outbound[0].Payload)
	if h.Type != wire.MessageWelcome {
		t.Fatalf("response type = %s, want %s", h.Type, wire.MessageWelcome)
	}
	welcome := body.(wire.WelcomeBody)
	if welcome.RequestID != 1 || welcome.SessionID == "" {
		t.Fatalf("welcome = %+v", welcome)
	}
}

func TestPublishFanoutReturnsPubResultAndDeliver(t *testing.T) {
	b := mustBroker(t)
	publisher := udpAddr(t, "127.0.0.1", 10001)
	subscriber := udpAddr(t, "127.0.0.1", 10002)

	mustHandle(t, b, publisher, wire.MessageHello, wire.HelloBody{RequestID: 1, ClientID: "publisher", Role: wire.RolePublisher})
	mustHandle(t, b, subscriber, wire.MessageHello, wire.HelloBody{RequestID: 2, ClientID: "subscriber", Role: wire.RoleConsumer})
	mustHandle(t, b, publisher, wire.MessageCreateTopic, wire.CreateTopicBody{RequestID: 3, Topic: "score"})
	mustHandle(t, b, subscriber, wire.MessageSubscribe, wire.SubscribeBody{RequestID: 4, SubscriptionID: "sub-1", Topic: "score"})

	outbound := mustHandle(t, b, publisher, wire.MessagePublish, wire.PublishBody{RequestID: 5, Topic: "score", PublisherID: "publisher", PublishedAtUnixNano: 123, Payload: []byte("goal")})
	if len(outbound) != 2 {
		t.Fatalf("outbound length = %d, want 2", len(outbound))
	}

	pubHeader, pubBodyAny := decodeDatagram(t, outbound[0].Payload)
	if pubHeader.Type != wire.MessagePubResult {
		t.Fatalf("first type = %s, want %s", pubHeader.Type, wire.MessagePubResult)
	}
	pubBody := pubBodyAny.(wire.PubResultBody)
	if pubBody.Outcome != wire.PublishOutcomeDelivered || pubBody.SubscriberCount != 1 {
		t.Fatalf("pub result = %+v", pubBody)
	}
	if outbound[0].To.Port != publisher.Port {
		t.Fatalf("pub result destination = %v, want publisher %v", outbound[0].To, publisher)
	}

	deliverHeader, deliverBodyAny := decodeDatagram(t, outbound[1].Payload)
	if deliverHeader.Type != wire.MessageDeliver {
		t.Fatalf("second type = %s, want %s", deliverHeader.Type, wire.MessageDeliver)
	}
	deliverBody := deliverBodyAny.(wire.DeliverBody)
	if deliverBody.Topic != "score" || string(deliverBody.Payload) != "goal" || deliverBody.Sequence != 1 {
		t.Fatalf("deliver body = %+v", deliverBody)
	}
	if outbound[1].To.Port != subscriber.Port {
		t.Fatalf("deliver destination = %v, want subscriber %v", outbound[1].To, subscriber)
	}
}

func TestPublishWithoutTopicReturnsTopicNotFound(t *testing.T) {
	b := mustBroker(t)
	publisher := udpAddr(t, "127.0.0.1", 10001)
	mustHandle(t, b, publisher, wire.MessageHello, wire.HelloBody{RequestID: 1, ClientID: "publisher", Role: wire.RolePublisher})

	outbound := mustHandle(t, b, publisher, wire.MessagePublish, wire.PublishBody{RequestID: 9, Topic: "missing", PublisherID: "publisher", Payload: []byte("x")})
	if len(outbound) != 1 {
		t.Fatalf("outbound length = %d, want 1", len(outbound))
	}
	_, body := decodeDatagram(t, outbound[0].Payload)
	pubResult := body.(wire.PubResultBody)
	if pubResult.Outcome != wire.PublishOutcomeTopicNotFound {
		t.Fatalf("outcome = %d, want %d", pubResult.Outcome, wire.PublishOutcomeTopicNotFound)
	}
}

func TestSubscribeWithoutSessionReturnsError(t *testing.T) {
	b := mustBroker(t)
	from := udpAddr(t, "127.0.0.1", 10001)
	outbound := mustHandle(t, b, from, wire.MessageSubscribe, wire.SubscribeBody{RequestID: 1, SubscriptionID: "sub", Topic: "score"})
	if len(outbound) != 1 {
		t.Fatalf("outbound length = %d, want 1", len(outbound))
	}
	h, body := decodeDatagram(t, outbound[0].Payload)
	if h.Type != wire.MessageError {
		t.Fatalf("type = %s, want %s", h.Type, wire.MessageError)
	}
	errBody := body.(wire.ErrorBody)
	if errBody.Code != uint16(api.ErrorSessionNotFound) {
		t.Fatalf("error code = %d, want %d", errBody.Code, api.ErrorSessionNotFound)
	}
}

func TestHandleDatagramRejectsNilSource(t *testing.T) {
	b := mustBroker(t)
	_, err := b.HandleDatagram([]byte("x"), nil)
	if !errors.Is(err, ErrNilSourceAddress) {
		t.Fatalf("error = %v, want %v", err, ErrNilSourceAddress)
	}
}

func TestControlRequestReplayReturnsCachedResponse(t *testing.T) {
	b := mustBroker(t)
	from := udpAddr(t, "127.0.0.1", 10001)
	mustHandle(t, b, from, wire.MessageHello, wire.HelloBody{RequestID: 1, ClientID: "client-a", Role: wire.RoleBoth})

	request := wire.CreateTopicBody{RequestID: 42, Topic: "score"}
	first := mustHandle(t, b, from, wire.MessageCreateTopic, request)
	second := mustHandle(t, b, from, wire.MessageCreateTopic, request)

	if len(first) != 1 || len(second) != 1 {
		t.Fatalf("first/second lengths = %d/%d, want 1/1", len(first), len(second))
	}
	if string(first[0].Payload) != string(second[0].Payload) {
		t.Fatalf("cached response differs across replay")
	}
	if _, body := decodeDatagram(t, first[0].Payload); body.(wire.ControlAckBody).RequestID != 42 {
		t.Fatalf("ACK request ID mismatch")
	}
}

func TestPublishBurstReturnsThrottle(t *testing.T) {
	b, err := New(Config{
		NodeAddress: "127.0.0.1:9000",
		Epoch:       1,
		QueueLimits: queue.Limits{MaxMessages: 8, MaxBytes: 1024},
		Admission:   admission.TokenBucketConfig{RatePerSecond: 1, Burst: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	publisher := udpAddr(t, "127.0.0.1", 10001)
	mustHandle(t, b, publisher, wire.MessageHello, wire.HelloBody{RequestID: 1, ClientID: "publisher", Role: wire.RolePublisher})
	mustHandle(t, b, publisher, wire.MessageCreateTopic, wire.CreateTopicBody{RequestID: 2, Topic: "score"})

	first := mustHandle(t, b, publisher, wire.MessagePublish, wire.PublishBody{RequestID: 3, Topic: "score", PublisherID: "publisher", Payload: []byte("one")})
	if len(first) != 1 {
		t.Fatalf("first publish outbound = %d, want 1", len(first))
	}
	second := mustHandle(t, b, publisher, wire.MessagePublish, wire.PublishBody{RequestID: 4, Topic: "score", PublisherID: "publisher", Payload: []byte("two")})
	if len(second) != 1 {
		t.Fatalf("throttled publish outbound = %d, want 1", len(second))
	}
	h, body := decodeDatagram(t, second[0].Payload)
	if h.Type != wire.MessageThrottle {
		t.Fatalf("second type = %s, want THROTTLE", h.Type)
	}
	throttle := body.(wire.ThrottleBody)
	if throttle.Topic != "score" || throttle.RetryAfterMillis == 0 {
		t.Fatalf("throttle = %+v", throttle)
	}
	metrics := b.Metrics()
	if metrics.Accepted != 1 || metrics.Throttled != 1 {
		t.Fatalf("metrics = %+v", metrics)
	}
}

func TestCreateTopicRedirectsToTopologyPrimary(t *testing.T) {
	staticTopology, err := topology.New([]topology.Partition{{
		ID: 0, Epoch: 9, PrimaryAddr: "127.0.0.1:9001", ReplicaAddr: "127.0.0.1:9101",
	}}, 1)
	if err != nil {
		t.Fatal(err)
	}
	b, err := New(Config{
		NodeAddress: "127.0.0.1:9000",
		Epoch: 9,
		QueueLimits: queue.Limits{MaxMessages: 8, MaxBytes: 1024},
		Topology: staticTopology,
	})
	if err != nil {
		t.Fatal(err)
	}
	from := udpAddr(t, "127.0.0.1", 10001)
	mustHandle(t, b, from, wire.MessageHello, wire.HelloBody{RequestID: 1, ClientID: "client-a", Role: wire.RoleBoth})

	outbound := mustHandle(t, b, from, wire.MessageCreateTopic, wire.CreateTopicBody{RequestID: 2, Topic: "score"})
	if len(outbound) != 1 {
		t.Fatalf("outbound length = %d, want 1", len(outbound))
	}
	h, body := decodeDatagram(t, outbound[0].Payload)
	if h.Type != wire.MessageRedirect {
		t.Fatalf("type = %s, want REDIRECT", h.Type)
	}
	redirect := body.(wire.RedirectBody)
	if redirect.Topic != "score" || redirect.BrokerAddr != "127.0.0.1:9001" || redirect.Epoch != 9 {
		t.Fatalf("redirect = %+v", redirect)
	}
}

func mustBroker(t *testing.T) *Broker {
	t.Helper()
	b, err := New(Config{NodeAddress: "127.0.0.1:9000", Epoch: 1, QueueLimits: queue.Limits{MaxMessages: 8, MaxBytes: 1024}})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return b
}

func mustHandle(t *testing.T, b *Broker, from *net.UDPAddr, messageType wire.MessageType, body any) []OutboundDatagram {
	t.Helper()
	encodedBody, err := wire.EncodeBody(messageType, body)
	if err != nil {
		t.Fatalf("EncodeBody() error = %v", err)
	}
	datagram, err := wire.EncodeDatagram(wire.Header{Version: wire.Version1, Type: messageType}, encodedBody)
	if err != nil {
		t.Fatalf("EncodeDatagram() error = %v", err)
	}
	outbound, err := b.HandleDatagram(datagram, from)
	if err != nil {
		t.Fatalf("HandleDatagram() error = %v", err)
	}
	return outbound
}

func decodeDatagram(t *testing.T, datagram []byte) (wire.Header, any) {
	t.Helper()
	h, bodyBytes, err := wire.DecodeDatagram(datagram)
	if err != nil {
		t.Fatalf("DecodeDatagram() error = %v", err)
	}
	body, err := wire.DecodeBody(h.Type, bodyBytes)
	if err != nil {
		t.Fatalf("DecodeBody() error = %v", err)
	}
	return h, body
}

func udpAddr(t *testing.T, ip string, port int) *net.UDPAddr {
	t.Helper()
	parsed := net.ParseIP(ip)
	if parsed == nil {
		t.Fatalf("invalid IP: %q", ip)
	}
	return &net.UDPAddr{IP: parsed, Port: port}
}
