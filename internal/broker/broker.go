package broker

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"pubsub/api"
	"pubsub/internal/queue"
	"pubsub/internal/session"
	"pubsub/internal/topic"
	"pubsub/internal/wire"
)

var (
	ErrNilSourceAddress = errors.New("broker: source address is nil")
)

type Config struct {
	NodeAddress string
	Epoch       uint64
	QueueLimits queue.Limits
}

type OutboundDatagram struct {
	To      *net.UDPAddr
	Payload []byte
}

type Broker struct {
	mu sync.Mutex

	cfg Config

	sessions *session.Manager
	topics   *topic.Registry

	nextSequenceByTopic map[topic.Name]uint64
	subscriptions       map[topic.SubscriptionID]subscriptionState
	controlReplies      map[controlReplyKey][]OutboundDatagram
}

type subscriptionState struct {
	SessionID string
	Queue     *queue.Queue
}

type controlReplyKey struct {
	Address     string
	RequestID   uint64
	MessageType wire.MessageType
}

const (
	operationHello     uint16 = 1
	operationCreate    uint16 = 2
	operationDelete    uint16 = 3
	operationSubscribe uint16 = 4
	operationUnsub     uint16 = 5
)

func New(cfg Config) (*Broker, error) {
	if cfg.QueueLimits.MaxMessages <= 0 || cfg.QueueLimits.MaxBytes <= 0 {
		return nil, fmt.Errorf("broker: invalid queue limits: %+v", cfg.QueueLimits)
	}
	return &Broker{
		cfg:                 cfg,
		sessions:            session.NewManager(),
		topics:              topic.NewRegistry(),
		nextSequenceByTopic: make(map[topic.Name]uint64),
		subscriptions:       make(map[topic.SubscriptionID]subscriptionState),
		controlReplies:      make(map[controlReplyKey][]OutboundDatagram),
	}, nil
}

// HandleDatagram processes one incoming datagram and returns outgoing datagrams.
func (b *Broker) HandleDatagram(payload []byte, from *net.UDPAddr) ([]OutboundDatagram, error) {
	if from == nil {
		return nil, ErrNilSourceAddress
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	header, bodyBytes, err := wire.DecodeDatagram(payload)
	if err != nil {
		return []OutboundDatagram{b.protocolError(from, 0, api.ErrorInvalidRequest, err.Error())}, nil
	}

	body, err := wire.DecodeBody(header.Type, bodyBytes)
	if err != nil {
		return []OutboundDatagram{b.protocolError(from, 0, api.ErrorInvalidRequest, err.Error())}, nil
	}

	requestID, isControl := controlRequestID(header.Type, body)
	if isControl {
		key := controlReplyKey{Address: from.String(), RequestID: requestID, MessageType: header.Type}
		if cached, exists := b.controlReplies[key]; exists {
			return copyOutbound(cached), nil
		}
	}

	var outbound []OutboundDatagram
	switch header.Type {
	case wire.MessageHello:
		outbound, err = b.handleHello(from, body.(wire.HelloBody))
	case wire.MessageCreateTopic:
		outbound, err = b.handleCreateTopic(from, body.(wire.CreateTopicBody))
	case wire.MessageDeleteTopic:
		outbound, err = b.handleDeleteTopic(from, body.(wire.DeleteTopicBody))
	case wire.MessageSubscribe:
		outbound, err = b.handleSubscribe(from, body.(wire.SubscribeBody))
	case wire.MessageUnsubscribe:
		outbound, err = b.handleUnsubscribe(from, body.(wire.UnsubscribeBody))
	case wire.MessagePublish:
		outbound, err = b.handlePublish(from, body.(wire.PublishBody))
	default:
		outbound = []OutboundDatagram{b.protocolError(from, 0, api.ErrorInvalidRequest, "message type not supported by single-node skeleton")}
	}

	if err == nil && isControl {
		key := controlReplyKey{Address: from.String(), RequestID: requestID, MessageType: header.Type}
		b.controlReplies[key] = copyOutbound(outbound)
	}
	return outbound, err
}

func (b *Broker) handleHello(from *net.UDPAddr, body wire.HelloBody) ([]OutboundDatagram, error) {
	s, err := b.sessions.Upsert(body.ClientID, body.Role, from, time.Now().UTC())
	if err != nil {
		return []OutboundDatagram{b.protocolError(from, body.RequestID, api.ErrorInvalidRequest, err.Error())}, nil
	}

	welcome := wire.WelcomeBody{
		RequestID:       body.RequestID,
		SessionID:       s.ID,
		Epoch:           b.cfg.Epoch,
		TopologyVersion: 1,
		PrimaryAddr:     b.cfg.NodeAddress,
	}
	frame, err := encodeFrame(wire.MessageWelcome, welcome)
	if err != nil {
		return nil, err
	}
	return []OutboundDatagram{{To: copyAddr(from), Payload: frame}}, nil
}

func (b *Broker) handleCreateTopic(from *net.UDPAddr, body wire.CreateTopicBody) ([]OutboundDatagram, error) {
	s, ok := b.sessions.GetByAddress(from)
	if !ok {
		return []OutboundDatagram{b.protocolError(from, body.RequestID, api.ErrorSessionNotFound, "session not established; send HELLO first")}, nil
	}
	if _, err := b.topics.Create(topic.Name(body.Topic)); err != nil {
		return []OutboundDatagram{b.protocolError(from, body.RequestID, api.ErrorInvalidRequest, err.Error())}, nil
	}
	return []OutboundDatagram{b.controlAck(from, body.RequestID, operationCreate, s.ID)}, nil
}

func (b *Broker) handleDeleteTopic(from *net.UDPAddr, body wire.DeleteTopicBody) ([]OutboundDatagram, error) {
	s, ok := b.sessions.GetByAddress(from)
	if !ok {
		return []OutboundDatagram{b.protocolError(from, body.RequestID, api.ErrorSessionNotFound, "session not established; send HELLO first")}, nil
	}
	result, err := b.topics.Delete(topic.Name(body.Topic))
	if err != nil {
		return []OutboundDatagram{b.protocolError(from, body.RequestID, api.ErrorInvalidRequest, err.Error())}, nil
	}

	outbound := make([]OutboundDatagram, 0, len(result.Subscribers)+1)
	for _, id := range result.Subscribers {
		state, exists := b.subscriptions[id]
		if !exists {
			continue
		}
		sessionInfo, ok := b.sessions.GetByID(state.SessionID)
		if !ok {
			continue
		}
		closed := wire.TopicClosedBody{Topic: body.Topic, Reason: "topic deleted"}
		frame, frameErr := encodeFrame(wire.MessageTopicClosed, closed)
		if frameErr != nil {
			return nil, frameErr
		}
		outbound = append(outbound, OutboundDatagram{To: sessionInfo.Address, Payload: frame})
		delete(b.subscriptions, id)
	}
	outbound = append(outbound, b.controlAck(from, body.RequestID, operationDelete, s.ID))
	return outbound, nil
}

func (b *Broker) handleSubscribe(from *net.UDPAddr, body wire.SubscribeBody) ([]OutboundDatagram, error) {
	s, ok := b.sessions.GetByAddress(from)
	if !ok {
		return []OutboundDatagram{b.protocolError(from, body.RequestID, api.ErrorSessionNotFound, "session not established; send HELLO first")}, nil
	}

	q, err := queue.New(b.cfg.QueueLimits)
	if err != nil {
		return nil, err
	}
	_, err = b.topics.Subscribe(topic.Name(body.Topic), topic.SubscriptionID(body.SubscriptionID))
	if err != nil {
		if errors.Is(err, topic.ErrTopicNotFound) {
			return []OutboundDatagram{b.protocolError(from, body.RequestID, api.ErrorTopicNotFound, err.Error())}, nil
		}
		return []OutboundDatagram{b.protocolError(from, body.RequestID, api.ErrorInvalidRequest, err.Error())}, nil
	}
	b.subscriptions[topic.SubscriptionID(body.SubscriptionID)] = subscriptionState{SessionID: s.ID, Queue: q}
	return []OutboundDatagram{b.controlAck(from, body.RequestID, operationSubscribe, s.ID)}, nil
}

func (b *Broker) handleUnsubscribe(from *net.UDPAddr, body wire.UnsubscribeBody) ([]OutboundDatagram, error) {
	s, ok := b.sessions.GetByAddress(from)
	if !ok {
		return []OutboundDatagram{b.protocolError(from, body.RequestID, api.ErrorSessionNotFound, "session not established; send HELLO first")}, nil
	}
	_, err := b.topics.Unsubscribe(topic.Name(body.Topic), topic.SubscriptionID(body.SubscriptionID))
	if err != nil {
		if errors.Is(err, topic.ErrTopicNotFound) {
			return []OutboundDatagram{b.protocolError(from, body.RequestID, api.ErrorTopicNotFound, err.Error())}, nil
		}
		return []OutboundDatagram{b.protocolError(from, body.RequestID, api.ErrorInvalidRequest, err.Error())}, nil
	}
	delete(b.subscriptions, topic.SubscriptionID(body.SubscriptionID))
	return []OutboundDatagram{b.controlAck(from, body.RequestID, operationUnsub, s.ID)}, nil
}

func (b *Broker) handlePublish(from *net.UDPAddr, body wire.PublishBody) ([]OutboundDatagram, error) {
	if _, ok := b.sessions.GetByAddress(from); !ok {
		return []OutboundDatagram{b.protocolError(from, body.RequestID, api.ErrorSessionNotFound, "session not established; send HELLO first")}, nil
	}

	topicName := topic.Name(body.Topic)
	if !b.topics.Exists(topicName) {
		pubResult, err := encodeFrame(wire.MessagePubResult, wire.PubResultBody{RequestID: body.RequestID, Outcome: wire.PublishOutcomeTopicNotFound})
		if err != nil {
			return nil, err
		}
		return []OutboundDatagram{{To: copyAddr(from), Payload: pubResult}}, nil
	}

	subscribers, err := b.topics.Subscribers(topicName)
	if err != nil {
		return []OutboundDatagram{b.protocolError(from, body.RequestID, api.ErrorInternal, err.Error())}, nil
	}

	if len(subscribers) == 0 {
		pubResult, frameErr := encodeFrame(wire.MessagePubResult, wire.PubResultBody{RequestID: body.RequestID, Outcome: wire.PublishOutcomeNoSubscribers})
		if frameErr != nil {
			return nil, frameErr
		}
		return []OutboundDatagram{{To: copyAddr(from), Payload: pubResult}}, nil
	}

	b.nextSequenceByTopic[topicName]++
	sequence := b.nextSequenceByTopic[topicName]
	outbound := make([]OutboundDatagram, 0, len(subscribers)+1)

	pubResult, frameErr := encodeFrame(wire.MessagePubResult, wire.PubResultBody{RequestID: body.RequestID, Outcome: wire.PublishOutcomeDelivered, SubscriberCount: uint32(len(subscribers))})
	if frameErr != nil {
		return nil, frameErr
	}
	outbound = append(outbound, OutboundDatagram{To: copyAddr(from), Payload: pubResult})

	for _, subscriberID := range subscribers {
		state, exists := b.subscriptions[subscriberID]
		if !exists {
			continue
		}
		sessionInfo, ok := b.sessions.GetByID(state.SessionID)
		if !ok {
			continue
		}

		_, _ = state.Queue.Enqueue(queue.Item{Sequence: sequence, Payload: body.Payload})
		for {
			event, hasEvent := state.Queue.Next()
			if !hasEvent {
				break
			}
			switch event.Kind {
			case queue.EventGap:
				gapFrame, gapErr := encodeFrame(wire.MessageGap, wire.GapBody{
					SubscriptionID: string(subscriberID),
					Topic:          body.Topic,
					From:           event.Gap.From,
					To:             event.Gap.To,
				})
				if gapErr != nil {
					return nil, gapErr
				}
				outbound = append(outbound, OutboundDatagram{To: sessionInfo.Address, Payload: gapFrame})
			case queue.EventMessage:
				deliverFrame, deliverErr := encodeFrame(wire.MessageDeliver, wire.DeliverBody{
					SubscriptionID:      string(subscriberID),
					Topic:               body.Topic,
					PublisherID:         body.PublisherID,
					Sequence:            event.Item.Sequence,
					PublishedAtUnixNano: body.PublishedAtUnixNano,
					Payload:             event.Item.Payload,
				})
				if deliverErr != nil {
					return nil, deliverErr
				}
				outbound = append(outbound, OutboundDatagram{To: sessionInfo.Address, Payload: deliverFrame})
			}
		}
	}

	return outbound, nil
}

func (b *Broker) protocolError(to *net.UDPAddr, requestID uint64, code api.ErrorCode, message string) OutboundDatagram {
	frame, err := encodeFrame(wire.MessageError, wire.ErrorBody{RequestID: requestID, Code: uint16(code), Message: message})
	if err != nil {
		// Fallback keeps protocol response possible in severe encoding failures.
		fallback, _ := encodeFrame(wire.MessageError, wire.ErrorBody{RequestID: requestID, Code: uint16(api.ErrorInternal), Message: "failed to encode protocol error"})
		return OutboundDatagram{To: copyAddr(to), Payload: fallback}
	}
	return OutboundDatagram{To: copyAddr(to), Payload: frame}
}

func encodeFrame(messageType wire.MessageType, body any) ([]byte, error) {
	encodedBody, err := wire.EncodeBody(messageType, body)
	if err != nil {
		return nil, err
	}
	return wire.EncodeDatagram(wire.Header{Version: wire.Version1, Type: messageType}, encodedBody)
}

func copyAddr(address *net.UDPAddr) *net.UDPAddr {
	if address == nil {
		return nil
	}
	copy := *address
	copy.IP = append(net.IP(nil), address.IP...)
	return &copy
}

func controlRequestID(messageType wire.MessageType, body any) (uint64, bool) {
	switch messageType {
	case wire.MessageHello:
		return body.(wire.HelloBody).RequestID, true
	case wire.MessageCreateTopic:
		return body.(wire.CreateTopicBody).RequestID, true
	case wire.MessageDeleteTopic:
		return body.(wire.DeleteTopicBody).RequestID, true
	case wire.MessageSubscribe:
		return body.(wire.SubscribeBody).RequestID, true
	case wire.MessageUnsubscribe:
		return body.(wire.UnsubscribeBody).RequestID, true
	case wire.MessageResume:
		return body.(wire.ResumeBody).RequestID, true
	default:
		return 0, false
	}
}

func copyOutbound(source []OutboundDatagram) []OutboundDatagram {
	copied := make([]OutboundDatagram, len(source))
	for i, item := range source {
		copied[i] = OutboundDatagram{To: copyAddr(item.To), Payload: append([]byte(nil), item.Payload...)}
	}
	return copied
}

func (b *Broker) controlAck(to *net.UDPAddr, requestID uint64, operation uint16, sessionID string) OutboundDatagram {
	frame, err := encodeFrame(wire.MessageControlAck, wire.ControlAckBody{RequestID: requestID, Operation: operation, SessionID: sessionID})
	if err != nil {
		return b.protocolError(to, requestID, api.ErrorInternal, err.Error())
	}
	return OutboundDatagram{To: copyAddr(to), Payload: frame}
}
