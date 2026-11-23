package broker

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"pubsub/api"
	"pubsub/internal/admission"
	"pubsub/internal/degrade"
	"pubsub/internal/queue"
	"pubsub/internal/replication"
	"pubsub/internal/session"
	"pubsub/internal/topic"
	"pubsub/internal/topology"
	"pubsub/internal/wire"
)

var (
	ErrNilSourceAddress = errors.New("broker: source address is nil")
)

type Config struct {
	NodeAddress      string
	Epoch            uint64
	QueueLimits      queue.Limits
	Admission        admission.TokenBucketConfig
	DRRQuantum       uint32
	Degrade          degrade.Config
	Topology         *topology.Topology
	HeartbeatTimeout time.Duration
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
	publisherBuckets    map[string]*admission.Bucket
	drr                 *admission.DRR
	acceptedPublishes   uint64
	throttledPublishes  uint64
	topology            *topology.Topology
	heartbeat           *replication.Heartbeat
	replicationStore    *replication.Store
	promoted            bool
}

type subscriptionState struct {
	SessionID string
	Queue     *queue.Queue
	Degrade   *degrade.Monitor
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
	if cfg.Admission.RatePerSecond <= 0 {
		cfg.Admission.RatePerSecond = 100
	}
	if cfg.Admission.Burst <= 0 {
		cfg.Admission.Burst = 20
	}
	if cfg.DRRQuantum == 0 {
		cfg.DRRQuantum = 1024
	}
	if cfg.Degrade.PressureMessages <= 0 {
		cfg.Degrade.PressureMessages = max(1, cfg.QueueLimits.MaxMessages/2)
	}
	if cfg.Degrade.PressureBytes <= 0 {
		cfg.Degrade.PressureBytes = max(1, cfg.QueueLimits.MaxBytes/2)
	}
	if cfg.Degrade.PersistentFor <= 0 {
		cfg.Degrade.PersistentFor = 30 * time.Second
	}
	if cfg.HeartbeatTimeout <= 0 {
		cfg.HeartbeatTimeout = 5 * time.Second
	}
	heartbeat, err := replication.NewHeartbeat(cfg.HeartbeatTimeout)
	if err != nil {
		return nil, err
	}
	drr, err := admission.NewDRR(cfg.DRRQuantum)
	if err != nil {
		return nil, err
	}
	if cfg.Topology == nil {
		cfg.Topology, err = topology.New([]topology.Partition{{
			ID:          0,
			Epoch:       cfg.Epoch,
			PrimaryAddr: cfg.NodeAddress,
		}}, 1)
		if err != nil {
			return nil, err
		}
	}
	return &Broker{
		cfg:                 cfg,
		sessions:            session.NewManager(),
		topics:              topic.NewRegistry(),
		nextSequenceByTopic: make(map[topic.Name]uint64),
		subscriptions:       make(map[topic.SubscriptionID]subscriptionState),
		controlReplies:      make(map[controlReplyKey][]OutboundDatagram),
		publisherBuckets:    make(map[string]*admission.Bucket),
		drr:                 drr,
		throttledPublishes:  0,
		topology:            cfg.Topology,
		heartbeat:           heartbeat,
		replicationStore:    replication.NewStore(),
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
	case wire.MessageHeartbeat:
		outbound, err = b.handleHeartbeat(from, body.(wire.HeartbeatBody))
	case wire.MessageResume:
		outbound, err = b.handleResume(from, body.(wire.ResumeBody))
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
	if outbound, redirected := b.redirectIfNotPrimary(from, body.Topic, body.RequestID); redirected {
		return outbound, nil
	}

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
	if outbound, redirected := b.redirectIfNotPrimary(from, body.Topic, body.RequestID); redirected {
		return outbound, nil
	}

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
	if outbound, redirected := b.redirectIfNotPrimary(from, body.Topic, body.RequestID); redirected {
		return outbound, nil
	}

	s, ok := b.sessions.GetByAddress(from)
	if !ok {
		return []OutboundDatagram{b.protocolError(from, body.RequestID, api.ErrorSessionNotFound, "session not established; send HELLO first")}, nil
	}

	q, err := queue.New(b.cfg.QueueLimits)
	if err != nil {
		return nil, err
	}
	monitor, err := degrade.New(b.cfg.Degrade)
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
	b.subscriptions[topic.SubscriptionID(body.SubscriptionID)] = subscriptionState{SessionID: s.ID, Queue: q, Degrade: monitor}
	return []OutboundDatagram{b.controlAck(from, body.RequestID, operationSubscribe, s.ID)}, nil
}

func (b *Broker) handleUnsubscribe(from *net.UDPAddr, body wire.UnsubscribeBody) ([]OutboundDatagram, error) {
	if outbound, redirected := b.redirectIfNotPrimary(from, body.Topic, body.RequestID); redirected {
		return outbound, nil
	}

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
	if outbound, redirected := b.redirectIfNotPrimary(from, body.Topic, body.RequestID); redirected {
		return outbound, nil
	}

	sessionInfo, ok := b.sessions.GetByAddress(from)
	if !ok {
		return []OutboundDatagram{b.protocolError(from, body.RequestID, api.ErrorSessionNotFound, "session not established; send HELLO first")}, nil
	}

	publisherID := body.PublisherID
	if publisherID == "" {
		publisherID = sessionInfo.ClientID
	}
	bucket, exists := b.publisherBuckets[publisherID]
	if !exists {
		var err error
		bucket, err = admission.NewBucket(b.cfg.Admission, time.Now().UTC())
		if err != nil {
			return nil, err
		}
		b.publisherBuckets[publisherID] = bucket
		b.drr.Add(publisherID)
	}
	if !bucket.Allow(time.Now().UTC()) {
		b.throttledPublishes++
		throttleFrame, err := encodeFrame(wire.MessageThrottle, wire.ThrottleBody{
			Topic:            body.Topic,
			RetryAfterMillis: 100,
			Reason:           "publisher rate exceeded",
		})
		if err != nil {
			return nil, err
		}
		return []OutboundDatagram{{To: copyAddr(from), Payload: throttleFrame}}, nil
	}
	b.acceptedPublishes++
	b.drr.Consume(publisherID, max(uint32(1), uint32(len(body.Payload))))

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

		_, enqueueErr := state.Queue.Enqueue(queue.Item{Sequence: sequence, Payload: body.Payload})
		if enqueueErr == nil && state.Degrade != nil {
			stats := state.Queue.Stats()
			level := state.Degrade.Observe(stats.QueuedMessages, stats.QueuedBytes, time.Now().UTC())
			if level == degrade.LevelPersistent {
				_, _ = b.topics.Unsubscribe(topicName, subscriberID)
				delete(b.subscriptions, subscriberID)
				errorFrame, errorErr := encodeFrame(wire.MessageError, wire.ErrorBody{
					RequestID: body.RequestID,
					Code:      uint16(api.ErrorUnavailable),
					Message:   "subscriber disconnected due to persistent queue pressure",
				})
				if errorErr == nil {
					outbound = append(outbound, OutboundDatagram{To: sessionInfo.Address, Payload: errorFrame})
				}
				continue
			}
		}
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

func (b *Broker) handleHeartbeat(from *net.UDPAddr, body wire.HeartbeatBody) ([]OutboundDatagram, error) {
	b.heartbeat.Mark(time.Now().UTC())
	frame, err := encodeFrame(wire.MessageHeartbeat, wire.HeartbeatBody{
		SessionID:      body.SessionID,
		SentAtUnixNano: time.Now().UTC().UnixNano(),
	})
	if err != nil {
		return nil, err
	}
	return []OutboundDatagram{{To: copyAddr(from), Payload: frame}}, nil
}

func (b *Broker) handleResume(from *net.UDPAddr, body wire.ResumeBody) ([]OutboundDatagram, error) {
	sessionInfo, exists := b.sessions.GetByID(body.SessionID)
	if !exists {
		return []OutboundDatagram{b.protocolError(from, body.RequestID, api.ErrorSessionNotFound, "session cannot be resumed")}, nil
	}
	if _, err := b.sessions.Upsert(sessionInfo.ClientID, sessionInfo.Role, from, time.Now().UTC()); err != nil {
		return []OutboundDatagram{b.protocolError(from, body.RequestID, api.ErrorInternal, err.Error())}, nil
	}

	outbound := make([]OutboundDatagram, 0, len(body.Cursors)+1)
	for _, cursor := range body.Cursors {
		current := b.nextSequenceByTopic[topic.Name(cursor.Topic)]
		if current <= cursor.LastSequence {
			continue
		}
		subscribers, err := b.topics.Subscribers(topic.Name(cursor.Topic))
		if err != nil {
			continue
		}
		for _, subscriptionID := range subscribers {
			gapFrame, err := encodeFrame(wire.MessageGap, wire.GapBody{
				SubscriptionID: string(subscriptionID),
				Topic:          cursor.Topic,
				From:           cursor.LastSequence + 1,
				To:             current,
			})
			if err != nil {
				continue
			}
			outbound = append(outbound, OutboundDatagram{To: copyAddr(from), Payload: gapFrame})
		}
	}
	outbound = append(outbound, b.controlAck(from, body.RequestID, operationHello, body.SessionID))
	return outbound, nil
}

// ControlSnapshot returns the minimum control state for asynchronous replication.
func (b *Broker) ControlSnapshot() replication.ControlSnapshot {
	b.mu.Lock()
	defer b.mu.Unlock()
	return replication.ControlSnapshot{
		Epoch:        b.cfg.Epoch,
		TopicState:   b.topics.Snapshot(),
		SessionState: b.sessions.Snapshot(),
	}
}

// ApplyControlSnapshot restores replicated control state into this broker.
func (b *Broker) ApplyControlSnapshot(snapshot replication.ControlSnapshot) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if topicSnapshot, ok := snapshot.TopicState.(topic.Snapshot); ok {
		b.topics.Restore(topicSnapshot)
	}
	if sessionSnapshot, ok := snapshot.SessionState.([]session.Session); ok {
		b.sessions.Restore(sessionSnapshot)
	}
	if snapshot.Epoch > b.cfg.Epoch {
		b.cfg.Epoch = snapshot.Epoch
	}
}

// PromotePartition performs one-time failover promotion for the broker partition.
func (b *Broker) PromotePartition(now time.Time) (bool, uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if !b.heartbeat.Suspect(now) {
		return false, b.cfg.Epoch
	}
	if !b.heartbeat.Promote() {
		return false, b.cfg.Epoch
	}
	b.cfg.Epoch++
	b.promoted = true
	return true, b.cfg.Epoch
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

func (b *Broker) redirectIfNotPrimary(from *net.UDPAddr, topicName string, requestID uint64) ([]OutboundDatagram, bool) {
	if b.topology == nil {
		return nil, false
	}
	partition, err := b.topology.PartitionFor(topicName)
	if err != nil {
		return nil, false
	}
	if partition.PrimaryAddr == b.cfg.NodeAddress {
		return nil, false
	}
	frame, err := encodeFrame(wire.MessageRedirect, wire.RedirectBody{
		Topic:      topicName,
		BrokerAddr: partition.PrimaryAddr,
		Epoch:      partition.Epoch,
	})
	if err != nil {
		return []OutboundDatagram{b.protocolError(from, requestID, api.ErrorInternal, err.Error())}, true
	}
	return []OutboundDatagram{{To: copyAddr(from), Payload: frame}}, true
}

type Metrics struct {
	Publishers      int
	Subscriptions   int
	Accepted        uint64
	Throttled       uint64
	DroppedMessages uint64
	DroppedBytes    uint64
}

func (b *Broker) Metrics() Metrics {
	b.mu.Lock()
	defer b.mu.Unlock()
	metrics := Metrics{
		Publishers:    len(b.publisherBuckets),
		Subscriptions: len(b.subscriptions),
		Accepted:      b.acceptedPublishes,
		Throttled:     b.throttledPublishes,
	}
	for _, state := range b.subscriptions {
		stats := state.Queue.Stats()
		metrics.DroppedMessages += stats.DroppedMessages
		metrics.DroppedBytes += stats.DroppedBytes
	}
	return metrics
}
