package client

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"pubsub/api"
	"pubsub/internal/transport"
	"pubsub/internal/wire"
)

var (
	ErrClosed              = errors.New("client: closed")
	ErrInvalidOptions      = errors.New("client: invalid options")
	ErrControlTimeout      = errors.New("client: control operation timed out")
	ErrInvalidTopic        = errors.New("client: invalid topic")
	ErrInvalidSubscription = errors.New("client: invalid subscription")
)

const defaultControlTimeout = 750 * time.Millisecond
const defaultControlRetries = 2

type wireResponse struct {
	Header wire.Header
	Body   any
}

type subscription struct {
	Topic    api.Topic
	Callback api.DeliverCallback
}

// Client is the public, transport-independent usage facade for one session.
type Client struct {
	mu sync.Mutex

	udp       *transport.UDP
	broker    *net.UDPAddr
	options   api.Options
	sessionID api.SessionID
	epoch     api.Epoch
	closed    bool

	nextRequestID uint64
	nextSubID     uint64

	controlResults  map[uint64]chan wireResponse
	publishResults  map[uint64]chan wire.PubResultBody
	redirectResults chan wire.RedirectBody
	subscriptions   map[api.SubscriptionID]subscription
	seenSequences   map[api.Topic]uint64

	callbacks    chan func()
	callbackDone chan struct{}
	receiveDone  chan struct{}
}

// Connect opens a UDP socket, sends HELLO, and waits for WELCOME.
func Connect(options api.Options) (*Client, error) {
	if options.BrokerAddress == "" {
		return nil, fmt.Errorf("%w: broker address is empty", ErrInvalidOptions)
	}
	if options.ClientID == "" {
		return nil, fmt.Errorf("%w: client ID is empty", ErrInvalidOptions)
	}
	if options.Role != api.RoleConsumer && options.Role != api.RolePublisher && options.Role != api.RoleBoth {
		return nil, fmt.Errorf("%w: invalid role %d", ErrInvalidOptions, options.Role)
	}
	if options.ControlTimeout <= 0 {
		options.ControlTimeout = defaultControlTimeout
	}
	if options.ControlRetries < 0 {
		options.ControlRetries = 0
	} else if options.ControlRetries == 0 {
		options.ControlRetries = defaultControlRetries
	}

	broker, err := net.ResolveUDPAddr("udp", options.BrokerAddress)
	if err != nil {
		return nil, fmt.Errorf("client: resolve broker address: %w", err)
	}
	udp, err := transport.Listen("127.0.0.1:0")
	if err != nil {
		return nil, err
	}

	c := &Client{
		udp:             udp,
		broker:          broker,
		options:         options,
		controlResults:  make(map[uint64]chan wireResponse),
		publishResults:  make(map[uint64]chan wire.PubResultBody),
		redirectResults: make(chan wire.RedirectBody, 8),
		subscriptions:   make(map[api.SubscriptionID]subscription),
		seenSequences:   make(map[api.Topic]uint64),
		callbacks:       make(chan func(), 128),
		callbackDone:    make(chan struct{}),
		receiveDone:     make(chan struct{}),
	}
	go c.callbackWorker()
	go c.receiveLoop()

	requestID := c.nextRequest()
	header, err := c.controlRequest(wire.MessageHello, wire.HelloBody{
		RequestID: requestID,
		ClientID:  string(options.ClientID),
		Role:      roleToWire(options.Role),
	}, wire.MessageWelcome)
	if err != nil {
		_ = c.Close()
		return nil, err
	}
	welcome, ok := header.Body.(wire.WelcomeBody)
	if !ok {
		_ = c.Close()
		return nil, fmt.Errorf("client: HELLO returned unexpected response %T", header.Body)
	}
	c.mu.Lock()
	c.sessionID = api.SessionID(welcome.SessionID)
	c.epoch = api.Epoch(welcome.Epoch)
	c.mu.Unlock()
	return c, nil
}

// Publish sends one ephemeral event and waits for PUB_RESULT only.
func (c *Client) Publish(topic api.Topic, payload []byte) (api.PublishResult, error) {
	if c == nil || c.closed {
		return api.PublishResult{}, ErrClosed
	}
	if topic == "" {
		return api.PublishResult{}, ErrInvalidTopic
	}
	requestID := c.nextRequest()
	body := wire.PublishBody{
		RequestID:           requestID,
		Topic:               string(topic),
		PublisherID:         string(c.options.ClientID),
		PublishedAtUnixNano: time.Now().UTC().UnixNano(),
		Payload:             append([]byte(nil), payload...),
	}
	frame, err := encodeFrame(wire.MessagePublish, body)
	if err != nil {
		return api.PublishResult{}, err
	}

	channel := make(chan wire.PubResultBody, 1)
	c.mu.Lock()
	c.publishResults[requestID] = channel
	c.mu.Unlock()
	defer c.removePublishResult(requestID)

	c.mu.Lock()
	broker := cloneUDPAddr(c.broker)
	c.mu.Unlock()
	if err := c.udp.SendTo(frame, broker); err != nil {
		return api.PublishResult{}, err
	}

	select {
	case result := <-channel:
		return api.PublishResult{Outcome: api.PublishOutcome(result.Outcome), SubscriberCount: result.SubscriberCount}, nil
	case <-c.redirectResults:
		return api.PublishResult{}, ErrControlTimeout
	case <-time.After(c.options.ControlTimeout):
		return api.PublishResult{}, ErrControlTimeout
	}
}

// CreateTopic creates or confirms a dynamic topic.
func (c *Client) CreateTopic(topic api.Topic) error {
	if c == nil || c.closed {
		return ErrClosed
	}
	if topic == "" {
		return ErrInvalidTopic
	}
	requestID := c.nextRequest()
	_, err := c.controlRequest(wire.MessageCreateTopic, wire.CreateTopicBody{RequestID: requestID, Topic: string(topic)}, wire.MessageControlAck)
	return err
}

// DeleteTopic deletes a topic and receives CONTROL_ACK before return.
func (c *Client) DeleteTopic(topic api.Topic) error {
	if c == nil || c.closed {
		return ErrClosed
	}
	if topic == "" {
		return ErrInvalidTopic
	}
	requestID := c.nextRequest()
	_, err := c.controlRequest(wire.MessageDeleteTopic, wire.DeleteTopicBody{RequestID: requestID, Topic: string(topic)}, wire.MessageControlAck)
	return err
}

// Subscribe registers one subscription callback for topic deliveries.
func (c *Client) Subscribe(topic api.Topic, callback api.DeliverCallback) (api.SubscriptionID, error) {
	if c == nil || c.closed {
		return "", ErrClosed
	}
	if topic == "" {
		return "", ErrInvalidTopic
	}
	c.mu.Lock()
	c.nextSubID++
	id := api.SubscriptionID(fmt.Sprintf("sub-%d", c.nextSubID))
	c.subscriptions[id] = subscription{Topic: topic, Callback: callback}
	c.mu.Unlock()

	requestID := c.nextRequest()
	_, err := c.controlRequest(wire.MessageSubscribe, wire.SubscribeBody{RequestID: requestID, SubscriptionID: string(id), Topic: string(topic)}, wire.MessageControlAck)
	if err != nil {
		c.mu.Lock()
		delete(c.subscriptions, id)
		c.mu.Unlock()
		return "", err
	}
	return id, nil
}

// Unsubscribe removes one subscription and waits for CONTROL_ACK.
func (c *Client) Unsubscribe(topic api.Topic, id api.SubscriptionID) error {
	if c == nil || c.closed {
		return ErrClosed
	}
	if topic == "" {
		return ErrInvalidTopic
	}
	if id == "" {
		return ErrInvalidSubscription
	}
	requestID := c.nextRequest()
	_, err := c.controlRequest(wire.MessageUnsubscribe, wire.UnsubscribeBody{RequestID: requestID, SubscriptionID: string(id), Topic: string(topic)}, wire.MessageControlAck)
	if err == nil {
		c.mu.Lock()
		delete(c.subscriptions, id)
		c.mu.Unlock()
	}
	return err
}

// Resume asks the broker to continue a session from the last known topic sequences.
func (c *Client) Resume(cursors map[api.Topic]uint64) error {
	if c == nil || c.closed {
		return ErrClosed
	}
	requestID := c.nextRequest()
	resumeCursors := make([]wire.ResumeCursor, 0, len(cursors))
	for topic, sequence := range cursors {
		resumeCursors = append(resumeCursors, wire.ResumeCursor{Topic: string(topic), LastSequence: sequence})
	}
	_, err := c.controlRequest(wire.MessageResume, wire.ResumeBody{
		RequestID: requestID,
		SessionID: string(c.SessionID()),
		Cursors:   resumeCursors,
	}, wire.MessageControlAck)
	return err
}

// Close closes UDP and callback workers and releases the receive loop.
func (c *Client) Close() error {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	c.mu.Unlock()

	err := c.udp.Close()
	<-c.receiveDone
	close(c.callbacks)
	<-c.callbackDone
	return err
}

// SessionID returns the broker-confirmed session identifier.
func (c *Client) SessionID() api.SessionID {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sessionID
}

// Epoch returns the highest epoch accepted from WELCOME or REDIRECT.
func (c *Client) Epoch() api.Epoch {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.epoch
}

func (c *Client) nextRequest() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nextRequestID++
	return c.nextRequestID
}

func (c *Client) controlRequest(messageType wire.MessageType, body any, expected wire.MessageType) (wireResponse, error) {
	requestID, ok := controlRequestID(body)
	if !ok {
		return wireResponse{}, fmt.Errorf("client: request body does not expose RequestID")
	}
	frame, err := encodeFrame(messageType, body)
	if err != nil {
		return wireResponse{}, err
	}

	channel := make(chan wireResponse, 1)
	c.mu.Lock()
	c.controlResults[requestID] = channel
	c.mu.Unlock()
	defer c.removeControlResult(requestID)

	attempts := c.options.ControlRetries + 1
	for attempt := 0; attempt < attempts; attempt++ {
		c.mu.Lock()
		broker := cloneUDPAddr(c.broker)
		c.mu.Unlock()
		if err := c.udp.SendTo(frame, broker); err != nil {
			return wireResponse{}, err
		}
		select {
		case response := <-channel:
			if response.Header.Type == wire.MessageError {
				body := response.Body.(wire.ErrorBody)
				return wireResponse{}, api.Error{Code: api.ErrorCode(body.Code), Message: body.Message}
			}
			if response.Header.Type != expected {
				return wireResponse{}, fmt.Errorf("client: expected %s, got %s", expected, response.Header.Type)
			}
			return response, nil
		case <-c.redirectResults:
			continue
		case <-time.After(c.options.ControlTimeout):
			continue
		}
	}
	return wireResponse{}, ErrControlTimeout
}

func (c *Client) receiveLoop() {
	defer close(c.receiveDone)
	for {
		datagram, err := c.udp.Receive()
		if err != nil {
			if !c.isClosed() {
				c.emitError(api.Error{Code: api.ErrorUnavailable, Message: err.Error()})
			}
			return
		}
		header, bodyBytes, err := wire.DecodeDatagram(datagram.Payload)
		if err != nil {
			c.emitError(api.Error{Code: api.ErrorInvalidRequest, Message: err.Error()})
			continue
		}
		body, err := wire.DecodeBody(header.Type, bodyBytes)
		if err != nil {
			c.emitError(api.Error{Code: api.ErrorInvalidRequest, Message: err.Error()})
			continue
		}
		response := wireResponse{Header: header, Body: body}
		c.dispatch(response)
	}
}

func (c *Client) dispatch(response wireResponse) {
	switch response.Header.Type {
	case wire.MessageWelcome:
		body := response.Body.(wire.WelcomeBody)
		c.deliverControl(body.RequestID, response)
	case wire.MessageControlAck:
		body := response.Body.(wire.ControlAckBody)
		c.deliverControl(body.RequestID, response)
	case wire.MessageError:
		body := response.Body.(wire.ErrorBody)
		c.deliverControl(body.RequestID, response)
	case wire.MessagePubResult:
		body := response.Body.(wire.PubResultBody)
		c.mu.Lock()
		channel, exists := c.publishResults[body.RequestID]
		c.mu.Unlock()
		if exists {
			channel <- body
		}
	case wire.MessageDeliver:
		body := response.Body.(wire.DeliverBody)
		c.dispatchDeliver(body)
	case wire.MessageGap:
		body := response.Body.(wire.GapBody)
		if c.options.OnGap != nil {
			copyGap := api.Gap{Topic: api.Topic(body.Topic), From: body.From, To: body.To}
			c.enqueueCallback(func() { c.options.OnGap(copyGap) })
		}
	case wire.MessageTopicClosed:
		body := response.Body.(wire.TopicClosedBody)
		if c.options.OnTopicClosed != nil {
			closedTopic := api.Topic(body.Topic)
			c.enqueueCallback(func() { c.options.OnTopicClosed(closedTopic) })
		}
	case wire.MessageThrottle:
		body := response.Body.(wire.ThrottleBody)
		if c.options.OnThrottle != nil {
			throttledTopic := api.Topic(body.Topic)
			retry := body.RetryAfterMillis
			c.enqueueCallback(func() { c.options.OnThrottle(throttledTopic, retry) })
		}
	case wire.MessageHeartbeat:
		body := response.Body.(wire.HeartbeatBody)
		frame, err := encodeFrame(wire.MessageHeartbeat, wire.HeartbeatBody{SessionID: body.SessionID, SentAtUnixNano: time.Now().UTC().UnixNano()})
		if err == nil {
			c.mu.Lock()
			broker := cloneUDPAddr(c.broker)
			c.mu.Unlock()
			_ = c.udp.SendTo(frame, broker)
		}
	case wire.MessageRedirect:
		body := response.Body.(wire.RedirectBody)
		c.mu.Lock()
		if api.Epoch(body.Epoch) >= c.epoch {
			address, err := net.ResolveUDPAddr("udp", body.BrokerAddr)
			if err == nil {
				c.broker = address
				c.epoch = api.Epoch(body.Epoch)
			}
		}
		c.mu.Unlock()
		select {
		case c.redirectResults <- body:
		default:
		}
	}
}

func (c *Client) dispatchDeliver(body wire.DeliverBody) {
	topic := api.Topic(body.Topic)
	sequence := body.Sequence

	c.mu.Lock()
	last, seen := c.seenSequences[topic]
	if seen && sequence <= last {
		c.mu.Unlock()
		return
	}
	if seen && sequence > last+1 {
		gap := api.Gap{Topic: topic, From: last + 1, To: sequence - 1}
		c.mu.Unlock()
		if c.options.OnGap != nil {
			c.enqueueCallback(func() { c.options.OnGap(gap) })
		}
	} else {
		c.mu.Unlock()
	}
	c.mu.Lock()
	c.seenSequences[topic] = sequence
	subscription, exists := c.subscriptions[api.SubscriptionID(body.SubscriptionID)]
	callback := subscription.Callback
	c.mu.Unlock()

	if !exists || callback == nil {
		return
	}
	message := api.Message{
		Topic:       topic,
		PublisherID: api.ClientID(body.PublisherID),
		Sequence:    sequence,
		PublishedAt: time.Unix(0, body.PublishedAtUnixNano).UTC(),
		Payload:     append([]byte(nil), body.Payload...),
	}
	c.enqueueCallback(func() { callback(message) })
}

func (c *Client) deliverControl(requestID uint64, response wireResponse) {
	c.mu.Lock()
	channel, exists := c.controlResults[requestID]
	c.mu.Unlock()
	if exists {
		channel <- response
	}
}

func controlRequestID(body any) (uint64, bool) {
	switch body := body.(type) {
	case wire.HelloBody:
		return body.RequestID, true
	case wire.CreateTopicBody:
		return body.RequestID, true
	case wire.DeleteTopicBody:
		return body.RequestID, true
	case wire.SubscribeBody:
		return body.RequestID, true
	case wire.UnsubscribeBody:
		return body.RequestID, true
	case wire.ResumeBody:
		return body.RequestID, true
	default:
		return 0, false
	}
}

func encodeFrame(messageType wire.MessageType, body any) ([]byte, error) {
	encodedBody, err := wire.EncodeBody(messageType, body)
	if err != nil {
		return nil, err
	}
	return wire.EncodeDatagram(wire.Header{Version: wire.Version1, Type: messageType}, encodedBody)
}

func roleToWire(role api.Role) uint8 {
	return uint8(role)
}

func (c *Client) removeControlResult(requestID uint64) {
	c.mu.Lock()
	delete(c.controlResults, requestID)
	c.mu.Unlock()
}

func (c *Client) removePublishResult(requestID uint64) {
	c.mu.Lock()
	delete(c.publishResults, requestID)
	c.mu.Unlock()
}

func (c *Client) isClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

func (c *Client) emitError(err api.Error) {
	if c.options.OnError != nil {
		c.enqueueCallback(func() { c.options.OnError(err) })
	}
}

func (c *Client) enqueueCallback(callback func()) {
	select {
	case c.callbacks <- callback:
	case <-c.callbackDone:
	}
}

func (c *Client) callbackWorker() {
	defer close(c.callbackDone)
	for callback := range c.callbacks {
		callback()
	}
}

func cloneUDPAddr(address *net.UDPAddr) *net.UDPAddr {
	if address == nil {
		return nil
	}
	copy := *address
	copy.IP = append(net.IP(nil), address.IP...)
	return &copy
}
