package wire

import (
	"encoding/binary"
	"errors"
	"fmt"
)

const (
	maxStringLen = 0xffff
	maxBytesLen  = 0xffff
)

const (
	RoleConsumer  uint8 = 1
	RolePublisher uint8 = 2
	RoleBoth      uint8 = 3
)

const (
	PublishOutcomeDelivered     uint8 = 1
	PublishOutcomeNoSubscribers uint8 = 2
	PublishOutcomeTopicNotFound uint8 = 3
)

var (
	ErrInvalidBodyType  = errors.New("wire: invalid body type for message")
	ErrInvalidBody      = errors.New("wire: invalid body")
	ErrBodyTrailingData = errors.New("wire: body has trailing bytes")
	ErrStringTooLong    = errors.New("wire: string field exceeds maximum length")
	ErrBytesTooLong     = errors.New("wire: bytes field exceeds maximum length")
)

// HelloBody starts a client session negotiation.
type HelloBody struct {
	RequestID uint64
	ClientID  string
	Role      uint8
}

// WelcomeBody confirms session establishment and carries topology hints.
type WelcomeBody struct {
	RequestID       uint64
	SessionID       string
	Epoch           uint64
	TopologyVersion uint64
	PrimaryAddr     string
}

// CreateTopicBody requests dynamic topic creation.
type CreateTopicBody struct {
	RequestID uint64
	Topic     string
}

// DeleteTopicBody requests dynamic topic deletion.
type DeleteTopicBody struct {
	RequestID uint64
	Topic     string
}

// SubscribeBody subscribes one session to one topic.
type SubscribeBody struct {
	RequestID      uint64
	SubscriptionID string
	Topic          string
}

// UnsubscribeBody removes one subscription from one topic.
type UnsubscribeBody struct {
	RequestID      uint64
	SubscriptionID string
	Topic          string
}

// PublishBody carries one publication from a publisher.
type PublishBody struct {
	RequestID           uint64
	Topic               string
	PublisherID         string
	PublishedAtUnixNano int64
	Payload             []byte
}

// PubResultBody reports broker processing outcome for one publication.
type PubResultBody struct {
	RequestID       uint64
	Outcome         uint8
	SubscriberCount uint32
}

// DeliverBody carries one sequenced event to one subscriber.
type DeliverBody struct {
	SubscriptionID      string
	Topic               string
	PublisherID         string
	Sequence            uint64
	PublishedAtUnixNano int64
	Payload             []byte
}

// GapBody informs one sequence interval that could not be delivered.
type GapBody struct {
	SubscriptionID string
	Topic          string
	From           uint64
	To             uint64
}

// ResumeCursor carries one topic checkpoint during session resume.
type ResumeCursor struct {
	Topic        string
	LastSequence uint64
}

// ResumeBody asks a broker to continue a previous session.
type ResumeBody struct {
	RequestID uint64
	SessionID string
	Cursors   []ResumeCursor
}

// ThrottleBody asks a publisher to reduce sending rate.
type ThrottleBody struct {
	Topic            string
	RetryAfterMillis uint32
	Reason           string
}

// RedirectBody tells a client to refresh route and use another broker.
type RedirectBody struct {
	Topic      string
	BrokerAddr string
	Epoch      uint64
}

// HeartbeatBody supports timeout-based liveness checks.
type HeartbeatBody struct {
	SessionID      string
	SentAtUnixNano int64
}

// TopicClosedBody informs subscribers that a topic was removed.
type TopicClosedBody struct {
	Topic  string
	Reason string
}

// ErrorBody carries one protocol or control-plane error.
type ErrorBody struct {
	RequestID uint64
	Code      uint16
	Message   string
}

// ControlAckBody confirms one idempotent control-plane request.
type ControlAckBody struct {
	RequestID uint64
	Operation uint16
	SessionID string
}

// EncodeBody serializes a typed body according to messageType.
func EncodeBody(messageType MessageType, body any) ([]byte, error) {
	switch messageType {
	case MessageHello:
		typed, ok := body.(HelloBody)
		if !ok {
			return nil, bodyTypeError(messageType, body)
		}
		return encodeHelloBody(typed)
	case MessageWelcome:
		typed, ok := body.(WelcomeBody)
		if !ok {
			return nil, bodyTypeError(messageType, body)
		}
		return encodeWelcomeBody(typed)
	case MessageCreateTopic:
		typed, ok := body.(CreateTopicBody)
		if !ok {
			return nil, bodyTypeError(messageType, body)
		}
		return encodeCreateTopicBody(typed)
	case MessageDeleteTopic:
		typed, ok := body.(DeleteTopicBody)
		if !ok {
			return nil, bodyTypeError(messageType, body)
		}
		return encodeDeleteTopicBody(typed)
	case MessageSubscribe:
		typed, ok := body.(SubscribeBody)
		if !ok {
			return nil, bodyTypeError(messageType, body)
		}
		return encodeSubscribeBody(typed)
	case MessageUnsubscribe:
		typed, ok := body.(UnsubscribeBody)
		if !ok {
			return nil, bodyTypeError(messageType, body)
		}
		return encodeUnsubscribeBody(typed)
	case MessagePublish:
		typed, ok := body.(PublishBody)
		if !ok {
			return nil, bodyTypeError(messageType, body)
		}
		return encodePublishBody(typed)
	case MessagePubResult:
		typed, ok := body.(PubResultBody)
		if !ok {
			return nil, bodyTypeError(messageType, body)
		}
		return encodePubResultBody(typed)
	case MessageDeliver:
		typed, ok := body.(DeliverBody)
		if !ok {
			return nil, bodyTypeError(messageType, body)
		}
		return encodeDeliverBody(typed)
	case MessageGap:
		typed, ok := body.(GapBody)
		if !ok {
			return nil, bodyTypeError(messageType, body)
		}
		return encodeGapBody(typed)
	case MessageResume:
		typed, ok := body.(ResumeBody)
		if !ok {
			return nil, bodyTypeError(messageType, body)
		}
		return encodeResumeBody(typed)
	case MessageThrottle:
		typed, ok := body.(ThrottleBody)
		if !ok {
			return nil, bodyTypeError(messageType, body)
		}
		return encodeThrottleBody(typed)
	case MessageRedirect:
		typed, ok := body.(RedirectBody)
		if !ok {
			return nil, bodyTypeError(messageType, body)
		}
		return encodeRedirectBody(typed)
	case MessageHeartbeat:
		typed, ok := body.(HeartbeatBody)
		if !ok {
			return nil, bodyTypeError(messageType, body)
		}
		return encodeHeartbeatBody(typed)
	case MessageTopicClosed:
		typed, ok := body.(TopicClosedBody)
		if !ok {
			return nil, bodyTypeError(messageType, body)
		}
		return encodeTopicClosedBody(typed)
	case MessageError:
		typed, ok := body.(ErrorBody)
		if !ok {
			return nil, bodyTypeError(messageType, body)
		}
		return encodeErrorBody(typed)
	case MessageControlAck:
		typed, ok := body.(ControlAckBody)
		if !ok {
			return nil, bodyTypeError(messageType, body)
		}
		return encodeControlAckBody(typed)
	default:
		return nil, fmt.Errorf("%w: %d", ErrUnknownMessageType, messageType)
	}
}

// DecodeBody decodes one message body according to messageType.
func DecodeBody(messageType MessageType, body []byte) (any, error) {
	switch messageType {
	case MessageHello:
		return decodeHelloBody(body)
	case MessageWelcome:
		return decodeWelcomeBody(body)
	case MessageCreateTopic:
		return decodeCreateTopicBody(body)
	case MessageDeleteTopic:
		return decodeDeleteTopicBody(body)
	case MessageSubscribe:
		return decodeSubscribeBody(body)
	case MessageUnsubscribe:
		return decodeUnsubscribeBody(body)
	case MessagePublish:
		return decodePublishBody(body)
	case MessagePubResult:
		return decodePubResultBody(body)
	case MessageDeliver:
		return decodeDeliverBody(body)
	case MessageGap:
		return decodeGapBody(body)
	case MessageResume:
		return decodeResumeBody(body)
	case MessageThrottle:
		return decodeThrottleBody(body)
	case MessageRedirect:
		return decodeRedirectBody(body)
	case MessageHeartbeat:
		return decodeHeartbeatBody(body)
	case MessageTopicClosed:
		return decodeTopicClosedBody(body)
	case MessageError:
		return decodeErrorBody(body)
	case MessageControlAck:
		return decodeControlAckBody(body)
	default:
		return nil, fmt.Errorf("%w: %d", ErrUnknownMessageType, messageType)
	}
}

func encodeHelloBody(body HelloBody) ([]byte, error) {
	if err := validateRole(body.Role); err != nil {
		return nil, err
	}
	w := newBodyWriter()
	w.u64(body.RequestID)
	w.str(body.ClientID)
	w.u8(body.Role)
	return w.finish()
}

func decodeHelloBody(data []byte) (HelloBody, error) {
	r := newBodyReader(data)
	body := HelloBody{RequestID: r.u64(), ClientID: r.str(), Role: r.u8()}
	if err := r.finish(); err != nil {
		return HelloBody{}, err
	}
	if err := validateRole(body.Role); err != nil {
		return HelloBody{}, err
	}
	return body, nil
}

func encodeWelcomeBody(body WelcomeBody) ([]byte, error) {
	w := newBodyWriter()
	w.u64(body.RequestID)
	w.str(body.SessionID)
	w.u64(body.Epoch)
	w.u64(body.TopologyVersion)
	w.str(body.PrimaryAddr)
	return w.finish()
}

func decodeWelcomeBody(data []byte) (WelcomeBody, error) {
	r := newBodyReader(data)
	body := WelcomeBody{
		RequestID:       r.u64(),
		SessionID:       r.str(),
		Epoch:           r.u64(),
		TopologyVersion: r.u64(),
		PrimaryAddr:     r.str(),
	}
	if err := r.finish(); err != nil {
		return WelcomeBody{}, err
	}
	return body, nil
}

func encodeCreateTopicBody(body CreateTopicBody) ([]byte, error) {
	w := newBodyWriter()
	w.u64(body.RequestID)
	w.str(body.Topic)
	return w.finish()
}

func decodeCreateTopicBody(data []byte) (CreateTopicBody, error) {
	r := newBodyReader(data)
	body := CreateTopicBody{RequestID: r.u64(), Topic: r.str()}
	if err := r.finish(); err != nil {
		return CreateTopicBody{}, err
	}
	return body, nil
}

func encodeDeleteTopicBody(body DeleteTopicBody) ([]byte, error) {
	w := newBodyWriter()
	w.u64(body.RequestID)
	w.str(body.Topic)
	return w.finish()
}

func decodeDeleteTopicBody(data []byte) (DeleteTopicBody, error) {
	r := newBodyReader(data)
	body := DeleteTopicBody{RequestID: r.u64(), Topic: r.str()}
	if err := r.finish(); err != nil {
		return DeleteTopicBody{}, err
	}
	return body, nil
}

func encodeSubscribeBody(body SubscribeBody) ([]byte, error) {
	w := newBodyWriter()
	w.u64(body.RequestID)
	w.str(body.SubscriptionID)
	w.str(body.Topic)
	return w.finish()
}

func decodeSubscribeBody(data []byte) (SubscribeBody, error) {
	r := newBodyReader(data)
	body := SubscribeBody{RequestID: r.u64(), SubscriptionID: r.str(), Topic: r.str()}
	if err := r.finish(); err != nil {
		return SubscribeBody{}, err
	}
	return body, nil
}

func encodeUnsubscribeBody(body UnsubscribeBody) ([]byte, error) {
	w := newBodyWriter()
	w.u64(body.RequestID)
	w.str(body.SubscriptionID)
	w.str(body.Topic)
	return w.finish()
}

func decodeUnsubscribeBody(data []byte) (UnsubscribeBody, error) {
	r := newBodyReader(data)
	body := UnsubscribeBody{RequestID: r.u64(), SubscriptionID: r.str(), Topic: r.str()}
	if err := r.finish(); err != nil {
		return UnsubscribeBody{}, err
	}
	return body, nil
}

func encodePublishBody(body PublishBody) ([]byte, error) {
	if len(body.Payload) > MaxBodySize {
		return nil, fmt.Errorf("%w: payload=%d bytes", ErrBodyTooLarge, len(body.Payload))
	}
	w := newBodyWriter()
	w.u64(body.RequestID)
	w.str(body.Topic)
	w.str(body.PublisherID)
	w.i64(body.PublishedAtUnixNano)
	w.bytes(body.Payload)
	return w.finish()
}

func decodePublishBody(data []byte) (PublishBody, error) {
	r := newBodyReader(data)
	body := PublishBody{
		RequestID:           r.u64(),
		Topic:               r.str(),
		PublisherID:         r.str(),
		PublishedAtUnixNano: r.i64(),
		Payload:             r.bytes(),
	}
	if err := r.finish(); err != nil {
		return PublishBody{}, err
	}
	return body, nil
}

func encodePubResultBody(body PubResultBody) ([]byte, error) {
	if err := validatePublishOutcome(body.Outcome); err != nil {
		return nil, err
	}
	w := newBodyWriter()
	w.u64(body.RequestID)
	w.u8(body.Outcome)
	w.u32(body.SubscriberCount)
	return w.finish()
}

func decodePubResultBody(data []byte) (PubResultBody, error) {
	r := newBodyReader(data)
	body := PubResultBody{RequestID: r.u64(), Outcome: r.u8(), SubscriberCount: r.u32()}
	if err := r.finish(); err != nil {
		return PubResultBody{}, err
	}
	if err := validatePublishOutcome(body.Outcome); err != nil {
		return PubResultBody{}, err
	}
	return body, nil
}

func encodeDeliverBody(body DeliverBody) ([]byte, error) {
	if len(body.Payload) > MaxBodySize {
		return nil, fmt.Errorf("%w: payload=%d bytes", ErrBodyTooLarge, len(body.Payload))
	}
	w := newBodyWriter()
	w.str(body.SubscriptionID)
	w.str(body.Topic)
	w.str(body.PublisherID)
	w.u64(body.Sequence)
	w.i64(body.PublishedAtUnixNano)
	w.bytes(body.Payload)
	return w.finish()
}

func decodeDeliverBody(data []byte) (DeliverBody, error) {
	r := newBodyReader(data)
	body := DeliverBody{
		SubscriptionID:      r.str(),
		Topic:               r.str(),
		PublisherID:         r.str(),
		Sequence:            r.u64(),
		PublishedAtUnixNano: r.i64(),
		Payload:             r.bytes(),
	}
	if err := r.finish(); err != nil {
		return DeliverBody{}, err
	}
	return body, nil
}

func encodeGapBody(body GapBody) ([]byte, error) {
	if body.To < body.From {
		return nil, fmt.Errorf("%w: invalid gap interval %d-%d", ErrInvalidBody, body.From, body.To)
	}
	w := newBodyWriter()
	w.str(body.SubscriptionID)
	w.str(body.Topic)
	w.u64(body.From)
	w.u64(body.To)
	return w.finish()
}

func decodeGapBody(data []byte) (GapBody, error) {
	r := newBodyReader(data)
	body := GapBody{SubscriptionID: r.str(), Topic: r.str(), From: r.u64(), To: r.u64()}
	if err := r.finish(); err != nil {
		return GapBody{}, err
	}
	if body.To < body.From {
		return GapBody{}, fmt.Errorf("%w: invalid gap interval %d-%d", ErrInvalidBody, body.From, body.To)
	}
	return body, nil
}

func encodeResumeBody(body ResumeBody) ([]byte, error) {
	w := newBodyWriter()
	w.u64(body.RequestID)
	w.str(body.SessionID)
	if len(body.Cursors) > maxStringLen {
		return nil, fmt.Errorf("%w: cursor count=%d", ErrInvalidBody, len(body.Cursors))
	}
	w.u16(uint16(len(body.Cursors)))
	for _, cursor := range body.Cursors {
		w.str(cursor.Topic)
		w.u64(cursor.LastSequence)
	}
	return w.finish()
}

func decodeResumeBody(data []byte) (ResumeBody, error) {
	r := newBodyReader(data)
	body := ResumeBody{RequestID: r.u64(), SessionID: r.str()}
	count := int(r.u16())
	body.Cursors = make([]ResumeCursor, count)
	for i := 0; i < count; i++ {
		body.Cursors[i] = ResumeCursor{Topic: r.str(), LastSequence: r.u64()}
	}
	if err := r.finish(); err != nil {
		return ResumeBody{}, err
	}
	return body, nil
}

func encodeThrottleBody(body ThrottleBody) ([]byte, error) {
	w := newBodyWriter()
	w.str(body.Topic)
	w.u32(body.RetryAfterMillis)
	w.str(body.Reason)
	return w.finish()
}

func decodeThrottleBody(data []byte) (ThrottleBody, error) {
	r := newBodyReader(data)
	body := ThrottleBody{Topic: r.str(), RetryAfterMillis: r.u32(), Reason: r.str()}
	if err := r.finish(); err != nil {
		return ThrottleBody{}, err
	}
	return body, nil
}

func encodeRedirectBody(body RedirectBody) ([]byte, error) {
	w := newBodyWriter()
	w.str(body.Topic)
	w.str(body.BrokerAddr)
	w.u64(body.Epoch)
	return w.finish()
}

func decodeRedirectBody(data []byte) (RedirectBody, error) {
	r := newBodyReader(data)
	body := RedirectBody{Topic: r.str(), BrokerAddr: r.str(), Epoch: r.u64()}
	if err := r.finish(); err != nil {
		return RedirectBody{}, err
	}
	return body, nil
}

func encodeHeartbeatBody(body HeartbeatBody) ([]byte, error) {
	w := newBodyWriter()
	w.str(body.SessionID)
	w.i64(body.SentAtUnixNano)
	return w.finish()
}

func decodeHeartbeatBody(data []byte) (HeartbeatBody, error) {
	r := newBodyReader(data)
	body := HeartbeatBody{SessionID: r.str(), SentAtUnixNano: r.i64()}
	if err := r.finish(); err != nil {
		return HeartbeatBody{}, err
	}
	return body, nil
}

func encodeTopicClosedBody(body TopicClosedBody) ([]byte, error) {
	w := newBodyWriter()
	w.str(body.Topic)
	w.str(body.Reason)
	return w.finish()
}

func decodeTopicClosedBody(data []byte) (TopicClosedBody, error) {
	r := newBodyReader(data)
	body := TopicClosedBody{Topic: r.str(), Reason: r.str()}
	if err := r.finish(); err != nil {
		return TopicClosedBody{}, err
	}
	return body, nil
}

func encodeErrorBody(body ErrorBody) ([]byte, error) {
	w := newBodyWriter()
	w.u64(body.RequestID)
	w.u16(body.Code)
	w.str(body.Message)
	return w.finish()
}

func decodeErrorBody(data []byte) (ErrorBody, error) {
	r := newBodyReader(data)
	body := ErrorBody{RequestID: r.u64(), Code: r.u16(), Message: r.str()}
	if err := r.finish(); err != nil {
		return ErrorBody{}, err
	}
	return body, nil
}

func encodeControlAckBody(body ControlAckBody) ([]byte, error) {
	w := newBodyWriter()
	w.u64(body.RequestID)
	w.u16(body.Operation)
	w.str(body.SessionID)
	return w.finish()
}

func decodeControlAckBody(data []byte) (ControlAckBody, error) {
	r := newBodyReader(data)
	body := ControlAckBody{RequestID: r.u64(), Operation: r.u16(), SessionID: r.str()}
	if err := r.finish(); err != nil {
		return ControlAckBody{}, err
	}
	return body, nil
}

func validateRole(role uint8) error {
	if role != RoleConsumer && role != RolePublisher && role != RoleBoth {
		return fmt.Errorf("%w: role=%d", ErrInvalidBody, role)
	}
	return nil
}

func validatePublishOutcome(outcome uint8) error {
	if outcome < PublishOutcomeDelivered || outcome > PublishOutcomeTopicNotFound {
		return fmt.Errorf("%w: publish outcome=%d", ErrInvalidBody, outcome)
	}
	return nil
}

func bodyTypeError(messageType MessageType, body any) error {
	return fmt.Errorf("%w: type %T for message %s", ErrInvalidBodyType, body, messageType.String())
}

type bodyWriter struct {
	buf []byte
	err error
}

func newBodyWriter() *bodyWriter {
	return &bodyWriter{}
}

func (w *bodyWriter) u8(value uint8) {
	if w.err != nil {
		return
	}
	w.buf = append(w.buf, value)
}

func (w *bodyWriter) u16(value uint16) {
	if w.err != nil {
		return
	}
	var scratch [2]byte
	binary.BigEndian.PutUint16(scratch[:], value)
	w.buf = append(w.buf, scratch[:]...)
}

func (w *bodyWriter) u32(value uint32) {
	if w.err != nil {
		return
	}
	var scratch [4]byte
	binary.BigEndian.PutUint32(scratch[:], value)
	w.buf = append(w.buf, scratch[:]...)
}

func (w *bodyWriter) u64(value uint64) {
	if w.err != nil {
		return
	}
	var scratch [8]byte
	binary.BigEndian.PutUint64(scratch[:], value)
	w.buf = append(w.buf, scratch[:]...)
}

func (w *bodyWriter) i64(value int64) {
	w.u64(uint64(value))
}

func (w *bodyWriter) str(value string) {
	if w.err != nil {
		return
	}
	if len(value) > maxStringLen {
		w.err = fmt.Errorf("%w: %d bytes", ErrStringTooLong, len(value))
		return
	}
	w.u16(uint16(len(value)))
	w.buf = append(w.buf, value...)
}

func (w *bodyWriter) bytes(value []byte) {
	if w.err != nil {
		return
	}
	if len(value) > maxBytesLen {
		w.err = fmt.Errorf("%w: %d bytes", ErrBytesTooLong, len(value))
		return
	}
	w.u16(uint16(len(value)))
	w.buf = append(w.buf, value...)
}

func (w *bodyWriter) finish() ([]byte, error) {
	if w.err != nil {
		return nil, w.err
	}
	if len(w.buf) > MaxBodySize {
		return nil, fmt.Errorf("%w: %d bytes, maximum %d", ErrBodyTooLarge, len(w.buf), MaxBodySize)
	}
	return append([]byte(nil), w.buf...), nil
}

type bodyReader struct {
	buf []byte
	off int
	err error
}

func newBodyReader(buf []byte) *bodyReader {
	return &bodyReader{buf: buf}
}

func (r *bodyReader) need(n int) bool {
	if r.err != nil {
		return false
	}
	if n < 0 || r.off+n > len(r.buf) {
		r.err = fmt.Errorf("%w: need %d bytes at offset %d but body length is %d", ErrInvalidBody, n, r.off, len(r.buf))
		return false
	}
	return true
}

func (r *bodyReader) u8() uint8 {
	if !r.need(1) {
		return 0
	}
	value := r.buf[r.off]
	r.off++
	return value
}

func (r *bodyReader) u16() uint16 {
	if !r.need(2) {
		return 0
	}
	value := binary.BigEndian.Uint16(r.buf[r.off : r.off+2])
	r.off += 2
	return value
}

func (r *bodyReader) u32() uint32 {
	if !r.need(4) {
		return 0
	}
	value := binary.BigEndian.Uint32(r.buf[r.off : r.off+4])
	r.off += 4
	return value
}

func (r *bodyReader) u64() uint64 {
	if !r.need(8) {
		return 0
	}
	value := binary.BigEndian.Uint64(r.buf[r.off : r.off+8])
	r.off += 8
	return value
}

func (r *bodyReader) i64() int64 {
	return int64(r.u64())
}

func (r *bodyReader) str() string {
	length := int(r.u16())
	if !r.need(length) {
		return ""
	}
	value := string(r.buf[r.off : r.off+length])
	r.off += length
	return value
}

func (r *bodyReader) bytes() []byte {
	length := int(r.u16())
	if !r.need(length) {
		return nil
	}
	value := append([]byte(nil), r.buf[r.off:r.off+length]...)
	r.off += length
	return value
}

func (r *bodyReader) finish() error {
	if r.err != nil {
		return r.err
	}
	if r.off != len(r.buf) {
		return fmt.Errorf("%w: %d unread bytes", ErrBodyTrailingData, len(r.buf)-r.off)
	}
	return nil
}
