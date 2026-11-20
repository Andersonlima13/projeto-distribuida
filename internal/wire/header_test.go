package wire

import (
	"errors"
	"testing"
)

func TestHeaderRoundTrip(t *testing.T) {
	want := Header{Version: Version1, Type: MessagePublish, BodyLength: 37}
	encoded, err := EncodeHeader(want)
	if err != nil {
		t.Fatalf("EncodeHeader() error = %v", err)
	}
	if len(encoded) != HeaderSize {
		t.Fatalf("encoded length = %d, want %d", len(encoded), HeaderSize)
	}
	got, err := DecodeHeader(encoded)
	if err != nil {
		t.Fatalf("DecodeHeader() error = %v", err)
	}
	if got != want {
		t.Fatalf("DecodeHeader() = %#v, want %#v", got, want)
	}
}

func TestEveryDefinedMessageTypeIsValid(t *testing.T) {
	types := []MessageType{
		MessageHello, MessageWelcome, MessageCreateTopic, MessageDeleteTopic,
		MessageSubscribe, MessageUnsubscribe, MessagePublish, MessagePubResult,
		MessageDeliver, MessageGap, MessageResume, MessageThrottle,
		MessageRedirect, MessageHeartbeat, MessageTopicClosed, MessageError,
	}
	for _, messageType := range types {
		if !messageType.Valid() {
			t.Fatalf("message type %d (%s) must be valid", messageType, messageType)
		}
		if messageType.String() == "UNKNOWN" {
			t.Fatalf("message type %d must have a protocol name", messageType)
		}
	}
}

func TestDatagramRoundTrip(t *testing.T) {
	body := []byte("live-event")
	gotHeader, gotBody, err := DecodeDatagram(mustDatagram(t, Header{Version: Version1, Type: MessageDeliver}, body))
	if err != nil {
		t.Fatalf("DecodeDatagram() error = %v", err)
	}
	if gotHeader.Type != MessageDeliver || gotHeader.BodyLength != uint32(len(body)) {
		t.Fatalf("header = %#v", gotHeader)
	}
	if string(gotBody) != string(body) {
		t.Fatalf("body = %q, want %q", gotBody, body)
	}
}

func TestDecodeHeaderRejectsInvalidFields(t *testing.T) {
	tests := []struct {
		name   string
		header Header
		want   error
	}{
		{"version", Header{Version: 2, Type: MessageHello}, ErrUnsupportedVersion},
		{"type", Header{Version: Version1, Type: 99}, ErrUnknownMessageType},
		{"flags", Header{Version: Version1, Type: MessageHello, Flags: 1}, ErrUnsupportedFlags},
		{"body", Header{Version: Version1, Type: MessageHello, BodyLength: MaxBodySize + 1}, ErrBodyTooLarge},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := make([]byte, HeaderSize)
			encoded[0] = tt.header.Version
			encoded[1] = byte(tt.header.Type)
			encoded[2] = byte(tt.header.Flags >> 8)
			encoded[3] = byte(tt.header.Flags)
			encoded[4] = byte(tt.header.BodyLength >> 24)
			encoded[5] = byte(tt.header.BodyLength >> 16)
			encoded[6] = byte(tt.header.BodyLength >> 8)
			encoded[7] = byte(tt.header.BodyLength)
			_, err := DecodeHeader(encoded)
			if !errors.Is(err, tt.want) {
				t.Fatalf("DecodeHeader() error = %v, want %v", err, tt.want)
			}
		})
	}
}

func TestDecodeDatagramRejectsMalformedLength(t *testing.T) {
	datagram := mustDatagram(t, Header{Version: Version1, Type: MessagePublish}, []byte("one"))
	datagram[7] = 4
	_, _, err := DecodeDatagram(datagram)
	if !errors.Is(err, ErrBodyLengthMismatch) {
		t.Fatalf("DecodeDatagram() error = %v, want %v", err, ErrBodyLengthMismatch)
	}
}

func TestDecodeDatagramRejectsShortAndOversized(t *testing.T) {
	_, _, err := DecodeDatagram(make([]byte, HeaderSize-1))
	if !errors.Is(err, ErrDatagramTooShort) {
		t.Fatalf("short datagram error = %v", err)
	}
	_, _, err = DecodeDatagram(make([]byte, MaxDatagramSize+1))
	if !errors.Is(err, ErrDatagramTooLarge) {
		t.Fatalf("oversized datagram error = %v", err)
	}
}

func mustDatagram(t *testing.T, h Header, body []byte) []byte {
	t.Helper()
	datagram, err := EncodeDatagram(h, body)
	if err != nil {
		t.Fatalf("EncodeDatagram() error = %v", err)
	}
	return datagram
}
