package wire

import (
	"encoding/binary"
	"errors"
	"fmt"
)

const (
	// Version1 is the only protocol version implemented by this module.
	Version1 uint8 = 1

	// HeaderSize is the exact number of bytes in every protocol header.
	// Layout: version (1), type (1), flags (2), body length (4), big-endian.
	HeaderSize = 8

	// MaxDatagramSize is the v1 UDP datagram budget. It prevents IP
	// fragmentation in the intended network environment.
	MaxDatagramSize = 1200

	// MaxBodySize is the largest body that fits after a fixed-size header.
	MaxBodySize = MaxDatagramSize - HeaderSize
)

var (
	// ErrDatagramTooShort means that a datagram cannot contain a full header.
	ErrDatagramTooShort = errors.New("wire: datagram shorter than header")
	// ErrDatagramTooLarge means that a datagram exceeds the v1 UDP budget.
	ErrDatagramTooLarge = errors.New("wire: datagram exceeds maximum size")
	// ErrUnsupportedVersion means that the peer uses a protocol version not understood here.
	ErrUnsupportedVersion = errors.New("wire: unsupported protocol version")
	// ErrUnknownMessageType means that the type byte is not assigned by this version.
	ErrUnknownMessageType = errors.New("wire: unknown message type")
	// ErrUnsupportedFlags means that a reserved header flag was set in v1.
	ErrUnsupportedFlags = errors.New("wire: unsupported header flags")
	// ErrBodyTooLarge means that the declared body cannot fit in one v1 datagram.
	ErrBodyTooLarge = errors.New("wire: body exceeds maximum size")
	// ErrBodyLengthMismatch means that the header length differs from the bytes received.
	ErrBodyLengthMismatch = errors.New("wire: body length does not match datagram")
)

// Header is the fixed-size prefix of every UDP datagram.
// Flags are reserved for future protocol versions and must be zero in v1.
type Header struct {
	Version    uint8
	Type       MessageType
	Flags      uint16
	BodyLength uint32
}

// Validate checks header fields independent of an actual datagram buffer.
func (h Header) Validate() error {
	if h.Version != Version1 {
		return fmt.Errorf("%w: got %d, want %d", ErrUnsupportedVersion, h.Version, Version1)
	}
	if !h.Type.Valid() {
		return fmt.Errorf("%w: %d", ErrUnknownMessageType, h.Type)
	}
	if h.Flags != 0 {
		return fmt.Errorf("%w: 0x%04x", ErrUnsupportedFlags, h.Flags)
	}
	if h.BodyLength > MaxBodySize {
		return fmt.Errorf("%w: %d bytes, maximum %d", ErrBodyTooLarge, h.BodyLength, MaxBodySize)
	}
	return nil
}

// EncodeHeader serializes h into a new eight-byte big-endian header.
func EncodeHeader(h Header) ([]byte, error) {
	if err := h.Validate(); err != nil {
		return nil, err
	}
	buf := make([]byte, HeaderSize)
	buf[0] = h.Version
	buf[1] = byte(h.Type)
	binary.BigEndian.PutUint16(buf[2:4], h.Flags)
	binary.BigEndian.PutUint32(buf[4:8], h.BodyLength)
	return buf, nil
}

// DecodeHeader decodes and validates the header prefix of datagram. It does
// not validate that the complete body is present; use DecodeDatagram for that.
func DecodeHeader(datagram []byte) (Header, error) {
	if len(datagram) < HeaderSize {
		return Header{}, fmt.Errorf("%w: got %d bytes, need %d", ErrDatagramTooShort, len(datagram), HeaderSize)
	}
	h := Header{
		Version:    datagram[0],
		Type:       MessageType(datagram[1]),
		Flags:      binary.BigEndian.Uint16(datagram[2:4]),
		BodyLength: binary.BigEndian.Uint32(datagram[4:8]),
	}
	if err := h.Validate(); err != nil {
		return Header{}, err
	}
	return h, nil
}

// EncodeDatagram builds one complete UDP datagram from a validated header and body.
func EncodeDatagram(h Header, body []byte) ([]byte, error) {
	if len(body) > MaxBodySize {
		return nil, fmt.Errorf("%w: %d bytes, maximum %d", ErrBodyTooLarge, len(body), MaxBodySize)
	}
	h.BodyLength = uint32(len(body))
	header, err := EncodeHeader(h)
	if err != nil {
		return nil, err
	}
	datagram := make([]byte, HeaderSize+len(body))
	copy(datagram, header)
	copy(datagram[HeaderSize:], body)
	return datagram, nil
}

// DecodeDatagram validates a complete UDP datagram and returns the header and
// a body view backed by datagram. Callers must copy the body before retaining
// it beyond the receive buffer's lifetime.
func DecodeDatagram(datagram []byte) (Header, []byte, error) {
	if len(datagram) > MaxDatagramSize {
		return Header{}, nil, fmt.Errorf("%w: %d bytes, maximum %d", ErrDatagramTooLarge, len(datagram), MaxDatagramSize)
	}
	h, err := DecodeHeader(datagram)
	if err != nil {
		return Header{}, nil, err
	}
	body := datagram[HeaderSize:]
	if uint32(len(body)) != h.BodyLength {
		return Header{}, nil, fmt.Errorf("%w: header=%d, actual=%d", ErrBodyLengthMismatch, h.BodyLength, len(body))
	}
	return h, body, nil
}
