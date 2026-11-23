package transport

import (
	"errors"
	"fmt"
	"net"
	"time"

	"pubsub/internal/wire"
)

var (
	ErrControlTimeout = errors.New("transport: reliable control request timed out")
	ErrNilReliableUDP = errors.New("transport: reliable control UDP is nil")
)

// ReliableControl sends one control-plane datagram repeatedly and waits for
// a matching response. PUBLISH and DELIVER must not use this helper.
type ReliableControl struct {
	UDP        *UDP
	Timeout    time.Duration
	MaxRetries int
}

// SendAndWait sends payload up to MaxRetries+1 times and waits for the first
// datagram matching expectedType. It leaves retry policy above data plane.
func (r *ReliableControl) SendAndWait(payload []byte, destination *net.UDPAddr, expectedType wire.MessageType) (Datagram, error) {
	if r == nil || r.UDP == nil {
		return Datagram{}, ErrNilReliableUDP
	}
	if r.Timeout <= 0 {
		return Datagram{}, fmt.Errorf("transport: reliable control timeout must be positive")
	}
	if r.MaxRetries < 0 {
		return Datagram{}, fmt.Errorf("transport: reliable control retries must not be negative")
	}

	attempts := r.MaxRetries + 1
	for attempt := 0; attempt < attempts; attempt++ {
		if err := r.UDP.SendTo(payload, destination); err != nil {
			return Datagram{}, err
		}

		deadline := time.Now().Add(r.Timeout)
		for {
			remaining := time.Until(deadline)
			if remaining <= 0 {
				break
			}
			if r.UDP.conn == nil {
				return Datagram{}, ErrNilConnection
			}
			if err := r.UDP.conn.SetReadDeadline(time.Now().Add(remaining)); err != nil {
				return Datagram{}, err
			}
			datagram, err := r.UDP.Receive()
			if err != nil {
				if errors.Is(err, ErrClosed) {
					return Datagram{}, err
				}
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					break
				}
				return Datagram{}, err
			}
			header, _, decodeErr := wire.DecodeDatagram(datagram.Payload)
			if decodeErr != nil {
				continue
			}
			if header.Type == expectedType {
				_ = r.UDP.conn.SetReadDeadline(time.Time{})
				return datagram, nil
			}
		}
	}
	return Datagram{}, ErrControlTimeout
}
