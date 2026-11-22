package transport

import (
	"errors"
	"fmt"
	"net"
	"sync"

	"pubsub/internal/wire"
)

var (
	// ErrNilConnection means a UDP transport was created without a socket.
	ErrNilConnection = errors.New("transport: UDP connection is nil")
	// ErrNilDestination means a datagram has no UDP destination.
	ErrNilDestination = errors.New("transport: UDP destination is nil")
	// ErrClosed means an operation was interrupted because the transport closed.
	ErrClosed = errors.New("transport: closed")
)

// Datagram is one complete UDP packet together with its source address.
// Payload is owned by the caller and can safely outlive the next Receive call.
type Datagram struct {
	Payload []byte
	From    *net.UDPAddr
}

// UDP owns one UDP socket. A UDP instance supports one or more concurrent
// senders and one or more concurrent receivers, although the broker will use
// a single receive loop to preserve its own dispatch ordering.
type UDP struct {
	conn *net.UDPConn

	closeOnce sync.Once
	closeErr  error
}

// Listen resolves address and opens a UDP socket. Typical broker addresses are
// "0.0.0.0:9000" in production and "127.0.0.1:0" in loopback tests.
func Listen(address string) (*UDP, error) {
	udpAddr, err := net.ResolveUDPAddr("udp", address)
	if err != nil {
		return nil, fmt.Errorf("transport: resolve UDP address %q: %w", address, err)
	}
	return ListenUDP(udpAddr)
}

// ListenUDP opens a UDP socket on address.
func ListenUDP(address *net.UDPAddr) (*UDP, error) {
	conn, err := net.ListenUDP("udp", address)
	if err != nil {
		return nil, fmt.Errorf("transport: listen UDP on %v: %w", address, err)
	}
	return NewUDP(conn)
}

// NewUDP wraps an existing UDP socket. It is useful for tests and for callers
// that need to set socket options before handing the socket to this package.
func NewUDP(conn *net.UDPConn) (*UDP, error) {
	if conn == nil {
		return nil, ErrNilConnection
	}
	return &UDP{conn: conn}, nil
}

// LocalAddr returns a copy of the bound local address.
func (u *UDP) LocalAddr() *net.UDPAddr {
	if u == nil || u.conn == nil {
		return nil
	}
	address, _ := u.conn.LocalAddr().(*net.UDPAddr)
	return copyUDPAddr(address)
}

// SendTo writes one complete datagram to destination. UDP data-plane sends are
// intentionally fire-and-forget: successful return only means the operating
// system accepted the datagram, not that a remote peer received it.
func (u *UDP) SendTo(payload []byte, destination *net.UDPAddr) error {
	if u == nil || u.conn == nil {
		return ErrNilConnection
	}
	if destination == nil {
		return ErrNilDestination
	}
	if len(payload) > wire.MaxDatagramSize {
		return fmt.Errorf("%w: %d bytes, maximum %d", wire.ErrDatagramTooLarge, len(payload), wire.MaxDatagramSize)
	}
	n, err := u.conn.WriteToUDP(payload, destination)
	if err != nil {
		return normalizeError(err)
	}
	if n != len(payload) {
		return fmt.Errorf("transport: partial UDP write: wrote %d of %d bytes", n, len(payload))
	}
	return nil
}

// Receive waits for one UDP datagram. Packets larger than the v1 limit are
// consumed and rejected instead of being passed upward as truncated payloads.
func (u *UDP) Receive() (Datagram, error) {
	if u == nil || u.conn == nil {
		return Datagram{}, ErrNilConnection
	}
	// One additional byte makes an oversized packet observable even when the
	// operating system truncates it to the supplied receive buffer.
	buffer := make([]byte, wire.MaxDatagramSize+1)
	n, source, err := u.conn.ReadFromUDP(buffer)
	if err != nil {
		return Datagram{}, normalizeError(err)
	}
	if n > wire.MaxDatagramSize {
		return Datagram{}, fmt.Errorf("%w: received at least %d bytes, maximum %d", wire.ErrDatagramTooLarge, n, wire.MaxDatagramSize)
	}
	payload := append([]byte(nil), buffer[:n]...)
	return Datagram{Payload: payload, From: copyUDPAddr(source)}, nil
}

// Close closes the socket and unblocks pending Receive calls. It is idempotent.
func (u *UDP) Close() error {
	if u == nil || u.conn == nil {
		return ErrNilConnection
	}
	u.closeOnce.Do(func() { u.closeErr = u.conn.Close() })
	return u.closeErr
}

func normalizeError(err error) error {
	if errors.Is(err, net.ErrClosed) {
		return fmt.Errorf("%w: %v", ErrClosed, err)
	}
	return err
}

func copyUDPAddr(address *net.UDPAddr) *net.UDPAddr {
	if address == nil {
		return nil
	}
	copy := *address
	copy.IP = append(net.IP(nil), address.IP...)
	return &copy
}
