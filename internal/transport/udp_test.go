package transport

import (
	"errors"
	"net"
	"testing"
	"time"

	"pubsub/internal/wire"
)

func TestSendAndReceiveOverLoopback(t *testing.T) {
	sender := mustListen(t)
	defer sender.Close()
	receiver := mustListen(t)
	defer receiver.Close()

	payload := []byte("live-event")
	if err := sender.SendTo(payload, receiver.LocalAddr()); err != nil {
		t.Fatalf("SendTo() error = %v", err)
	}

	type result struct {
		datagram Datagram
		err      error
	}
	results := make(chan result, 1)
	go func() { datagram, err := receiver.Receive(); results <- result{datagram, err} }()
	select {
	case got := <-results:
		if got.err != nil {
			t.Fatalf("Receive() error = %v", got.err)
		}
		if string(got.datagram.Payload) != string(payload) {
			t.Fatalf("payload = %q, want %q", got.datagram.Payload, payload)
		}
		if got.datagram.From == nil || got.datagram.From.Port != sender.LocalAddr().Port {
			t.Fatalf("source = %v, sender = %v", got.datagram.From, sender.LocalAddr())
		}
	case <-time.After(time.Second):
		t.Fatal("Receive() did not return within one second")
	}
}

func TestSendRejectsOversizedDatagram(t *testing.T) {
	udp := mustListen(t)
	defer udp.Close()
	err := udp.SendTo(make([]byte, wire.MaxDatagramSize+1), udp.LocalAddr())
	if !errors.Is(err, wire.ErrDatagramTooLarge) {
		t.Fatalf("SendTo() error = %v, want %v", err, wire.ErrDatagramTooLarge)
	}
}

func TestReceiveRejectsOversizedDatagram(t *testing.T) {
	receiver := mustListen(t)
	defer receiver.Close()
	sender, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Close()
	if _, err := sender.WriteToUDP(make([]byte, wire.MaxDatagramSize+1), receiver.LocalAddr()); err != nil {
		t.Fatal(err)
	}
	if _, err := receiver.Receive(); !errors.Is(err, wire.ErrDatagramTooLarge) {
		t.Fatalf("Receive() error = %v, want %v", err, wire.ErrDatagramTooLarge)
	}
}

func TestCloseUnblocksReceiveAndIsIdempotent(t *testing.T) {
	udp := mustListen(t)
	results := make(chan error, 1)
	go func() { _, err := udp.Receive(); results <- err }()
	if err := udp.Close(); err != nil {
		t.Fatalf("first Close() error = %v", err)
	}
	if err := udp.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
	select {
	case err := <-results:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("Receive() error = %v, want %v", err, ErrClosed)
		}
	case <-time.After(time.Second):
		t.Fatal("Receive() did not unblock after Close")
	}
}

func TestNewUDPAndSendValidateNilInputs(t *testing.T) {
	if _, err := NewUDP(nil); !errors.Is(err, ErrNilConnection) {
		t.Fatalf("NewUDP(nil) error = %v", err)
	}
	var nilUDP *UDP
	if err := nilUDP.SendTo(nil, &net.UDPAddr{}); !errors.Is(err, ErrNilConnection) {
		t.Fatalf("nil UDP SendTo() error = %v", err)
	}
	if _, err := nilUDP.Receive(); !errors.Is(err, ErrNilConnection) {
		t.Fatalf("nil UDP Receive() error = %v", err)
	}
	udp := mustListen(t)
	defer udp.Close()
	if err := udp.SendTo(nil, nil); !errors.Is(err, ErrNilDestination) {
		t.Fatalf("SendTo(nil destination) error = %v", err)
	}
}

func mustListen(t *testing.T) *UDP {
	t.Helper()
	udp, err := Listen("127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen() error = %v", err)
	}
	return udp
}
