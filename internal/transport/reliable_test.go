package transport

import (
	"errors"
	"testing"
	"time"

	"pubsub/internal/wire"
)

func TestReliableControlTimesOutAfterConfiguredRetries(t *testing.T) {
	sender := mustListen(t)
	defer sender.Close()
	receiver := mustListen(t)
	defer receiver.Close()

	payload, err := wire.EncodeDatagram(wire.Header{Version: wire.Version1, Type: wire.MessageCreateTopic}, []byte{0})
	if err != nil {
		t.Fatal(err)
	}

	reliable := &ReliableControl{UDP: sender, Timeout: 20 * time.Millisecond, MaxRetries: 1}
	_, err = reliable.SendAndWait(payload, receiver.LocalAddr(), wire.MessageControlAck)
	if !errors.Is(err, ErrControlTimeout) {
		t.Fatalf("SendAndWait() error = %v, want %v", err, ErrControlTimeout)
	}
}

func TestReliableControlRejectsInvalidConfiguration(t *testing.T) {
	var nilReliable *ReliableControl
	_, err := nilReliable.SendAndWait(nil, nil, wire.MessageControlAck)
	if !errors.Is(err, ErrNilReliableUDP) {
		t.Fatalf("nil ReliableControl error = %v, want %v", err, ErrNilReliableUDP)
	}

	reliable := &ReliableControl{UDP: mustListen(t), Timeout: 0, MaxRetries: 0}
	defer reliable.UDP.Close()
	_, err = reliable.SendAndWait(nil, reliable.UDP.LocalAddr(), wire.MessageControlAck)
	if err == nil {
		t.Fatal("SendAndWait() must reject a non-positive timeout")
	}
}
