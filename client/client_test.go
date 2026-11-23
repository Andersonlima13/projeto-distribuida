package client_test

import (
	"testing"
	"time"

	"pubsub/api"
	"pubsub/client"
	"pubsub/internal/broker"
	"pubsub/internal/queue"
	"pubsub/internal/transport"
)

func TestClientConnectCreateSubscribePublishAndDeliver(t *testing.T) {
	udp, err := transport.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer udp.Close()

	b, err := broker.New(broker.Config{
		NodeAddress: udp.LocalAddr().String(),
		Epoch:       1,
		QueueLimits: queue.Limits{MaxMessages: 8, MaxBytes: 1024},
	})
	if err != nil {
		t.Fatal(err)
	}

	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		for {
			datagram, err := udp.Receive()
			if err != nil {
				return
			}
			outbound, err := b.HandleDatagram(datagram.Payload, datagram.From)
			if err != nil {
				continue
			}
			for _, response := range outbound {
				_ = udp.SendTo(response.Payload, response.To)
			}
		}
	}()

	delivered := make(chan api.Message, 1)
	c, err := client.Connect(api.Options{
		BrokerAddress:  udp.LocalAddr().String(),
		ClientID:       "client-1",
		Role:           api.RoleBoth,
		ControlTimeout: 500 * time.Millisecond,
		ControlRetries: 2,
		OnDeliver:      nil,
		OnGap:          nil,
		OnTopicClosed:  nil,
		OnThrottle:     nil,
		OnError:        nil,
	})
	if err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	defer c.Close()

	if c.SessionID() == "" {
		t.Fatal("SessionID() is empty")
	}
	if err := c.CreateTopic("score"); err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	if _, err := c.Subscribe("score", func(message api.Message) {
		delivered <- message
	}); err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}

	result, err := c.Publish("score", []byte("goal"))
	if err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	if result.Outcome != api.PublishDelivered || result.SubscriberCount != 1 {
		t.Fatalf("Publish result = %+v", result)
	}

	select {
	case message := <-delivered:
		if message.Topic != "score" || string(message.Payload) != "goal" || message.Sequence != 1 {
			t.Fatalf("delivery = %+v", message)
		}
	case <-time.After(time.Second):
		t.Fatal("delivery callback timed out")
	}
}
