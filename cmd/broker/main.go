package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"pubsub/internal/broker"
	"pubsub/internal/queue"
	"pubsub/internal/transport"
)

func main() {
	address := flag.String("address", "127.0.0.1:9000", "broker UDP address")
	maxMessages := flag.Int("queue-messages", 1024, "maximum queued messages per subscriber")
	maxBytes := flag.Int("queue-bytes", 1048576, "maximum queued bytes per subscriber")
	flag.Parse()

	udp, err := transport.Listen(*address)
	if err != nil {
		log.Fatalf("listen broker: %v", err)
	}
	defer udp.Close()

	b, err := broker.New(broker.Config{
		NodeAddress: udp.LocalAddr().String(),
		Epoch:       1,
		QueueLimits: queue.Limits{MaxMessages: *maxMessages, MaxBytes: *maxBytes},
	})
	if err != nil {
		log.Fatalf("create broker: %v", err)
	}

	log.Printf("broker listening on udp://%s epoch=1", udp.LocalAddr())

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-stop
		_ = udp.Close()
	}()

	for {
		datagram, err := udp.Receive()
		if err != nil {
			return
		}
		outbound, err := b.HandleDatagram(datagram.Payload, datagram.From)
		if err != nil {
			log.Printf("handle datagram from %v: %v", datagram.From, err)
			continue
		}
		for _, response := range outbound {
			if err := udp.SendTo(response.Payload, response.To); err != nil {
				log.Printf("send datagram to %v: %v", response.To, err)
			}
		}
	}
}
