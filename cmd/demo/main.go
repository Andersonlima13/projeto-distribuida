package main

import (
	"flag"
	"fmt"
	"time"

	"pubsub/api"
	"pubsub/client"
)

func main() {
	brokerAddr := flag.String("broker", "127.0.0.1:9000", "UDP broker address")
	clientID := flag.String("client", "demo-client", "stable client identifier")
	flag.Parse()

	c, err := client.Connect(api.Options{
		BrokerAddress:  *brokerAddr,
		ClientID:       api.ClientID(*clientID),
		Role:           api.RoleBoth,
		ControlTimeout: 750 * time.Millisecond,
		ControlRetries: 2,
		OnDeliver: func(message api.Message) {
			fmt.Printf("deliver topic=%s sequence=%d payload=%q\n", message.Topic, message.Sequence, message.Payload)
		},
		OnGap: func(gap api.Gap) {
			fmt.Printf("gap topic=%s from=%d to=%d\n", gap.Topic, gap.From, gap.To)
		},
		OnTopicClosed: func(topic api.Topic) {
			fmt.Printf("topic_closed topic=%s\n", topic)
		},
		OnThrottle: func(topic api.Topic, retryMillis uint32) {
			fmt.Printf("throttle topic=%s retry_ms=%d\n", topic, retryMillis)
		},
		OnError: func(err api.Error) {
			fmt.Printf("error code=%d message=%s\n", err.Code, err.Message)
		},
	})
	if err != nil {
		fmt.Printf("connect error: %v\n", err)
		return
	}
	defer c.Close()

	const topic = api.Topic("demo")
	if err := c.CreateTopic(topic); err != nil {
		fmt.Printf("create topic error: %v\n", err)
		return
	}
	if _, err := c.Subscribe(topic, func(message api.Message) {
		fmt.Printf("demo_delivery topic=%s sequence=%d payload=%q\n", message.Topic, message.Sequence, message.Payload)
	}); err != nil {
		fmt.Printf("subscribe error: %v\n", err)
		return
	}

	result, err := c.Publish(topic, []byte("hello"))
	if err != nil {
		fmt.Printf("publish error: %v\n", err)
		return
	}
	fmt.Printf("pub_result outcome=%d subscribers=%d session=%s\n", result.Outcome, result.SubscriberCount, c.SessionID())
}
