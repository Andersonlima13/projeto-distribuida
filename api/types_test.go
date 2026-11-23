package api

import (
	"errors"
	"testing"
	"time"
)

func TestPublicMessageAndPublishResultCanRepresentPublishLifecycle(t *testing.T) {
	message := Message{
		Topic:       "score",
		PublisherID: "producer-1",
		PublishedAt: time.Unix(1, 0),
		Payload:     []byte("goal"),
	}
	if message.Sequence != 0 { t.Fatalf("client-originated sequence = %d, want 0", message.Sequence) }
	result := PublishResult{Outcome: PublishDelivered, SubscriberCount: 3}
	if result.Outcome != PublishDelivered || result.SubscriberCount != 3 { t.Fatalf("result = %+v", result) }
}

func TestPublicErrorPreservesCodeAndMessage(t *testing.T) {
	err := Error{Code: ErrorThrottled, Message: "publisher rate exceeded"}
	if err.Error() != "publisher rate exceeded" { t.Fatalf("Error() = %q", err.Error()) }
	if !errors.As(error(err), new(Error)) { t.Fatal("Error must implement error") }
}
