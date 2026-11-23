package replication

import (
	"errors"
	"testing"
	"time"
)

func TestHeartbeatTimesOutAndPromotesOnce(t *testing.T) {
	heartbeat, err := NewHeartbeat(time.Second)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Unix(100, 0)
	heartbeat.Mark(start)
	if heartbeat.Suspect(start.Add(999 * time.Millisecond)) {
		t.Fatal("heartbeat must not be suspect before timeout")
	}
	if !heartbeat.Suspect(start.Add(time.Second)) {
		t.Fatal("heartbeat must be suspect at timeout")
	}
	if !heartbeat.Promote() {
		t.Fatal("first promotion must win")
	}
	if heartbeat.Promote() {
		t.Fatal("second promotion must lose")
	}
	if !heartbeat.Promoted() {
		t.Fatal("promoted state must be retained")
	}
}

func TestInvalidHeartbeatTimeout(t *testing.T) {
	if _, err := NewHeartbeat(0); !errors.Is(err, ErrInvalidTimeout) {
		t.Fatalf("error = %v", err)
	}
}

func TestControlStoreRetainsSnapshot(t *testing.T) {
	store := NewStore()
	store.Apply(ControlSnapshot{Epoch: 7, TopicState: "topics", SessionState: "sessions"})
	got := store.Snapshot()
	if got.Epoch != 7 || got.TopicState != "topics" || got.SessionState != "sessions" {
		t.Fatalf("snapshot = %+v", got)
	}
}
