package topic

import (
	"errors"
	"reflect"
	"testing"
)

func TestCreateIsIdempotentAndRejectsInvalidNames(t *testing.T) {
	r := NewRegistry()
	if _, err := r.Create(" "); !errors.Is(err, ErrInvalidTopicName) {
		t.Fatalf("Create(empty) error = %v", err)
	}
	created, err := r.Create("score")
	if err != nil || !created {
		t.Fatalf("first Create() = %t, %v", created, err)
	}
	created, err = r.Create("score")
	if err != nil || created {
		t.Fatalf("retry Create() = %t, %v", created, err)
	}
	if !r.Exists("score") {
		t.Fatal("Exists(score) = false")
	}
}

func TestSubscribeTracksOnlyControlReferences(t *testing.T) {
	r := NewRegistry()
	mustCreate(t, r, "auction")
	added, err := r.Subscribe("auction", "sub-b")
	if err != nil || !added {
		t.Fatalf("Subscribe() = %t, %v", added, err)
	}
	added, err = r.Subscribe("auction", "sub-b")
	if err != nil || added {
		t.Fatalf("retry Subscribe() = %t, %v", added, err)
	}
	mustSubscribe(t, r, "auction", "sub-a")
	subs, err := r.Subscribers("auction")
	if err != nil {
		t.Fatal(err)
	}
	want := []SubscriptionID{"sub-a", "sub-b"}
	if !reflect.DeepEqual(subs, want) {
		t.Fatalf("Subscribers() = %v, want %v", subs, want)
	}
	subs[0] = "mutated"
	subs, _ = r.Subscribers("auction")
	if !reflect.DeepEqual(subs, want) {
		t.Fatalf("registry exposed internal slice: %v", subs)
	}
}

func TestSubscribeRequiresExistingTopicAndUniqueSubscriptionID(t *testing.T) {
	r := NewRegistry()
	if _, err := r.Subscribe("missing", "sub"); !errors.Is(err, ErrTopicNotFound) {
		t.Fatalf("missing topic error = %v", err)
	}
	mustCreate(t, r, "one")
	mustCreate(t, r, "two")
	mustSubscribe(t, r, "one", "shared")
	if _, err := r.Subscribe("two", "shared"); !errors.Is(err, ErrSubscriptionBoundElsewhere) {
		t.Fatalf("shared ID error = %v", err)
	}
}

func TestUnsubscribeIsIdempotentButChecksTopicOwnership(t *testing.T) {
	r := NewRegistry()
	mustCreate(t, r, "one")
	mustCreate(t, r, "two")
	mustSubscribe(t, r, "one", "sub")
	if _, err := r.Unsubscribe("two", "sub"); !errors.Is(err, ErrSubscriptionBoundElsewhere) {
		t.Fatalf("wrong topic error = %v", err)
	}
	removed, err := r.Unsubscribe("one", "sub")
	if err != nil || !removed {
		t.Fatalf("Unsubscribe() = %t, %v", removed, err)
	}
	removed, err = r.Unsubscribe("one", "sub")
	if err != nil || removed {
		t.Fatalf("retry Unsubscribe() = %t, %v", removed, err)
	}
}

func TestDeleteReturnsSubscribersAndReleasesIDs(t *testing.T) {
	r := NewRegistry()
	mustCreate(t, r, "auction")
	mustCreate(t, r, "replacement")
	mustSubscribe(t, r, "auction", "sub-b")
	mustSubscribe(t, r, "auction", "sub-a")
	result, err := r.Delete("auction")
	if err != nil {
		t.Fatal(err)
	}
	if !result.Deleted || !reflect.DeepEqual(result.Subscribers, []SubscriptionID{"sub-a", "sub-b"}) {
		t.Fatalf("Delete() = %+v", result)
	}
	if r.Exists("auction") {
		t.Fatal("deleted topic still exists")
	}
	if _, err := r.Subscribers("auction"); !errors.Is(err, ErrTopicNotFound) {
		t.Fatalf("Subscribers(deleted) error = %v", err)
	}
	mustSubscribe(t, r, "replacement", "sub-a")
	result, err = r.Delete("auction")
	if err != nil || result.Deleted {
		t.Fatalf("retry Delete() = %+v, %v", result, err)
	}
}

func TestSnapshotIsSortedAndIndependent(t *testing.T) {
	r := NewRegistry()
	mustCreate(t, r, "zeta")
	mustCreate(t, r, "alpha")
	mustSubscribe(t, r, "zeta", "sub-z")
	mustSubscribe(t, r, "alpha", "sub-b")
	mustSubscribe(t, r, "alpha", "sub-a")
	snapshot := r.Snapshot()
	want := Snapshot{Topics: []TopicSnapshot{{Name: "alpha", Subscribers: []SubscriptionID{"sub-a", "sub-b"}}, {Name: "zeta", Subscribers: []SubscriptionID{"sub-z"}}}}
	if !reflect.DeepEqual(snapshot, want) {
		t.Fatalf("Snapshot() = %+v, want %+v", snapshot, want)
	}
	snapshot.Topics[0].Subscribers[0] = "mutated"
	again := r.Snapshot()
	if !reflect.DeepEqual(again, want) {
		t.Fatalf("registry exposed snapshot data: %+v", again)
	}
}

func TestRestoreReplacesControlState(t *testing.T) {
	r := NewRegistry()
	mustCreate(t, r, "old")
	r.Restore(Snapshot{Topics: []TopicSnapshot{{Name: "score", Subscribers: []SubscriptionID{"sub-a"}}}})
	if r.Exists("old") {
		t.Fatal("old topic survived restore")
	}
	subscribers, err := r.Subscribers("score")
	if err != nil || len(subscribers) != 1 || subscribers[0] != "sub-a" {
		t.Fatalf("restored subscribers = %+v, %v", subscribers, err)
	}
}

func mustCreate(t *testing.T, r *Registry, name Name) {
	t.Helper()
	created, err := r.Create(name)
	if err != nil || !created {
		t.Fatalf("Create(%q) = %t, %v", name, created, err)
	}
}

func mustSubscribe(t *testing.T, r *Registry, name Name, id SubscriptionID) {
	t.Helper()
	added, err := r.Subscribe(name, id)
	if err != nil || !added {
		t.Fatalf("Subscribe(%q, %q) = %t, %v", name, id, added, err)
	}
}
