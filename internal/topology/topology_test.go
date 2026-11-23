package topology

import (
	"reflect"
	"testing"
)

func TestPartitionForIsDeterministic(t *testing.T) {
	topology, err := New([]Partition{
		{ID: 0, Epoch: 1, PrimaryAddr: "127.0.0.1:9000", ReplicaAddr: "127.0.0.1:9100"},
		{ID: 1, Epoch: 2, PrimaryAddr: "127.0.0.1:9001", ReplicaAddr: "127.0.0.1:9101"},
	}, 7)
	if err != nil {
		t.Fatal(err)
	}
	first, err := topology.PartitionFor("score")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		got, err := topology.PartitionFor("score")
		if err != nil {
			t.Fatal(err)
		}
		if got != first {
			t.Fatalf("partition changed: first=%+v got=%+v", first, got)
		}
	}
}

func TestPromotionIncrementsEpochAndSwapsRole(t *testing.T) {
	topology, err := New([]Partition{{ID: 0, Epoch: 4, PrimaryAddr: "a", ReplicaAddr: "b"}}, 1)
	if err != nil {
		t.Fatal(err)
	}
	promoted, err := topology.Promote(0)
	if err != nil {
		t.Fatal(err)
	}
	if promoted.Epoch != 5 || promoted.PrimaryAddr != "b" || promoted.ReplicaAddr != "a" {
		t.Fatalf("promoted = %+v", promoted)
	}
}

func TestSnapshotIsSortedAndIndependent(t *testing.T) {
	topology, err := New([]Partition{
		{ID: 2, Epoch: 1, PrimaryAddr: "c"},
		{ID: 1, Epoch: 1, PrimaryAddr: "b"},
	}, 3)
	if err != nil {
		t.Fatal(err)
	}
	first := topology.Snapshot()
	if !reflect.DeepEqual(first, Snapshot{Version: 3, Partitions: []Partition{{ID: 1, Epoch: 1, PrimaryAddr: "b"}, {ID: 2, Epoch: 1, PrimaryAddr: "c"}}}) {
		t.Fatalf("snapshot = %+v", first)
	}
	first.Partitions[0].PrimaryAddr = "mutated"
	if got := topology.Snapshot().Partitions[0].PrimaryAddr; got != "b" {
		t.Fatalf("snapshot leaked internal data: %q", got)
	}
}

func TestInvalidTopology(t *testing.T) {
	if _, err := New(nil, 1); err == nil {
		t.Fatal("New(nil) must fail")
	}
	if _, err := New([]Partition{{ID: 0}}, 1); err == nil {
		t.Fatal("empty primary must fail")
	}
	if _, err := New([]Partition{{ID: 0, PrimaryAddr: "a"}, {ID: 0, PrimaryAddr: "b"}}, 1); err == nil {
		t.Fatal("duplicate partition ID must fail")
	}
}
