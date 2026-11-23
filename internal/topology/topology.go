package topology

import (
	"errors"
	"fmt"
	"hash/fnv"
	"sort"
	"sync"
)

var (
	ErrInvalidTopology = errors.New("topology: invalid static topology")
	ErrPartitionMissing = errors.New("topology: partition not found")
)

// Partition defines primary and replica ownership for one static partition.
type Partition struct {
	ID            uint32
	Epoch         uint64
	PrimaryAddr   string
	ReplicaAddr   string
}

// Snapshot is a copy-only representation of cluster topology.
type Snapshot struct {
	Version    uint64
	Partitions []Partition
}

// Topology is the concurrency-safe static route table.
type Topology struct {
	mu sync.RWMutex
	version uint64
	partitions map[uint32]Partition
	vNodes []ringNode
}

type ringNode struct {
	hash uint64
	partition uint32
}

const defaultVirtualNodes = 128

// New creates a static topology and validates unique partition IDs/endpoints.
func New(partitions []Partition, version uint64) (*Topology, error) {
	if len(partitions) == 0 {
		return nil, fmt.Errorf("%w: no partitions", ErrInvalidTopology)
	}
	t := &Topology{version: version, partitions: make(map[uint32]Partition)}
	for _, partition := range partitions {
		if partition.PrimaryAddr == "" {
			return nil, fmt.Errorf("%w: partition %d has empty primary", ErrInvalidTopology, partition.ID)
		}
		if _, exists := t.partitions[partition.ID]; exists {
			return nil, fmt.Errorf("%w: duplicate partition %d", ErrInvalidTopology, partition.ID)
		}
		t.partitions[partition.ID] = partition
	}
	t.buildRing(defaultVirtualNodes)
	return t, nil
}

func (t *Topology) buildRing(virtualNodes int) {
	t.vNodes = make([]ringNode, 0, len(t.partitions)*virtualNodes)
	for id := range t.partitions {
		for vnode := 0; vnode < virtualNodes; vnode++ {
			point := fmt.Sprintf("partition-%d-vnode-%d", id, vnode)
			t.vNodes = append(t.vNodes, ringNode{hash: fnv64(point), partition: id})
		}
	}
	sort.Slice(t.vNodes, func(i, j int) bool { return t.vNodes[i].hash < t.vNodes[j].hash })
}

// PartitionFor returns the deterministic partition for a topic name.
func (t *Topology) PartitionFor(topic string) (Partition, error) {
	if topic == "" {
		return Partition{}, fmt.Errorf("%w: empty topic", ErrInvalidTopology)
	}
	if t == nil {
		return Partition{}, fmt.Errorf("%w: topology is nil", ErrInvalidTopology)
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	if len(t.vNodes) == 0 {
		return Partition{}, ErrPartitionMissing
	}
	hash := fnv64(topic)
	left, right := 0, len(t.vNodes)
	for left < right {
		middle := int(uint(left+right) >> 1)
		if t.vNodes[middle].hash < hash {
			left = middle + 1
		} else {
			right = middle
		}
	}
	if left == len(t.vNodes) {
		left = 0
		}
	partition, exists := t.partitions[t.vNodes[left].partition]
	if !exists {
		return Partition{}, ErrPartitionMissing
	}
	return partition, nil
}

// PartitionByID returns one partition by ID.
func (t *Topology) PartitionByID(id uint32) (Partition, bool) {
	if t == nil {
		return Partition{}, false
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	partition, exists := t.partitions[id]
	return partition, exists
}

// Promote increments epoch and switches primary/replica for a partition.
func (t *Topology) Promote(id uint32) (Partition, error) {
	if t == nil {
		return Partition{}, fmt.Errorf("%w: topology is nil", ErrInvalidTopology)
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	partition, exists := t.partitions[id]
	if !exists {
		return Partition{}, fmt.Errorf("%w: %d", ErrPartitionMissing, id)
	}
	oldPrimary := partition.PrimaryAddr
	partition.PrimaryAddr = partition.ReplicaAddr
	partition.ReplicaAddr = oldPrimary
	partition.Epoch++
	t.partitions[id] = partition
	return partition, nil
}

// Snapshot returns a deterministic copy of the topology.
func (t *Topology) Snapshot() Snapshot {
	if t == nil {
		return Snapshot{}
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	ids := make([]int, 0, len(t.partitions))
	for id := range t.partitions {
		ids = append(ids, int(id))
	}
	sort.Ints(ids)
	result := Snapshot{Version: t.version, Partitions: make([]Partition, 0, len(ids))}
	for _, id := range ids {
		result.Partitions = append(result.Partitions, t.partitions[uint32(id)])
	}
	return result
}

// Version returns the topology snapshot version.
func (t *Topology) Version() uint64 {
	if t == nil {
		return 0
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.version
}

func fnv64(value string) uint64 {
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(value))
	return hash.Sum64()
}
