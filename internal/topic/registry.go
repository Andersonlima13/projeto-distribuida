package topic

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
)

var (
	// ErrInvalidTopicName means a topic cannot be represented by the protocol.
	ErrInvalidTopicName = errors.New("topic: invalid topic name")
	// ErrTopicNotFound means an operation requires a topic that does not exist.
	ErrTopicNotFound = errors.New("topic: topic not found")
	// ErrInvalidSubscriptionID means a subscription identifier is empty or malformed.
	ErrInvalidSubscriptionID = errors.New("topic: invalid subscription ID")
	// ErrSubscriptionBoundElsewhere means an ID is already assigned to another topic.
	ErrSubscriptionBoundElsewhere = errors.New("topic: subscription already belongs to another topic")
)

// Name is a flat, case-sensitive topic identifier. The registry does not give
// special meaning to separators; hierarchical matching is outside v1 scope.
type Name string

// SubscriptionID identifies one subscriber registration. IDs are globally
// unique inside a Registry so the broker can resolve them unambiguously.
type SubscriptionID string

// DeleteResult describes the control-plane effect of deleting a topic.
// Subscribers must receive TOPIC_CLOSED when Deleted is true.
type DeleteResult struct {
	Deleted     bool
	Subscribers []SubscriptionID
}

// Snapshot is a deterministic, copy-only representation of registry state.
// Replication can serialize it without accessing Registry internals.
type Snapshot struct {
	Topics []TopicSnapshot
}

// TopicSnapshot holds the control state of one topic.
type TopicSnapshot struct {
	Name        Name
	Subscribers []SubscriptionID
}

// Registry stores only topic existence and subscription relationships. All
// methods are concurrency-safe and return copies of internal slices.
type Registry struct {
	mu sync.RWMutex

	topics          map[Name]map[SubscriptionID]struct{}
	subscriptionFor map[SubscriptionID]Name
}

// NewRegistry returns an empty dynamic topic registry.
func NewRegistry() *Registry {
	return &Registry{
		topics:          make(map[Name]map[SubscriptionID]struct{}),
		subscriptionFor: make(map[SubscriptionID]Name),
	}
}

// Create creates name when absent. Calling Create again for the same valid
// name is successful and returns false, making control-plane retries safe.
func (r *Registry) Create(name Name) (created bool, err error) {
	if err := validateName(name); err != nil {
		return false, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.topics[name]; exists {
		return false, nil
	}
	r.topics[name] = make(map[SubscriptionID]struct{})
	return true, nil
}

// Delete removes name and all of its subscriptions. Repeating a delete is a
// no-op and returns Deleted false, which makes DELETE_TOPIC retry-safe.
func (r *Registry) Delete(name Name) (DeleteResult, error) {
	if err := validateName(name); err != nil {
		return DeleteResult{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	subscribers, exists := r.topics[name]
	if !exists {
		return DeleteResult{}, nil
	}
	result := DeleteResult{Deleted: true, Subscribers: sortedSubscribers(subscribers)}
	for id := range subscribers {
		delete(r.subscriptionFor, id)
	}
	delete(r.topics, name)
	return result, nil
}

// Subscribe adds id to name. Repeating the same pair is a no-op and returns
// false. Reusing an ID for a different topic is rejected because a delivery
// worker must map one subscription ID to exactly one topic.
func (r *Registry) Subscribe(name Name, id SubscriptionID) (added bool, err error) {
	if err := validateName(name); err != nil {
		return false, err
	}
	if err := validateSubscriptionID(id); err != nil {
		return false, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	subscribers, exists := r.topics[name]
	if !exists {
		return false, fmt.Errorf("%w: %q", ErrTopicNotFound, name)
	}
	if boundTopic, used := r.subscriptionFor[id]; used {
		if boundTopic == name {
			return false, nil
		}
		return false, fmt.Errorf("%w: %q is bound to %q", ErrSubscriptionBoundElsewhere, id, boundTopic)
	}
	subscribers[id] = struct{}{}
	r.subscriptionFor[id] = name
	return true, nil
}

// Unsubscribe removes id from name. A repeated unsubscribe for an absent ID is
// a no-op; an ID bound to another topic is an explicit caller error.
func (r *Registry) Unsubscribe(name Name, id SubscriptionID) (removed bool, err error) {
	if err := validateName(name); err != nil {
		return false, err
	}
	if err := validateSubscriptionID(id); err != nil {
		return false, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	boundTopic, exists := r.subscriptionFor[id]
	if !exists {
		return false, nil
	}
	if boundTopic != name {
		return false, fmt.Errorf("%w: %q is bound to %q", ErrSubscriptionBoundElsewhere, id, boundTopic)
	}
	delete(r.subscriptionFor, id)
	delete(r.topics[name], id)
	return true, nil
}

// Subscribers returns a sorted copy of the IDs currently subscribed to name.
func (r *Registry) Subscribers(name Name) ([]SubscriptionID, error) {
	if err := validateName(name); err != nil {
		return nil, err
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	subscribers, exists := r.topics[name]
	if !exists {
		return nil, fmt.Errorf("%w: %q", ErrTopicNotFound, name)
	}
	return sortedSubscribers(subscribers), nil
}

// Exists reports whether a valid topic currently exists.
func (r *Registry) Exists(name Name) bool {
	if validateName(name) != nil {
		return false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, exists := r.topics[name]
	return exists
}

// Snapshot returns a deterministic deep copy of control state, sorted by topic
// name and subscription ID for stable replication and tests.
func (r *Registry) Snapshot() Snapshot {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]Name, 0, len(r.topics))
	for name := range r.topics {
		names = append(names, name)
	}
	sort.Slice(names, func(i, j int) bool { return names[i] < names[j] })
	snapshot := Snapshot{Topics: make([]TopicSnapshot, 0, len(names))}
	for _, name := range names {
		snapshot.Topics = append(snapshot.Topics, TopicSnapshot{Name: name, Subscribers: sortedSubscribers(r.topics[name])})
	}
	return snapshot
}

// Restore replaces control state from a replicated deterministic snapshot.
func (r *Registry) Restore(snapshot Snapshot) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.topics = make(map[Name]map[SubscriptionID]struct{})
	r.subscriptionFor = make(map[SubscriptionID]Name)
	for _, topicSnapshot := range snapshot.Topics {
		name := topicSnapshot.Name
		if validateName(name) != nil {
			continue
		}
		r.topics[name] = make(map[SubscriptionID]struct{})
		for _, id := range topicSnapshot.Subscribers {
			if validateSubscriptionID(id) != nil {
				continue
			}
			r.topics[name][id] = struct{}{}
			r.subscriptionFor[id] = name
		}
	}
}

func validateName(name Name) error {
	value := string(name)
	if strings.TrimSpace(value) == "" || strings.IndexByte(value, 0) >= 0 {
		return fmt.Errorf("%w: %q", ErrInvalidTopicName, name)
	}
	return nil
}

func validateSubscriptionID(id SubscriptionID) error {
	value := string(id)
	if strings.TrimSpace(value) == "" || strings.IndexByte(value, 0) >= 0 {
		return fmt.Errorf("%w: %q", ErrInvalidSubscriptionID, id)
	}
	return nil
}

func sortedSubscribers(subscribers map[SubscriptionID]struct{}) []SubscriptionID {
	ids := make([]SubscriptionID, 0, len(subscribers))
	for id := range subscribers {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}
