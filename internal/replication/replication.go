package replication

import (
	"errors"
	"sync"
	"time"
)

var (
	ErrInvalidTimeout = errors.New("replication: failure timeout must be positive")
)

// Heartbeat tracks liveness for primary-replica or broker-broker links.
type Heartbeat struct {
	mu       sync.Mutex
	timeout  time.Duration
	lastSeen time.Time
	promoted bool
}

func NewHeartbeat(timeout time.Duration) (*Heartbeat, error) {
	if timeout <= 0 {
		return nil, ErrInvalidTimeout
	}
	return &Heartbeat{timeout: timeout}, nil
}

// Mark records one heartbeat observation.
func (h *Heartbeat) Mark(now time.Time) {
	if h == nil {
		return
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	h.mu.Lock()
	h.lastSeen = now
	h.mu.Unlock()
}

// Suspect reports whether timeout elapsed since the last heartbeat.
func (h *Heartbeat) Suspect(now time.Time) bool {
	if h == nil {
		return false
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.lastSeen.IsZero() {
		return false
	}
	return now.Sub(h.lastSeen) >= h.timeout
}

// Promote claims failover once and returns true only for the caller that wins it.
func (h *Heartbeat) Promote() bool {
	if h == nil {
		return false
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.promoted {
		return false
	}
	h.promoted = true
	return true
}

func (h *Heartbeat) Promoted() bool {
	if h == nil {
		return false
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.promoted
}

// ControlSnapshot is the minimum asynchronous-replicated partition state.
type ControlSnapshot struct {
	Epoch        uint64
	TopicState   any
	SessionState any
}

// Store keeps the last control snapshot received from a primary.
type Store struct {
	mu       sync.RWMutex
	snapshot ControlSnapshot
}

func NewStore() *Store {
	return &Store{}
}

func (s *Store) Apply(snapshot ControlSnapshot) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.snapshot = snapshot
	s.mu.Unlock()
}

func (s *Store) Snapshot() ControlSnapshot {
	if s == nil {
		return ControlSnapshot{}
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.snapshot
}
