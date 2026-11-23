package metrics

import (
	"sync"
	"time"
)

// Monitor is a sliding-window latency recorder for live operational data.
type Monitor struct {
	mu       sync.Mutex
	window   []time.Duration
	capacity int
}

func NewMonitor(capacity int) *Monitor {
	if capacity <= 0 {
		capacity = 4096
	}
	return &Monitor{capacity: capacity, window: make([]time.Duration, 0, capacity)}
}

func (m *Monitor) Record(latency time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.window) == m.capacity {
		copy(m.window, m.window[1:])
		m.window[len(m.window)-1] = latency
		return
	}
	m.window = append(m.window, latency)
}

func (m *Monitor) Snapshot() (p50, p99, p999 time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	values := append([]time.Duration(nil), m.window...)
	return Percentile(values, 0.50), Percentile(values, 0.99), Percentile(values, 0.999)
}
