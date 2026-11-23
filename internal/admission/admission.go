package admission

import (
	"errors"
	"sync"
	"time"
)

var ErrInvalidConfig = errors.New("admission: invalid configuration")

// TokenBucketConfig defines average rate and burst capacity per publisher.
type TokenBucketConfig struct {
	RatePerSecond float64
	Burst         float64
}

// Bucket is a concurrency-safe token bucket.
type Bucket struct {
	mu sync.Mutex

	capacity float64
	refill   float64
	tokens   float64
	last     time.Time
}

func NewBucket(config TokenBucketConfig, now time.Time) (*Bucket, error) {
	if config.RatePerSecond <= 0 || config.Burst <= 0 {
		return nil, ErrInvalidConfig
	}
	if now.IsZero() {
		now = time.Now()
	}
	return &Bucket{capacity: config.Burst, refill: config.RatePerSecond, tokens: config.Burst, last: now}, nil
}

// Allow consumes one token. It returns false when the publisher burst/rate is exceeded.
func (b *Bucket) Allow(now time.Time) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if now.IsZero() {
		now = time.Now()
	}
	elapsed := now.Sub(b.last).Seconds()
	if elapsed > 0 {
		b.tokens += elapsed * b.refill
		if b.tokens > b.capacity {
			b.tokens = b.capacity
		}
		b.last = now
	}
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// Snapshot reports the remaining token count for metrics.
func (b *Bucket) Snapshot(now time.Time) float64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	if now.IsZero() {
		now = time.Now()
	}
	elapsed := now.Sub(b.last).Seconds()
	tokens := b.tokens + elapsed*b.refill
	if tokens > b.capacity {
		tokens = b.capacity
	}
	return tokens
}

// DRR implements deficit round robin for publisher ingress scheduling.
type DRR struct {
	mu      sync.Mutex
	quantum uint32
	state   map[string]*drrPublisher
	order   []string
}

type drrPublisher struct {
	deficit uint32
}

func NewDRR(quantum uint32) (*DRR, error) {
	if quantum == 0 {
		return nil, ErrInvalidConfig
	}
	return &DRR{quantum: quantum, state: make(map[string]*drrPublisher)}, nil
}

// Add registers a publisher and activates fair scheduling.
func (d *DRR) Add(publisher string) {
	if publisher == "" {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, exists := d.state[publisher]; exists {
		return
	}
	d.state[publisher] = &drrPublisher{}
	d.order = append(d.order, publisher)
}

// Next returns the next registered publisher and adds one quantum to its deficit.
// The caller should consume one admitted packet and subtract one cost from the publisher deficit.
func (d *DRR) Next() (string, uint32, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.order) == 0 {
		return "", 0, false
	}
	publisher := d.order[0]
	d.order = d.order[1:]
	state := d.state[publisher]
	state.deficit += d.quantum
	d.order = append(d.order, publisher)
	return publisher, state.deficit, true
}

// Consume subtracts packet cost from a publisher deficit.
func (d *DRR) Consume(publisher string, cost uint32) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if state, exists := d.state[publisher]; exists {
		if cost >= state.deficit {
			state.deficit = 0
		} else {
			state.deficit -= cost
		}
	}
}

// Stats reports admission scheduler state.
type Stats struct {
	Publishers int
	Quantum    uint32
}

func (d *DRR) Stats() Stats {
	d.mu.Lock()
	defer d.mu.Unlock()
	return Stats{Publishers: len(d.state), Quantum: d.quantum}
}
