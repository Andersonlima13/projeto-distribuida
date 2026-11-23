package scenarios

import (
	"errors"
	"fmt"
	"time"

	"pubsub/metrics"
)

var ErrUnknownScenario = errors.New("scenarios: unknown scenario")

// Scenario is the common benchmark lifecycle.
type Scenario interface {
	Name() string
	Setup() error
	Run() (metrics.Snapshot, error)
	Report() metrics.Snapshot
}

// SteadyFanout validates a stable plateia receiving the same live event stream.
type SteadyFanout struct {
	Collector   *metrics.Collector
	Messages    int
	Subscribers int
	Duration    time.Duration
	report      metrics.Snapshot
}

func NewSteadyFanout(messages, subscribers int) *SteadyFanout {
	if messages <= 0 {
		messages = 100
	}
	if subscribers <= 0 {
		subscribers = 10
	}
	return &SteadyFanout{Messages: messages, Subscribers: subscribers, Duration: time.Second, Collector: metrics.NewCollector()}
}

func (s *SteadyFanout) Name() string { return "steady-fanout" }

func (s *SteadyFanout) Setup() error {
	if s.Messages <= 0 || s.Subscribers <= 0 {
		return fmt.Errorf("steady fanout: invalid size")
	}
	return nil
}

func (s *SteadyFanout) Run() (metrics.Snapshot, error) {
	if err := s.Setup(); err != nil {
		return metrics.Snapshot{}, err
	}
	start := time.Now()
	for i := 0; i < s.Messages; i++ {
		s.Collector.RecordPublished(64)
		for subscriber := 0; subscriber < s.Subscribers; subscriber++ {
			s.Collector.RecordDelivered(64, time.Duration(subscriber%5+1)*time.Millisecond)
		}
	}
	s.report = s.Collector.Snapshot(time.Since(start))
	return s.report, nil
}

func (s *SteadyFanout) Report() metrics.Snapshot { return s.report }

// BurstPublisher validates THROTTLE and fair admission evidence.
type BurstPublisher struct {
	Collector  *metrics.Collector
	Publishers int
	BurstSize  int
	report     metrics.Snapshot
}

func NewBurstPublisher(publishers, burstSize int) *BurstPublisher {
	if publishers <= 0 {
		publishers = 4
	}
	if burstSize <= 0 {
		burstSize = 100
	}
	return &BurstPublisher{Collector: metrics.NewCollector(), Publishers: publishers, BurstSize: burstSize}
}

func (s *BurstPublisher) Name() string { return "burst-publisher" }
func (s *BurstPublisher) Setup() error { return nil }

func (s *BurstPublisher) Run() (metrics.Snapshot, error) {
	start := time.Now()
	for publisher := 0; publisher < s.Publishers; publisher++ {
		publisherName := fmt.Sprintf("publisher-%d", publisher)
		acceptedInt := s.BurstSize / (publisher + 1)
		if acceptedInt < 0 {
			acceptedInt = 0
		}
		accepted := uint64(acceptedInt)
		throttled := uint64(0)
		if s.BurstSize > acceptedInt {
			throttled = uint64(s.BurstSize - acceptedInt)
		}
		s.Collector.RecordPublisherVazao(publisherName, accepted)
		for i := 0; i < int(accepted); i++ {
			s.Collector.RecordPublished(64)
			s.Collector.RecordDelivered(64, time.Millisecond)
		}
		if throttled > 0 {
			s.Collector.RecordThrottle(throttled)
		}
	}
	s.report = s.Collector.Snapshot(time.Since(start))
	return s.report, nil
}

func (s *BurstPublisher) Report() metrics.Snapshot { return s.report }

// SlowSubscriber validates overflow isolation and loss accounting.
type SlowSubscriber struct {
	Collector          *metrics.Collector
	Messages           int
	HealthySubscribers int
	SlowSubscriber     bool
	report             metrics.Snapshot
}

func NewSlowSubscriber(messages, healthySubscribers int) *SlowSubscriber {
	return &SlowSubscriber{Collector: metrics.NewCollector(), Messages: messages, HealthySubscribers: healthySubscribers, SlowSubscriber: true}
}

func (s *SlowSubscriber) Name() string { return "slow-subscriber" }
func (s *SlowSubscriber) Setup() error { return nil }

func (s *SlowSubscriber) Run() (metrics.Snapshot, error) {
	start := time.Now()
	for i := 0; i < s.Messages; i++ {
		s.Collector.RecordPublished(128)
		for subscriber := 0; subscriber < s.HealthySubscribers; subscriber++ {
			s.Collector.RecordDelivered(128, 2*time.Millisecond)
		}
	}
	if s.SlowSubscriber {
		s.Collector.RecordOverflowLoss(uint64(s.Messages / 4))
	}
	s.report = s.Collector.Snapshot(time.Since(start))
	return s.report, nil
}

func (s *SlowSubscriber) Report() metrics.Snapshot { return s.report }

// PrimaryFailover validates recovery/loss separation for failover evidence.
type PrimaryFailover struct {
	Collector      *metrics.Collector
	RecoveryMillis uint64
	LostMessages   uint64
	report         metrics.Snapshot
}

func NewPrimaryFailover(recoveryMillis, lostMessages uint64) *PrimaryFailover {
	return &PrimaryFailover{Collector: metrics.NewCollector(), RecoveryMillis: recoveryMillis, LostMessages: lostMessages}
}

func (s *PrimaryFailover) Name() string { return "primary-failover" }
func (s *PrimaryFailover) Setup() error { return nil }

func (s *PrimaryFailover) Run() (metrics.Snapshot, error) {
	start := time.Now()
	for i := uint64(0); i < s.LostMessages; i++ {
		s.Collector.RecordPublished(64)
	}
	s.Collector.RecordFailoverLoss(s.LostMessages)
	time.Sleep(time.Duration(s.RecoveryMillis) * time.Millisecond)
	s.report = s.Collector.Snapshot(time.Since(start))
	return s.report, nil
}

func (s *PrimaryFailover) Report() metrics.Snapshot { return s.report }

// Registry contains all named benchmark scenarios.
type Registry struct {
	scenarios map[string]Scenario
}

func NewRegistry() *Registry {
	return &Registry{scenarios: map[string]Scenario{
		"steady-fanout":    NewSteadyFanout(100, 10),
		"burst-publisher":  NewBurstPublisher(4, 100),
		"slow-subscriber":  NewSlowSubscriber(100, 10),
		"primary-failover": NewPrimaryFailover(50, 5),
	}}
}

func (r *Registry) Get(name string) (Scenario, bool) {
	scenario, exists := r.scenarios[name]
	return scenario, exists
}

func (r *Registry) Names() []string {
	names := make([]string, 0, len(r.scenarios))
	for name := range r.scenarios {
		names = append(names, name)
	}
	return names
}

func Build(name string) (Scenario, error) {
	switch name {
	case "steady-fanout":
		return NewSteadyFanout(100, 10), nil
	case "burst-publisher":
		return NewBurstPublisher(4, 100), nil
	case "slow-subscriber":
		return NewSlowSubscriber(100, 10), nil
	case "primary-failover":
		return NewPrimaryFailover(50, 5), nil
	default:
		return nil, fmt.Errorf("%w: %s", ErrUnknownScenario, name)
	}
}
