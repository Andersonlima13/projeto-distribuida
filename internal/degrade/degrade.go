package degrade

import (
	"errors"
	"sync"
	"time"
)

var ErrInvalidConfig = errors.New("degrade: invalid configuration")

type Level uint8

const (
	LevelNormal     Level = 1
	LevelPressure   Level = 2
	LevelPersistent Level = 3
)

// Config defines pressure threshold and persistent slow duration.
type Config struct {
	PressureMessages int
	PressureBytes    int
	PersistentFor    time.Duration
}

// Monitor tracks one subscriber queue level.
type Monitor struct {
	mu sync.Mutex

	config        Config
	level         Level
	pressureSince time.Time
}

func New(config Config) (*Monitor, error) {
	if config.PressureMessages <= 0 || config.PressureBytes <= 0 || config.PersistentFor <= 0 {
		return nil, ErrInvalidConfig
	}
	return &Monitor{config: config, level: LevelNormal}, nil
}

// Observe updates the level from queued depth and the amount of time in pressure.
func (m *Monitor) Observe(queuedMessages, queuedBytes int, now time.Time) Level {
	m.mu.Lock()
	defer m.mu.Unlock()

	pressured := queuedMessages >= m.config.PressureMessages || queuedBytes >= m.config.PressureBytes
	if !pressured {
		m.level = LevelNormal
		m.pressureSince = time.Time{}
		return m.level
	}
	if m.level == LevelNormal {
		m.level = LevelPressure
		m.pressureSince = now
		return m.level
	}
	if m.level == LevelPressure && !m.pressureSince.IsZero() && now.Sub(m.pressureSince) >= m.config.PersistentFor {
		m.level = LevelPersistent
	}
	return m.level
}

func (m *Monitor) Level() Level {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.level
}
