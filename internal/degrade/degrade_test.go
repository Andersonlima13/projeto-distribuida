package degrade

import (
	"testing"
	"time"
)

func TestDegradationEscalatesAndReturnsToNormal(t *testing.T) {
	m, err := New(Config{PressureMessages: 2, PressureBytes: 100, PersistentFor: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	start := time.Unix(100, 0)
	if got := m.Observe(2, 10, start); got != LevelPressure {
		t.Fatalf("level = %d, want pressure", got)
	}
	if got := m.Observe(2, 10, start.Add(time.Second)); got != LevelPersistent {
		t.Fatalf("level = %d, want persistent", got)
	}
	if got := m.Observe(0, 0, start.Add(2*time.Second)); got != LevelNormal {
		t.Fatalf("level = %d, want normal", got)
	}
}

func TestInvalidConfig(t *testing.T) {
	if _, err := New(Config{}); err != ErrInvalidConfig {
		t.Fatalf("error = %v", err)
	}
}
