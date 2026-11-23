package scenarios

import (
	"errors"
	"testing"
)

func TestSteadyFanoutReport(t *testing.T) {
	scenario := NewSteadyFanout(10, 3)
	report, err := scenario.Run()
	if err != nil {
		t.Fatal(err)
	}
	if report.Published != 10 || report.Delivered != 30 {
		t.Fatalf("report = %+v", report)
	}
}

func TestBurstFairnessIsDefined(t *testing.T) {
	scenario := NewBurstPublisher(4, 100)
	report, err := scenario.Run()
	if err != nil {
		t.Fatal(err)
	}
	if report.Throttled == 0 || report.JainIndex <= 0 {
		t.Fatalf("report = %+v", report)
	}
}

func TestSlowSubscriberReportsOverflow(t *testing.T) {
	scenario := NewSlowSubscriber(100, 5)
	report, err := scenario.Run()
	if err != nil {
		t.Fatal(err)
	}
	if report.OverflowLoss == 0 {
		t.Fatalf("report = %+v", report)
	}
}

func TestUnknownScenario(t *testing.T) {
	if _, err := Build("missing"); !errors.Is(err, ErrUnknownScenario) {
		t.Fatalf("error = %v", err)
	}
}
