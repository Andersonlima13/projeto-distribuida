package metrics

import (
	"encoding/json"
	"os"
	"testing"
	"time"
)

func TestPercentilesAndSnapshot(t *testing.T) {
	latencies := []time.Duration{1 * time.Millisecond, 2 * time.Millisecond, 100 * time.Millisecond}
	collector := NewCollector()
	collector.RecordPublished(10)
	collector.RecordDelivered(10, latencies[0])
	collector.RecordDelivered(10, latencies[1])
	collector.RecordDelivered(10, latencies[2])
	collector.RecordOverflowLoss(2)
	collector.RecordPublisherVazao("a", 10)
	collector.RecordPublisherVazao("b", 5)

	if Percentile(latencies, 0.99) <= 2*time.Millisecond {
		t.Fatal("p99 calculation is too low")
	}
	snapshot := collector.Snapshot(time.Second)
	if snapshot.Published != 1 || snapshot.Delivered != 3 || snapshot.OverflowLoss != 2 {
		t.Fatalf("snapshot = %+v", snapshot)
	}
	if snapshot.JainIndex <= 0.8 || snapshot.JainIndex > 1 {
		t.Fatalf("Jain index = %f", snapshot.JainIndex)
	}
}

func TestWriteJSONAndCSV(t *testing.T) {
	dir := t.TempDir()
	report := Snapshot{Published: 10, Delivered: 8, P50Millis: 1.25, JainIndex: 0.9}
	jsonPath := dir + "/report.json"
	csvPath := dir + "/report.csv"
	if err := WriteJSON(jsonPath, report); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(jsonPath)
	if err != nil {
		t.Fatal(err)
	}
	var decoded Snapshot
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	if err := WriteCSV(csvPath, report); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(csvPath); err != nil {
		t.Fatal(err)
	}
}
