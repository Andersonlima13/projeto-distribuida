package metrics

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"sync"
	"time"
)

var ErrInvalidReport = errors.New("metrics: invalid report")

// Collector records delivery latency and aggregate throughput/loss evidence.
type Collector struct {
	mu             sync.Mutex
	latencies      []time.Duration
	published      uint64
	publishedBytes uint64
	delivered      uint64
	deliveredBytes uint64
	overflowLoss   uint64
	failoverLoss   uint64
	networkLoss    uint64
	throttled      uint64
	perPublisher   map[string]uint64
}

func NewCollector() *Collector {
	return &Collector{perPublisher: make(map[string]uint64)}
}

func (c *Collector) RecordPublished(bytes int) {
	c.mu.Lock()
	c.published++
	c.publishedBytes += uint64(bytes)
	c.mu.Unlock()
}

func (c *Collector) RecordDelivered(bytes int, latency time.Duration) {
	c.mu.Lock()
	c.delivered++
	c.deliveredBytes += uint64(bytes)
	c.latencies = append(c.latencies, latency)
	c.mu.Unlock()
}

func (c *Collector) RecordOverflowLoss(count uint64) {
	c.mu.Lock()
	c.overflowLoss += count
	c.mu.Unlock()
}

func (c *Collector) RecordFailoverLoss(count uint64) {
	c.mu.Lock()
	c.failoverLoss += count
	c.mu.Unlock()
}

func (c *Collector) RecordNetworkLoss(count uint64) {
	c.mu.Lock()
	c.networkLoss += count
	c.mu.Unlock()
}

func (c *Collector) RecordThrottle(count uint64) {
	c.mu.Lock()
	c.throttled += count
	c.mu.Unlock()
}

func (c *Collector) RecordPublisherVazao(publisher string, count uint64) {
	c.mu.Lock()
	c.perPublisher[publisher] += count
	c.mu.Unlock()
}

// Snapshot is a copy-only report suitable for JSON/CSV output.
type Snapshot struct {
	Published      uint64        `json:"published"`
	PublishedBytes uint64        `json:"published_bytes"`
	Delivered      uint64        `json:"delivered"`
	DeliveredBytes uint64        `json:"delivered_bytes"`
	OverflowLoss   uint64        `json:"overflow_loss"`
	FailoverLoss   uint64        `json:"failover_loss"`
	NetworkLoss    uint64        `json:"network_loss"`
	Throttled      uint64        `json:"throttled"`
	P50Millis      float64       `json:"p50_millis"`
	P99Millis      float64       `json:"p99_millis"`
	P999Millis     float64       `json:"p99_9_millis"`
	JainIndex      float64       `json:"jain_index"`
	Duration       time.Duration `json:"duration_ns"`
}

func Percentile(values []time.Duration, percentile float64) time.Duration {
	if len(values) == 0 {
		return 0
	}
	copied := append([]time.Duration(nil), values...)
	sort.Slice(copied, func(i, j int) bool { return copied[i] < copied[j] })
	if percentile <= 0 {
		return copied[0]
	}
	if percentile >= 1 {
		return copied[len(copied)-1]
	}
	rank := int((percentile * float64(len(copied))) + 0.999999)
	if rank < 1 {
		rank = 1
	}
	if rank > len(copied) {
		rank = len(copied)
	}
	return copied[rank-1]
}

// JainIndex calculates fairness over accepted throughput per publisher.
func JainIndex(values map[string]uint64) float64 {
	if len(values) == 0 {
		return 0
	}
	sum, squareSum := 0.0, 0.0
	count := 0.0
	for _, value := range values {
		x := float64(value)
		sum += x
		squareSum += x * x
		count++
	}
	if squareSum == 0 {
		return 0
	}
	return (sum * sum) / (count * squareSum)
}

func (c *Collector) Snapshot(duration time.Duration) Snapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	latencies := append([]time.Duration(nil), c.latencies...)
	publisherValues := make(map[string]uint64, len(c.perPublisher))
	for publisher, count := range c.perPublisher {
		publisherValues[publisher] = count
	}
	return Snapshot{
		Published:      c.published,
		PublishedBytes: c.publishedBytes,
		Delivered:      c.delivered,
		DeliveredBytes: c.deliveredBytes,
		OverflowLoss:   c.overflowLoss,
		FailoverLoss:   c.failoverLoss,
		NetworkLoss:    c.networkLoss,
		Throttled:      c.throttled,
		P50Millis:      float64(Percentile(latencies, 0.50)) / float64(time.Millisecond),
		P99Millis:      float64(Percentile(latencies, 0.99)) / float64(time.Millisecond),
		P999Millis:     float64(Percentile(latencies, 0.999)) / float64(time.Millisecond),
		JainIndex:      JainIndex(publisherValues),
		Duration:       duration,
	}
}

func WriteJSON(path string, report Snapshot) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	return encoder.Encode(report)
}

func WriteCSV(path string, report Snapshot) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()
	writer := csv.NewWriter(file)
	defer writer.Flush()
	if err := writer.Write([]string{"published", "published_bytes", "delivered", "delivered_bytes", "overflow_loss", "failover_loss", "network_loss", "throttled", "p50_ms", "p99_ms", "p99_9_ms", "jain", "duration_ns"}); err != nil {
		return err
	}
	return writer.Write([]string{
		itoa(report.Published), itoa(report.PublishedBytes), itoa(report.Delivered), itoa(report.DeliveredBytes),
		itoa(report.OverflowLoss), itoa(report.FailoverLoss), itoa(report.NetworkLoss), itoa(report.Throttled),
		formatFloat(report.P50Millis), formatFloat(report.P99Millis), formatFloat(report.P999Millis),
		formatFloat(report.JainIndex), itoa(uint64(report.Duration)),
	})
}

func itoa(value uint64) string {
	return fmt.Sprint(value)
}

func formatFloat(value float64) string {
	return fmt.Sprintf("%.6f", value)
}
