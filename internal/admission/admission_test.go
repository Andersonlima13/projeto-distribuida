package admission

import (
	"testing"
	"time"
)

func TestTokenBucketAllowsBurstAndRefills(t *testing.T) {
	start := time.Unix(100, 0)
	bucket, err := NewBucket(TokenBucketConfig{RatePerSecond: 10, Burst: 2}, start)
	if err != nil {
		t.Fatal(err)
	}
	if !bucket.Allow(start) {
		t.Fatal("first burst token must be allowed")
	}
	if !bucket.Allow(start) {
		t.Fatal("second burst token must be allowed")
	}
	if bucket.Allow(start) {
		t.Fatal("bucket must reject after burst")
	}
	if !bucket.Allow(start.Add(100 * time.Millisecond)) {
		t.Fatal("one token should refill after 100ms")
	}
}

func TestDRRRotatesPublishers(t *testing.T) {
	drr, err := NewDRR(10)
	if err != nil {
		t.Fatal(err)
	}
	drr.Add("a")
	drr.Add("b")
	first, _, _ := drr.Next()
	second, _, _ := drr.Next()
	if first != "a" || second != "b" {
		t.Fatalf("round = %q, %q; want a, b", first, second)
	}
}

func TestInvalidAdmissionConfig(t *testing.T) {
	if _, err := NewBucket(TokenBucketConfig{}, time.Time{}); err != ErrInvalidConfig {
		t.Fatalf("bucket error = %v", err)
	}
	if _, err := NewDRR(0); err != ErrInvalidConfig {
		t.Fatalf("DRR error = %v", err)
	}
}
