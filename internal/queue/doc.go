// Package queue provides the bounded per-subscriber buffer used by the broker.
//
// The queue is intentionally unaware of UDP, topics, sessions, and wire
// frames. It accepts already sequenced payloads, bounds both memory and item
// count, and represents discarded sequence intervals as GAP events.
package queue
