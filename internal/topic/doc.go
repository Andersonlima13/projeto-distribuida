// Package topic manages dynamic topic and subscription control state.
//
// It does not own subscriber queues, transport connections, delivery workers,
// or degradation state. The broker uses the subscription IDs returned here to
// locate those runtime resources in their respective modules.
package topic
