// Package session manages in-memory client sessions for one broker node.
//
// It tracks stable client identity, broker-assigned session IDs, and the
// latest observed UDP address so control and delivery messages can be routed
// back to the right peer.
package session
