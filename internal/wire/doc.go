// Package wire defines the binary framing used by the Pub/Sub UDP protocol.
//
// It owns no sockets, sessions, topics, or application types. Its job is to
// validate a datagram boundary and to encode or decode the fixed-size header.
// Message bodies are deliberately left to the next implementation step.
package wire
