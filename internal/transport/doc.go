// Package transport provides UDP datagram input and output for the middleware.
//
// It enforces the v1 datagram budget but intentionally does not decode wire
// frames, manage sessions, retry control messages, or route topics. Those
// responsibilities belong to wire, session, and broker respectively.
package transport
