package api

// ErrorCode is a stable machine-readable outcome for public operations and
// protocol ERROR bodies. Human-readable context belongs in Error.Message.
type ErrorCode uint16

const (
	// ErrorUnknown is used only when no more specific code applies.
	ErrorUnknown ErrorCode = iota
	// ErrorInvalidRequest means a required field or command is invalid.
	ErrorInvalidRequest
	// ErrorUnsupportedVersion means the peer uses an unsupported wire version.
	ErrorUnsupportedVersion
	// ErrorTopicNotFound means the requested topic is absent.
	ErrorTopicNotFound
	// ErrorSessionNotFound means a session cannot be resumed or addressed.
	ErrorSessionNotFound
	// ErrorPayloadTooLarge means a publication exceeds the UDP v1 budget.
	ErrorPayloadTooLarge
	// ErrorThrottled means admission control rejected the publisher rate.
	ErrorThrottled
	// ErrorStaleEpoch means a request reached a previous partition primary.
	ErrorStaleEpoch
	// ErrorUnavailable means a broker cannot currently serve the operation.
	ErrorUnavailable
	// ErrorInternal represents an unexpected broker-side failure.
	ErrorInternal
)

// Error is the public error value returned by the client library. It carries
// a stable code while preserving a concise diagnostic message for callers.
type Error struct {
	Code    ErrorCode
	Message string
}

// Error implements the standard error interface.
func (e Error) Error() string {
	return e.Message
}
