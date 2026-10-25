package amqprpc

import "errors"

var (
	// ErrUnavailable means the broker or the service behind it could not be reached.
	ErrUnavailable = errors.New("amqprpc: service unavailable")
	// ErrSource means the reply was malformed.
	ErrSource = errors.New("amqprpc: inconsistent reply")
)
