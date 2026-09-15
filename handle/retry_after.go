package handle

import (
	"github.com/cenkalti/backoff/v5"
	"github.com/rotisserie/eris"
)

type retryAfterError struct {
	err   error
	after error
}

func (e *retryAfterError) Error() string {
	return e.err.Error()
}

func (e *retryAfterError) Unwrap() []error {
	return []error{e.err, e.after}
}

func withRetryAfter(err error, seconds int) error {
	return eris.Wrap(
		&retryAfterError{err: err, after: backoff.RetryAfter(seconds)},
		"rate limited by telegram api",
	)
}
