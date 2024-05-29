package customerror

import "errors"

// The following errors serve as domain errors that can be used by the
// different layers. The middlewares in the entrypoint will intercept these and
// convert them to the relevant gRPC / HTTP codes.
var (
	// Permission denied.
	ErrNoPermission = errors.New("no permission")
	// Not found.
	ErrNotFound = errors.New("not found")
	// Unauthenticated.
	ErrUnauthenticated = errors.New("unauthenticated")
	// Rate limit exceeded.
	ErrRateLimiting = errors.New("rate limiting")
	// Batch size exceeded.
	ErrExceedMaxBatchSize = errors.New("the batch size can not exceed 32")
)
