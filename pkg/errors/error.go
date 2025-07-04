// package errors contains domain errors that different layers can use to add
// meaning to an error and that middleware can transform to a status code or
// retry policy. This is implemented as a separate package in order to avoid
// cycle import errors.
//
// TODO When transforming domain errors to response codes, the middleware
// package should eventually use these errors only.
package errors

import (
	"fmt"

	"github.com/instill-ai/x/errmsg"
)

// The following errors serve as domain errors that can be used by the
// different layers. The middlewares in the entrypoint will intercept these and
// convert them to the relevant gRPC / HTTP codes.
var (
	// ErrInvalidArgument is used when the provided argument is incorrect (e.g.
	// format, reserved).
	ErrInvalidArgument = fmt.Errorf("invalid")
	// ErrNotFound is used when a resource doesn't exist.
	ErrNotFound = fmt.Errorf("not found")
	// ErrInvalidCloneTarget is used when the pipeline clone target is not
	// valid. The format should be `<user-id>/<pipeline-id>` or
	// `<org-id>/<pipeline-id>`.
	ErrInvalidCloneTarget = fmt.Errorf("invalid target")
	// ErrUnauthorized is used when a request can't be performed due to
	// insufficient permissions.
	ErrUnauthorized = fmt.Errorf("unauthorized")
	// ErrAlreadyExists is used when a resource can't be created because it
	// already exists.
	ErrAlreadyExists = errmsg.AddMessage(fmt.Errorf("resource already exists"), "Resource already exists.")
	// ErrUnauthenticated is used when no authentication is provided.
	ErrUnauthenticated = fmt.Errorf("unauthenticated")
	// ErrRateLimiting is used when the rate limit is exceeded.
	ErrRateLimiting = fmt.Errorf("rate limit exceeded")
	// ErrExceedMaxBatchSize is used when the max batch size is exceeded.
	ErrExceedMaxBatchSize = fmt.Errorf("batch size exceeded")
)
