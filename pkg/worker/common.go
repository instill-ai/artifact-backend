package worker

import (
	"context"
	"encoding/json"
	"fmt"

	"go.temporal.io/sdk/temporal"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/instill-ai/artifact-backend/pkg/constant"

	errorsx "github.com/instill-ai/x/errors"
)

// activityError creates a Temporal ApplicationError with a flattened error chain.
// This prevents deeply nested error structures in Temporal logs by:
// 1. Using errorsx.MessageOrErr(err) for the user-facing message
// 2. Creating a new flat error (not wrapped) for the cause
//
// Use this at activity boundaries instead of temporal.NewApplicationErrorWithCause
// to avoid error nesting like: { cause: { cause: { cause: ... } } }
func activityError(err error, errorType string) error {
	return temporal.NewApplicationErrorWithCause(
		errorsx.MessageOrErr(err),
		errorType,
		fmt.Errorf("%s", err.Error()), // Break the chain with a new flat error
	)
}

// activityErrorWithMessage creates a Temporal ApplicationError with a custom message and flattened cause.
// Use this when you want to provide a specific message instead of extracting it from the error.
func activityErrorWithMessage(message string, errorType string, err error) error {
	return temporal.NewApplicationErrorWithCause(
		message,
		errorType,
		fmt.Errorf("%s", err.Error()), // Break the chain with a new flat error
	)
}

// activityErrorNonRetryableWithMessage creates a non-retryable Temporal ApplicationError
// with a custom message and flattened cause.
func activityErrorNonRetryableWithMessage(message string, errorType string, err error) error {
	return temporal.NewNonRetryableApplicationError(
		message,
		errorType,
		fmt.Errorf("%s", err.Error()), // Break the chain with a new flat error
	)
}

// activityErrorWithCauseFlat is a drop-in replacement for temporal.NewApplicationErrorWithCause
// that flattens the error chain to prevent deeply nested errors in Temporal logs.
// This function has the same signature as temporal.NewApplicationErrorWithCause for easy migration.
func activityErrorWithCauseFlat(message string, errorType string, cause error) error {
	return temporal.NewApplicationErrorWithCause(
		message,
		errorType,
		fmt.Errorf("%s", cause.Error()), // Break the chain with a new flat error
	)
}

// activityErrorNonRetryableFlat is a drop-in replacement for temporal.NewNonRetryableApplicationError
// that flattens the error chain to prevent deeply nested errors in Temporal logs.
func activityErrorNonRetryableFlat(message string, errorType string, cause error) error {
	return temporal.NewNonRetryableApplicationError(
		message,
		errorType,
		fmt.Errorf("%s", cause.Error()), // Break the chain with a new flat error
	)
}

// activityErrorSimple is a replacement for temporal.NewApplicationError (no cause)
// Used when we only have a message and type, no underlying cause error.
func activityErrorSimple(message string, errorType string) error {
	return temporal.NewApplicationError(message, errorType)
}

// extractRequestMetadata extracts the gRPC metadata from a file's ExternalMetadataE
// and returns it as metadata.MD that can be used to create an authenticated context.
func extractRequestMetadata(externalMetadata *structpb.Struct) (metadata.MD, error) {
	md := metadata.MD{}
	if externalMetadata == nil {
		return md, nil
	}

	if externalMetadata.Fields[constant.MetadataRequestKey] == nil {
		return md, nil
	}

	// In order to simplify the code translating metadata.MD <->
	// structpb.Struct, JSON marshalling is used. This is less efficient than
	// leveraging the knowledge about the metadata structure (a
	// map[string][]string), but readability has been prioritized.
	j, err := externalMetadata.Fields[constant.MetadataRequestKey].GetStructValue().MarshalJSON()
	if err != nil {
		return nil, errorsx.AddMessage(err, "Unable to process authentication metadata. Please try again.")
	}

	if err := json.Unmarshal(j, &md); err != nil {
		return nil, errorsx.AddMessage(err, "Unable to process authentication metadata. Please try again.")
	}

	return md, nil
}

// CreateAuthenticatedContext creates a context with the authentication metadata
// from the file's ExternalMetadata. This allows activities to make authenticated
// calls to other services (like pipeline-backend).
func CreateAuthenticatedContext(ctx context.Context, externalMetadata *structpb.Struct) (context.Context, error) {
	md, err := extractRequestMetadata(externalMetadata)
	if err != nil {
		return ctx, errorsx.AddMessage(err, "Authentication failed. Please try again.")
	}

	if len(md) == 0 {
		return ctx, nil
	}

	return metadata.NewOutgoingContext(ctx, md), nil
}
