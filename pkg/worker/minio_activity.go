package worker

import (
	"context"
	"encoding/base64"
	"fmt"
	"path/filepath"

	"go.temporal.io/sdk/temporal"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/instill-ai/artifact-backend/config"
	"github.com/instill-ai/artifact-backend/pkg/types"

	errorsx "github.com/instill-ai/x/errors"
)

// This file contains MinIO storage activities:
// - SaveChunkBatchActivity - Saves multiple text chunks to MinIO in batch
// - GetFileActivity - Retrieves file metadata and content from MinIO

// SaveChunkBatchActivityParam defines parameters for saving multiple text chunks in one activity
type SaveChunkBatchActivityParam struct {
	KBUID   types.KBUIDType   // Knowledge base unique identifier
	FileUID types.FileUIDType // File unique identifier
	Chunks  map[string][]byte // Map of text chunk UID to content bytes
}

// SaveChunkBatchActivityResult defines the result of saving a batch of text chunks
type SaveChunkBatchActivityResult struct {
	Destinations map[string]string // Map of text chunk UID to MinIO destination path
}

// UpdateChunkDestinationsActivityParam defines parameters for UpdateTextChunkDestinationsActivity
type UpdateChunkDestinationsActivityParam struct {
	Destinations map[string]string // Map of text chunk UID to MinIO destination path
}

// DeleteFileActivityParam defines parameters for deleting a single file
type DeleteFileActivityParam struct {
	Bucket string // MinIO bucket containing the file
	Path   string // MinIO path to the file
}

// GetFileActivityParam defines parameters for getting a single file
type GetFileActivityParam struct {
	Bucket   string           // MinIO bucket containing the file
	Path     string           // MinIO path to the file
	Index    int              // Index for maintaining order in batch operations
	Metadata *structpb.Struct // Request metadata for authentication context
}

// GetFileActivityResult contains the file content
type GetFileActivityResult struct {
	Index   int    // Index from the request (for ordering results)
	Name    string // File name
	Content []byte // File content bytes
}

// DeleteFileActivity deletes a single file from MinIO
func (w *Worker) DeleteFileActivity(ctx context.Context, param *DeleteFileActivityParam) error {
	w.log.Info("Starting DeleteFileActivity",
		zap.String("bucket", param.Bucket),
		zap.String("path", param.Path))

	err := w.repository.DeleteFile(ctx, param.Bucket, param.Path)
	if err != nil {
		w.log.Error("Failed to delete file",
			zap.String("path", param.Path),
			zap.Error(err))
		err = errorsx.AddMessage(err, "Unable to delete file from storage. Please try again.")
		return temporal.NewApplicationErrorWithCause(
			errorsx.MessageOrErr(err),
			deleteFileActivityError,
			err,
		)
	}

	return nil
}

// GetFileActivity retrieves a single file from MinIO
func (w *Worker) GetFileActivity(ctx context.Context, param *GetFileActivityParam) (*GetFileActivityResult, error) {
	w.log.Info("Starting GetFileActivity",
		zap.String("bucket", param.Bucket),
		zap.String("path", param.Path))

	// Create authenticated context if metadata provided
	authCtx := ctx
	if param.Metadata != nil {
		var err error
		authCtx, err = createAuthenticatedContext(ctx, param.Metadata)
		if err != nil {
			err = errorsx.AddMessage(err, "Authentication failed. Please try again.")
			return nil, temporal.NewApplicationErrorWithCause(
				errorsx.MessageOrErr(err),
				getFileActivityError,
				err,
			)
		}
	}

	content, err := w.repository.GetFile(authCtx, param.Bucket, param.Path)
	if err != nil {
		w.log.Error("Failed to get file",
			zap.String("path", param.Path),
			zap.Error(err))
		err = errorsx.AddMessage(err, "Unable to retrieve file from storage. Please try again.")
		return nil, temporal.NewApplicationErrorWithCause(
			errorsx.MessageOrErr(err),
			getFileActivityError,
			err,
		)
	}

	return &GetFileActivityResult{
		Index:   param.Index,
		Name:    filepath.Base(param.Path),
		Content: content,
	}, nil
}

// UpdateTextChunkDestinationsActivity updates the destinations of text chunks in the database
func (w *Worker) UpdateTextChunkDestinationsActivity(ctx context.Context, param *UpdateChunkDestinationsActivityParam) error {
	w.log.Info("Starting UpdateTextChunkDestinationsActivity",
		zap.Int("chunkCount", len(param.Destinations)))

	err := w.repository.UpdateTextChunkDestinations(ctx, param.Destinations)
	if err != nil {
		w.log.Error("Failed to update text chunk destinations",
			zap.Int("chunkCount", len(param.Destinations)),
			zap.Error(err))
		err = errorsx.AddMessage(err, "Unable to update text chunk references. Please try again.")
		return temporal.NewApplicationErrorWithCause(
			errorsx.MessageOrErr(err),
			updateTextChunkDestinationsActivityError,
			err,
		)
	}

	return nil
}

// SaveTextChunkBatchActivity saves multiple text chunks to MinIO in one activity
// This reduces Temporal overhead by batching multiple text chunks into a single activity
func (w *Worker) SaveTextChunkBatchActivity(ctx context.Context, param *SaveChunkBatchActivityParam) (*SaveChunkBatchActivityResult, error) {
	w.log.Info("Starting SaveTextChunkBatchActivity",
		zap.String("kbUID", param.KBUID.String()),
		zap.String("fileUID", param.FileUID.String()),
		zap.Int("chunkCount", len(param.Chunks)))

	destinations := make(map[string]string, len(param.Chunks))

	for chunkUID, chunkContent := range param.Chunks {
		basePath := fmt.Sprintf("kb-%s/file-%s/chunk", param.KBUID.String(), param.FileUID.String())
		path := fmt.Sprintf("%s/%s.md", basePath, chunkUID)

		base64Content := base64.StdEncoding.EncodeToString(chunkContent)

		err := w.repository.UploadBase64File(
			ctx,
			config.Config.Minio.BucketName,
			path,
			base64Content,
			"text/markdown",
		)
		if err != nil {
			w.log.Error("Failed to save text chunk",
				zap.String("chunkUID", chunkUID),
				zap.Error(err))
			err = errorsx.AddMessage(err, fmt.Sprintf("Unable to save text chunk (ID: %s). Please try again.", chunkUID))
			return nil, temporal.NewApplicationErrorWithCause(
				errorsx.MessageOrErr(err),
				saveTextChunkBatchActivityError,
				err,
			)
		}

		destinations[chunkUID] = path
	}

	return &SaveChunkBatchActivityResult{
		Destinations: destinations,
	}, nil
}

// Activity error type constants
const (
	saveTextChunkBatchActivityError          = "SaveTextChunkBatchActivity"
	deleteFileActivityError                  = "DeleteFileActivity"
	getFileActivityError                     = "GetFileActivity"
	updateTextChunkDestinationsActivityError = "UpdateTextChunkDestinationsActivity"
)
