package worker

import (
	"context"
	"encoding/base64"
	"fmt"
	"path/filepath"

	"github.com/gofrs/uuid"
	"go.temporal.io/sdk/temporal"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/instill-ai/artifact-backend/config"

	errorsx "github.com/instill-ai/x/errors"
)

// This file contains MinIO storage activities used by SaveChunksWorkflow and GetFilesWorkflow:
// - SaveChunkBatchActivity - Saves multiple text chunks to MinIO in batch
// - GetFileActivity - Retrieves file metadata and content from MinIO

// SaveChunkBatchActivityParam defines parameters for saving multiple chunks in one activity
type SaveChunkBatchActivityParam struct {
	KnowledgeBaseUID uuid.UUID
	FileUID          uuid.UUID
	Chunks           map[string][]byte // chunkUID -> content
}

// SaveChunkBatchActivityResult defines the result of saving a batch of chunks
type SaveChunkBatchActivityResult struct {
	Destinations map[string]string // chunkUID -> destination
}

// UpdateChunkDestinationsActivityParam defines parameters for updating chunk destinations
type UpdateChunkDestinationsActivityParam struct {
	Destinations map[string]string // chunkUID -> destination
}

// DeleteFileActivityParam defines parameters for deleting a single file
type DeleteFileActivityParam struct {
	Bucket string
	Path   string
}

// GetFileActivityParam defines parameters for getting a single file
type GetFileActivityParam struct {
	Bucket   string
	Path     string
	Index    int              // For maintaining order
	Metadata *structpb.Struct // For authentication context
}

// GetFileActivityResult contains the file content
type GetFileActivityResult struct {
	Index   int
	Name    string
	Content []byte
}

// DeleteFileActivity deletes a single file from MinIO
func (w *Worker) DeleteFileActivity(ctx context.Context, param *DeleteFileActivityParam) error {
	w.log.Info("Starting DeleteFileActivity",
		zap.String("bucket", param.Bucket),
		zap.String("path", param.Path))

	err := w.service.MinIO().DeleteFile(ctx, param.Bucket, param.Path)
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

	w.log.Info("File deleted successfully", zap.String("path", param.Path))
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

	content, err := w.service.MinIO().GetFile(authCtx, param.Bucket, param.Path)
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

	w.log.Info("File retrieved successfully",
		zap.String("path", param.Path),
		zap.Int("size", len(content)))

	return &GetFileActivityResult{
		Index:   param.Index,
		Name:    filepath.Base(param.Path),
		Content: content,
	}, nil
}

// UpdateChunkDestinationsActivity updates the destinations of chunks in the database
func (w *Worker) UpdateChunkDestinationsActivity(ctx context.Context, param *UpdateChunkDestinationsActivityParam) error {
	w.log.Info("Starting UpdateChunkDestinationsActivity",
		zap.Int("chunkCount", len(param.Destinations)))

	err := w.service.Repository().UpdateChunkDestinations(ctx, param.Destinations)
	if err != nil {
		w.log.Error("Failed to update chunk destinations",
			zap.Int("chunkCount", len(param.Destinations)),
			zap.Error(err))
		err = errorsx.AddMessage(err, "Unable to update chunk references. Please try again.")
		return temporal.NewApplicationErrorWithCause(
			errorsx.MessageOrErr(err),
			updateChunkDestinationsActivityError,
			err,
		)
	}

	w.log.Info("Chunk destinations updated successfully",
		zap.Int("chunkCount", len(param.Destinations)))
	return nil
}

// SaveChunkBatchActivity saves multiple chunks to MinIO in one activity
// This reduces Temporal overhead by batching multiple chunks into a single activity
func (w *Worker) SaveChunkBatchActivity(ctx context.Context, param *SaveChunkBatchActivityParam) (*SaveChunkBatchActivityResult, error) {
	w.log.Info("Starting SaveChunkBatchActivity",
		zap.String("kbUID", param.KnowledgeBaseUID.String()),
		zap.String("fileUID", param.FileUID.String()),
		zap.Int("chunkCount", len(param.Chunks)))

	destinations := make(map[string]string, len(param.Chunks))

	for chunkUID, chunkContent := range param.Chunks {
		basePath := fmt.Sprintf("kb-%s/file-%s/chunk", param.KnowledgeBaseUID.String(), param.FileUID.String())
		path := fmt.Sprintf("%s/%s.md", basePath, chunkUID)

		base64Content := base64.StdEncoding.EncodeToString(chunkContent)

		err := w.service.MinIO().UploadBase64File(
			ctx,
			config.Config.Minio.BucketName,
			path,
			base64Content,
			"text/markdown",
		)
		if err != nil {
			w.log.Error("Failed to save chunk",
				zap.String("chunkUID", chunkUID),
				zap.Error(err))
			err = errorsx.AddMessage(err, fmt.Sprintf("Unable to save chunk (ID: %s). Please try again.", chunkUID))
			return nil, temporal.NewApplicationErrorWithCause(
				errorsx.MessageOrErr(err),
				saveChunkBatchActivityError,
				err,
			)
		}

		destinations[chunkUID] = path
	}

	w.log.Info("Batch saved successfully",
		zap.Int("chunkCount", len(destinations)))

	return &SaveChunkBatchActivityResult{
		Destinations: destinations,
	}, nil
}

// Activity error type constants
const (
	saveChunkBatchActivityError          = "SaveChunkBatchActivity"
	deleteFileActivityError              = "DeleteFileActivity"
	getFileActivityError                 = "GetFileActivity"
	updateChunkDestinationsActivityError = "UpdateChunkDestinationsActivity"
)
