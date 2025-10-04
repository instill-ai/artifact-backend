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

// SaveChunkActivityParam defines parameters for saving a single chunk
type SaveChunkActivityParam struct {
	KnowledgeBaseUID uuid.UUID
	FileUID          uuid.UUID
	ChunkUID         string
	ChunkContent     []byte
}

// SaveChunkActivityResult contains the result of saving a chunk
type SaveChunkActivityResult struct {
	ChunkUID    string
	Destination string
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

// SaveChunkActivity saves a single text chunk to MinIO
func (w *Worker) SaveChunkActivity(ctx context.Context, param *SaveChunkActivityParam) (*SaveChunkActivityResult, error) {
	w.log.Info("Starting SaveChunkActivity",
		zap.String("chunkUID", param.ChunkUID))

	// Construct the path using the correct format: kb-{kbUID}/file-{fileUID}/chunk/{chunkUID}.md
	basePath := filepath.Join("kb-"+param.KnowledgeBaseUID.String(), "file-"+param.FileUID.String(), "chunk")
	path := filepath.Join(basePath, param.ChunkUID) + ".md"

	// Upload the chunk
	err := w.service.MinIO().UploadBase64File(
		ctx,
		config.Config.Minio.BucketName,
		path,
		base64.StdEncoding.EncodeToString(param.ChunkContent),
		"text/markdown",
	)
	if err != nil {
		w.log.Error("Failed to upload chunk",
			zap.String("chunkUID", param.ChunkUID),
			zap.Error(err))
		return nil, temporal.NewApplicationErrorWithCause(
			fmt.Sprintf("Failed to upload chunk to storage: %s", errorsx.MessageOrErr(err)),
			saveChunkActivityError,
			err,
		)
	}

	w.log.Info("Chunk uploaded successfully",
		zap.String("chunkUID", param.ChunkUID),
		zap.String("path", path))

	return &SaveChunkActivityResult{
		ChunkUID:    param.ChunkUID,
		Destination: path,
	}, nil
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
		return temporal.NewApplicationErrorWithCause(
			fmt.Sprintf("Failed to delete file from storage: %s", errorsx.MessageOrErr(err)),
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
			return nil, temporal.NewApplicationErrorWithCause(
				fmt.Sprintf("Failed to create authenticated context: %s", errorsx.MessageOrErr(err)),
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
		return nil, temporal.NewApplicationErrorWithCause(
			fmt.Sprintf("Failed to retrieve file from storage: %s", errorsx.MessageOrErr(err)),
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
		return temporal.NewApplicationErrorWithCause(
			fmt.Sprintf("Failed to update chunk destinations: %s", errorsx.MessageOrErr(err)),
			updateChunkDestinationsActivityError,
			err,
		)
	}

	w.log.Info("Chunk destinations updated successfully",
		zap.Int("chunkCount", len(param.Destinations)))
	return nil
}

// Activity error type constants
const (
	saveChunkActivityError               = "SaveChunkActivity"
	deleteFileActivityError              = "DeleteFileActivity"
	getFileActivityError                 = "GetFileActivity"
	updateChunkDestinationsActivityError = "UpdateChunkDestinationsActivity"
)
