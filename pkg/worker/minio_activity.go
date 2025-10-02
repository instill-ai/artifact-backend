package worker

import (
	"context"
	"encoding/base64"
	"fmt"
	"path/filepath"

	"go.uber.org/zap"

	"github.com/instill-ai/artifact-backend/config"
)

// SaveChunkActivity saves a single text chunk to MinIO
func (w *worker) SaveChunkActivity(ctx context.Context, param *SaveChunkActivityParam) (*SaveChunkActivityResult, error) {
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
		return nil, fmt.Errorf("failed to upload chunk %s: %w", param.ChunkUID, err)
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
func (w *worker) DeleteFileActivity(ctx context.Context, param *DeleteFileActivityParam) error {
	w.log.Info("Starting DeleteFileActivity",
		zap.String("bucket", param.Bucket),
		zap.String("path", param.Path))

	err := w.service.MinIO().DeleteFile(ctx, param.Bucket, param.Path)
	if err != nil {
		w.log.Error("Failed to delete file",
			zap.String("path", param.Path),
			zap.Error(err))
		return fmt.Errorf("failed to delete file %s: %w", param.Path, err)
	}

	w.log.Info("File deleted successfully", zap.String("path", param.Path))
	return nil
}

// GetFileActivity retrieves a single file from MinIO
func (w *worker) GetFileActivity(ctx context.Context, param *GetFileActivityParam) (*GetFileActivityResult, error) {
	w.log.Info("Starting GetFileActivity",
		zap.String("bucket", param.Bucket),
		zap.String("path", param.Path))

	content, err := w.service.MinIO().GetFile(ctx, param.Bucket, param.Path)
	if err != nil {
		w.log.Error("Failed to get file",
			zap.String("path", param.Path),
			zap.Error(err))
		return nil, fmt.Errorf("failed to get file %s: %w", param.Path, err)
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
