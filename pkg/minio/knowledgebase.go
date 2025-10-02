package minio

import (
	"context"
	"encoding/base64"
	"fmt"
	"path/filepath"

	"github.com/gofrs/uuid"

	"github.com/instill-ai/artifact-backend/config"
)

// KnowledgeBaseI is the interface for knowledge base related operations.
type KnowledgeBaseI interface {
	// SaveConvertedFile saves a converted file to MinIO with the appropriate
	// MIME type.
	SaveConvertedFile(_ context.Context, kbUID, fileUID, convertedFileUID uuid.UUID, fileExt string, content []byte) (path string, _ error)
	// ListKnowledgeBaseFiles lists all file paths in a knowledge base
	ListKnowledgeBaseFiles(ctx context.Context, kbUID uuid.UUID) ([]string, error)
	// ListConvertedFilesByFileUID lists converted file paths
	ListConvertedFilesByFileUID(ctx context.Context, kbUID, fileUID uuid.UUID) ([]string, error)
	// ListTextChunksByFileUID lists text chunk file paths
	ListTextChunksByFileUID(ctx context.Context, kbUID, fileUID uuid.UUID) ([]string, error)
}

const convertedFileDir = "converted-file"
const chunkDir = "chunk"

func fileBasePath(kbUID, fileUID uuid.UUID) string {
	return filepath.Join("kb-"+kbUID.String(), "file-"+fileUID.String())
}

func convertedFileBasePath(kbUID, fileUID uuid.UUID) string {
	return filepath.Join(fileBasePath(kbUID, fileUID), convertedFileDir)
}

// SaveConvertedFile saves a converted file to MinIO with the appropriate MIME
// type, returning the path of the saved file.
func (m *Minio) SaveConvertedFile(ctx context.Context, kbUID, fileUID, convertedFileUID uuid.UUID, fileExt string, content []byte) (string, error) {
	filename := convertedFileUID.String() + "." + fileExt
	path := filepath.Join(convertedFileBasePath(kbUID, fileUID), filename)

	mimeType := "application/octet-stream"
	if fileExt == "md" {
		mimeType = "text/markdown"
	}

	err := m.UploadBase64File(ctx, config.Config.Minio.BucketName, path, base64.StdEncoding.EncodeToString(content), mimeType)
	if err != nil {
		return "", err
	}

	return path, nil
}

func chunkBasePath(kbUID, fileUID uuid.UUID) string {
	return filepath.Join(fileBasePath(kbUID, fileUID), chunkDir)
}

// ListKnowledgeBaseFiles lists all file paths in a knowledge base (including legacy paths).
// Returns combined paths from both "kb-" prefixed and legacy paths.
func (m *Minio) ListKnowledgeBaseFiles(ctx context.Context, kbUID uuid.UUID) ([]string, error) {
	bucket := config.Config.Minio.BucketName
	legacyPrefix := kbUID.String()

	paths1, err := m.ListFilePathsWithPrefix(ctx, bucket, "kb-"+legacyPrefix)
	if err != nil {
		return nil, fmt.Errorf("listing catalog files: %w", err)
	}

	paths2, err := m.ListFilePathsWithPrefix(ctx, bucket, legacyPrefix)
	if err != nil {
		return nil, fmt.Errorf("listing legacy catalog files: %w", err)
	}

	return append(paths1, paths2...), nil
}

// ListConvertedFilesByFileUID lists converted file paths for a given file.
func (m *Minio) ListConvertedFilesByFileUID(ctx context.Context, kbUID, fileUID uuid.UUID) ([]string, error) {
	return m.ListFilePathsWithPrefix(ctx, config.Config.Minio.BucketName, convertedFileBasePath(kbUID, fileUID))
}

// ListTextChunksByFileUID lists text chunk file paths for a given file.
func (m *Minio) ListTextChunksByFileUID(ctx context.Context, kbUID, fileUID uuid.UUID) ([]string, error) {
	return m.ListFilePathsWithPrefix(ctx, config.Config.Minio.BucketName, chunkBasePath(kbUID, fileUID))
}
