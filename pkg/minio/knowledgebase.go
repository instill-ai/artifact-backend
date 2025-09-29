package minio

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"path/filepath"
	"sync"

	"github.com/gofrs/uuid"
	"go.uber.org/zap"

	"github.com/instill-ai/artifact-backend/config"
	"github.com/instill-ai/artifact-backend/pkg/utils"

	logx "github.com/instill-ai/x/log"
)

// KnowledgeBaseI is the interface for knowledge base related operations.
type KnowledgeBaseI interface {
	// SaveConvertedFile saves a converted file to MinIO with the appropriate
	// MIME type.
	SaveConvertedFile(_ context.Context, kbUID, fileUID, convertedFileUID uuid.UUID, fileExt string, content []byte) (path string, _ error)
	// SaveTextChunks saves batch of chunks(text files) to MinIO.
	SaveTextChunks(_ context.Context, kbUID, fileUID uuid.UUID, chunks map[ChunkUIDType]ChunkContentType) (destinations map[string]string, _ error)
	// DeleteKnowledgeBase deletes all files in the knowledge base.
	DeleteKnowledgeBase(_ context.Context, kbUID uuid.UUID) error
	// DeleteConvertedFileByFileUID deletes a converted file by file UID.
	DeleteConvertedFileByFileUID(ctx context.Context, kbUID, fileUID uuid.UUID) error
	// DeleteTextChunksByFileUID deletes all the chunks extracted from a file.
	DeleteTextChunksByFileUID(ctx context.Context, kbUID, fileUID uuid.UUID) error
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

type ChunkUIDType string
type ChunkContentType []byte

func chunkBasePath(kbUID, fileUID uuid.UUID) string {
	return filepath.Join(fileBasePath(kbUID, fileUID), chunkDir)
}

// SaveTextChunks saves batch of chunks(text files) to MinIO.
// rate limiting is implemented to avoid overwhelming the MinIO server.
func (m *Minio) SaveTextChunks(ctx context.Context, kbUID, fileUID uuid.UUID, chunks map[ChunkUIDType]ChunkContentType) (destinations map[string]string, _ error) {
	logger, _ := logx.GetZapLogger(ctx)
	var wg sync.WaitGroup
	var mu sync.Mutex
	destinations = make(map[string]string)

	type ChunkError struct {
		ChunkUID     string
		ErrorMessage string
	}
	errorUIDChan := make(chan ChunkError, len(chunks))

	counter := 0
	maxConcurrentUploads := 50
	for chunkUID, chunkContent := range chunks {
		wg.Add(1)
		go utils.GoRecover(
			func() {
				func(chunkUID ChunkUIDType, chunkContent ChunkContentType) {
					defer wg.Done()

					path := filepath.Join(chunkBasePath(kbUID, fileUID), string(chunkUID)) + ".txt"
					err := m.UploadBase64File(ctx, config.Config.Minio.BucketName, path, base64.StdEncoding.EncodeToString(chunkContent), "text/plain")
					if err != nil {
						logger.Error("Failed to upload chunk after retries", zap.String("chunkUID", string(chunkUID)), zap.Error(err))
						errorUIDChan <- ChunkError{ChunkUID: string(chunkUID), ErrorMessage: err.Error()}
						return
					}

					// Add successful chunk to destinations map
					mu.Lock()
					destinations[string(chunkUID)] = path
					mu.Unlock()
				}(chunkUID, chunkContent)
			}, fmt.Sprintf("SaveTextChunks %s", chunkUID))

		counter++
		if counter == maxConcurrentUploads {
			wg.Wait()
			counter = 0
		}
	}
	wg.Wait()
	close(errorUIDChan)
	var errStr []ChunkError
	for err := range errorUIDChan {
		errStr = append(errStr, err)
	}
	if len(errStr) > 0 {
		return nil, fmt.Errorf("failed to upload chunks: %v", errStr)
	}
	return destinations, nil
}

// DeleteKnowledgeBase removes all the blobs in the knowledge base.
func (m *Minio) DeleteKnowledgeBase(ctx context.Context, kbUID uuid.UUID) error {
	bucket := config.Config.Minio.BucketName
	legacyPrefix := kbUID.String()

	err := m.collectErrors(m.DeleteFilesWithPrefix(ctx, bucket, "kb-"+legacyPrefix))
	if err != nil {
		return fmt.Errorf("deleting catalog files: %w", err)
	}

	// Catalogs might have blobs in legacy paths.
	err = m.collectErrors(m.DeleteFilesWithPrefix(ctx, bucket, legacyPrefix))
	if err != nil {
		return fmt.Errorf("deleting legacy catalog files: %w", err)
	}

	return nil
}

func (m *Minio) collectErrors(errChan chan error) error {
	var errs []error
	for err := range errChan {
		if err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return errors.Join(errs[0])
	}

	return nil
}

func (m *Minio) DeleteConvertedFileByFileUID(ctx context.Context, kbUID, fileUID uuid.UUID) error {
	return m.collectErrors(m.DeleteFilesWithPrefix(ctx, config.Config.Minio.BucketName, convertedFileBasePath(kbUID, fileUID)))

}

func (m *Minio) DeleteTextChunksByFileUID(ctx context.Context, kbUID, fileUID uuid.UUID) error {
	return m.collectErrors(m.DeleteFilesWithPrefix(ctx, config.Config.Minio.BucketName, chunkBasePath(kbUID, fileUID)))
}
