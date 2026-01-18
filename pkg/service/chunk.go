package service

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/instill-ai/artifact-backend/config"
	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/types"

	logx "github.com/instill-ai/x/log"
)

// GetChunksByFile returns the chunks of a file
// Fetches chunk content directly from MinIO using parallel goroutines for better performance
// Returns chunks and their content texts in the same order (use index to access matching content)
func (s *service) GetChunksByFile(ctx context.Context, file *repository.FileModel) (
	types.SourceTableType,
	types.SourceUIDType,
	[]repository.ChunkModel,
	[]string,
	error,
) {

	logger, _ := logx.GetZapLogger(ctx)
	var sourceTable string
	var sourceUID types.SourceUIDType

	// Get converted file for all types
	convertedFile, err := s.repository.GetConvertedFileByFileUID(ctx, file.UID)
	if err != nil {
		logger.Error("Failed to get converted file metadata.", zap.String("File uid", file.UID.String()))
		return sourceTable, sourceUID, nil, nil, err
	}
	sourceTable = repository.ConvertedFileTableName
	sourceUID = convertedFile.UID

	// Get textChunks metadata from database
	textChunks, err := s.repository.GetTextChunksBySource(ctx, sourceTable, sourceUID)
	if err != nil {
		logger.Error("Failed to get chunks from database.", zap.String("SourceUID", sourceUID.String()))
		return sourceTable, sourceUID, nil, nil, err
	}

	// Fetch chunks from MinIO in parallel using goroutines with retry
	texts := make([]string, len(textChunks))
	var wg sync.WaitGroup
	var mu sync.Mutex
	var fetchErr error

	bucket := config.Config.Minio.BucketName

	for i, chunk := range textChunks {
		wg.Add(1)
		go func(idx int, path string) {
			defer wg.Done()

			// Retry up to 3 times with exponential backoff for transient failures
			var content []byte
			var err error
			maxAttempts := 3

			for attempt := range maxAttempts {
				content, err = s.repository.GetMinIOStorage().GetFile(ctx, bucket, path)
				if err == nil {
					break // Success!
				}

				// Don't sleep after last attempt
				if attempt < maxAttempts-1 {
					// Exponential backoff: 1s, 2s
					backoff := time.Duration(1<<uint(attempt)) * time.Second
					time.Sleep(backoff)
				}
			}

			if err != nil {
				mu.Lock()
				if fetchErr == nil {
					fetchErr = fmt.Errorf("failed to fetch chunk %s after %d attempts: %w", path, maxAttempts, err)
				}
				mu.Unlock()
				return
			}

			mu.Lock()
			texts[idx] = string(content)
			mu.Unlock()
		}(i, chunk.StoragePath)
	}

	wg.Wait()

	if fetchErr != nil {
		logger.Error("Failed to get chunks from minIO.",
			zap.String("SourceTable", sourceTable),
			zap.String("SourceUID", sourceUID.String()),
			zap.Error(fetchErr))
		return sourceTable, sourceUID, nil, nil, fetchErr
	}

	return sourceTable, sourceUID, textChunks, texts, nil
}
