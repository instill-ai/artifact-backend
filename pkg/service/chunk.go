package service

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/gofrs/uuid"
	"go.uber.org/zap"

	"github.com/instill-ai/artifact-backend/config"
	"github.com/instill-ai/artifact-backend/pkg/repository"

	logx "github.com/instill-ai/x/log"
)

// ChunkUIDType is the type for the chunk UID
type ChunkUIDType = uuid.UUID

// ContentType is the type for the content type
type ContentType = string

// SourceTableType is the type for the source table
type SourceTableType = string

// SourceIDType is the type for the source ID
type SourceIDType = uuid.UUID

// GetChunksByFile returns the chunks of a file
// Fetches chunk content directly from MinIO using parallel goroutines for better performance
func (s *service) GetChunksByFile(ctx context.Context, file *repository.KnowledgeBaseFile) (
	SourceTableType,
	SourceIDType,
	[]repository.TextChunk,
	map[ChunkUIDType]ContentType,
	[]string,
	error,
) {

	logger, _ := logx.GetZapLogger(ctx)
	var sourceTable string
	var sourceUID uuid.UUID

	// Get converted file for all types
	convertedFile, err := s.repository.GetConvertedFileByFileUID(ctx, file.UID)
	if err != nil {
		logger.Error("Failed to get converted file metadata.", zap.String("File uid", file.UID.String()))
		return sourceTable, sourceUID, nil, nil, nil, err
	}
	sourceTable = s.repository.ConvertedFileTableName()
	sourceUID = convertedFile.UID

	// Get chunks metadata from database
	chunks, err := s.repository.GetTextChunksBySource(ctx, sourceTable, sourceUID)
	if err != nil {
		logger.Error("Failed to get chunks from database.", zap.String("SourceUID", sourceUID.String()))
		return sourceTable, sourceUID, nil, nil, nil, err
	}

	// Fetch chunks from MinIO in parallel using goroutines with retry
	texts := make([]string, len(chunks))
	var wg sync.WaitGroup
	var mu sync.Mutex
	var fetchErr error

	bucket := config.Config.Minio.BucketName

	for i, chunk := range chunks {
		wg.Add(1)
		go func(idx int, path string) {
			defer wg.Done()

			// Retry up to 3 times with exponential backoff for transient failures
			var content []byte
			var err error
			maxAttempts := 3

			for attempt := 0; attempt < maxAttempts; attempt++ {
				content, err = s.minIO.GetFile(ctx, bucket, path)
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
		}(i, chunk.ContentDest)
	}

	wg.Wait()

	if fetchErr != nil {
		logger.Error("Failed to get chunks from minIO.",
			zap.String("SourceTable", sourceTable),
			zap.String("SourceUID", sourceUID.String()),
			zap.Error(fetchErr))
		return sourceTable, sourceUID, nil, nil, nil, fetchErr
	}

	// Build chunk UID to content map
	chunkUIDToContents := make(map[ChunkUIDType]ContentType, len(chunks))
	for i, c := range chunks {
		chunkUIDToContents[c.UID] = texts[i]
	}

	return sourceTable, sourceUID, chunks, chunkUIDToContents, texts, nil
}
