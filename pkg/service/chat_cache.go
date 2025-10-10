package service

import (
	"context"
	"fmt"

	"github.com/instill-ai/artifact-backend/internal/ai"
	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/types"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
)

// GetChatCacheForFiles retrieves active chat cache metadata for given files from Redis
// Returns nil if cache not found or expired
func (s *service) GetChatCacheForFiles(
	ctx context.Context,
	kbUID types.KBUIDType,
	fileUIDs []types.FileUIDType,
) (*repository.ChatCacheMetadata, error) {
	if len(fileUIDs) == 0 {
		return nil, nil
	}

	// Lookup cache metadata from Redis using repository
	metadata, err := s.repository.GetChatCacheMetadata(ctx, kbUID, fileUIDs)
	if err != nil {
		return nil, fmt.Errorf("failed to get chat cache metadata: %w", err)
	}

	// Redis returns nil if key not found or expired (TTL handled automatically)
	return metadata, nil
}

// ChatWithCache responds to a prompt using cached context or file content
// This enables instant chat without needing embeddings/retrieval
// Supports two modes:
// 1. Cached mode: Use Gemini cache for large files (fast, reusable)
// 2. Uncached mode: Upload files directly for small files (< 1024 tokens)
func (s *service) ChatWithCache(
	ctx context.Context,
	metadata *repository.ChatCacheMetadata,
	prompt string,
) (string, error) {
	if metadata == nil {
		return "", fmt.Errorf("cache metadata cannot be nil")
	}
	if prompt == "" {
		return "", fmt.Errorf("prompt cannot be empty")
	}

	// Try to access AI provider through worker interface
	// We need to use interface{} and type assertion to avoid import cycles
	accessor, ok := s.worker.(interface{ GetAIProvider() ai.Provider })
	if !ok {
		return "", fmt.Errorf("worker does not support AI provider access")
	}

	aiProvider := accessor.GetAIProvider()
	if aiProvider == nil {
		return "", fmt.Errorf("AI provider not available")
	}

	// Path 1: Use AI cache if available (for large files)
	if metadata.CachedContextEnabled && metadata.CacheName != "" {
		result, err := aiProvider.ChatWithCache(ctx, metadata.CacheName, prompt)
		if err == nil {
			return result.Answer, nil
		}
		// Cache failed (possibly expired) - fall through to path 2 if file refs exist
	}

	// Path 2: Use stored file content for uncached chat (for small files)
	// Content is already stored in Redis (no need to fetch from MinIO)
	if len(metadata.FileContents) > 0 {
		files := make([]ai.FileContent, 0, len(metadata.FileContents))
		for _, ref := range metadata.FileContents {
			// Parse file type from string
			fileTypeVal, ok := artifactpb.FileType_value[ref.FileType]
			if !ok {
				continue
			}
			fileType := artifactpb.FileType(fileTypeVal)

			// Use content already stored in Redis (no MinIO fetch needed!)
			files = append(files, ai.FileContent{
				FileUID:  ref.FileUID,
				Content:  ref.Content, // Content stored directly in Redis
				FileType: fileType,
				Filename: ref.Filename,
			})
		}

		if len(files) == 0 {
			return "", fmt.Errorf("no valid files found in cache metadata")
		}

		// Call AI provider with files directly (no cache)
		result, err := aiProvider.ChatWithFiles(ctx, files, prompt)
		if err != nil {
			return "", fmt.Errorf("failed to generate chat response with files: %w", err)
		}

		return result.Answer, nil
	}

	return "", fmt.Errorf("no cache or file references available for chat")
}

// CheckFilesProcessingStatus checks if files are still being processed
// Returns: allCompleted (true if all files are completed), processingCount (number of files still processing), error
func (s *service) CheckFilesProcessingStatus(
	ctx context.Context,
	fileUIDs []types.FileUIDType,
) (bool, int, error) {
	if len(fileUIDs) == 0 {
		return true, 0, nil
	}

	// Get file metadata from repository
	files, err := s.repository.GetKnowledgeBaseFilesByFileUIDs(ctx, fileUIDs)
	if err != nil {
		return false, 0, fmt.Errorf("failed to get file metadata: %w", err)
	}

	// Count files that are not completed
	processingCount := 0
	for _, file := range files {
		status := artifactpb.FileProcessStatus(artifactpb.FileProcessStatus_value[file.ProcessStatus])
		if status != artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_COMPLETED {
			processingCount++
		}
	}

	allCompleted := processingCount == 0
	return allCompleted, processingCount, nil
}
