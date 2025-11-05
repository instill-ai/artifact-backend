package service

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/instill-ai/artifact-backend/pkg/ai"
	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/types"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
	errorsx "github.com/instill-ai/x/errors"
	logx "github.com/instill-ai/x/log"
)

// FileCacheResult represents the result of getting or creating a file cache
type FileCacheResult struct {
	CacheName            string
	Model                string
	CreateTime           time.Time
	ExpireTime           time.Time
	CachedContextEnabled bool
	// FileContents contains file content refs if cache creation failed (uncached mode)
	FileContents []repository.FileContentRef
}

// GetOrCreateFileCache retrieves an existing cache or creates a new one for a file
// This is used by GetFile?view=VIEW_CACHE to ensure cache exists with proper TTL
func (s *service) GetOrCreateFileCache(
	ctx context.Context,
	kbUID types.KBUIDType,
	fileUID types.FileUIDType,
	bucketName, objectName string,
	fileType artifactpb.File_Type,
	filename string,
) (*FileCacheResult, error) {
	logger, _ := logx.GetZapLogger(ctx)

	// Check if cache metadata already exists in Redis
	fileUIDs := []types.FileUIDType{fileUID}
	cacheMetadata, err := s.repository.GetCacheMetadata(ctx, kbUID, fileUIDs)
	if err != nil {
		logger.Warn("Failed to get cache metadata, will create new cache",
			zap.Error(err),
			zap.String("kbUID", kbUID.String()),
			zap.String("fileUID", fileUID.String()))
	}

	// If cache exists and is valid, return it (it will be renewed by caller)
	if cacheMetadata != nil {
		return &FileCacheResult{
			CacheName:            cacheMetadata.CacheName,
			Model:                cacheMetadata.Model,
			CreateTime:           cacheMetadata.CreateTime,
			ExpireTime:           cacheMetadata.ExpireTime,
			CachedContextEnabled: cacheMetadata.CachedContextEnabled,
			FileContents:         cacheMetadata.FileContents,
		}, nil
	}

	// Cache doesn't exist, need to create it

	// Check if AI client is available
	if s.aiClient == nil {
		return nil, errorsx.AddMessage(
			fmt.Errorf("AI client not available"),
			"Cache service is not available. Please contact your administrator.",
		)
	}

	// Check if file type is supported by AI client
	if !s.aiClient.SupportsFileType(fileType) {
		return nil, errorsx.AddMessage(
			fmt.Errorf("file type not supported for caching: %s", fileType.String()),
			fmt.Sprintf("File type %s is not supported for caching. Only documents, images, audio, and video files can be cached.", fileType.String()),
		)
	}

	// Fetch file content from MinIO
	content, err := s.repository.GetFile(ctx, bucketName, objectName)
	if err != nil {
		return nil, errorsx.AddMessage(
			fmt.Errorf("failed to fetch file content: %w", err),
			"Unable to retrieve file content for caching. Please try again.",
		)
	}

	// Try to create Gemini cache with ViewCacheTTL
	logger.Info("Creating new Gemini cache for file",
		zap.String("fileUID", fileUID.String()),
		zap.String("fileType", fileType.String()),
		zap.Duration("ttl", ai.ViewCacheTTL))

	cacheResult, err := s.aiClient.CreateCache(ctx, []ai.FileContent{
		{
			Content:  content,
			FileType: fileType,
			Filename: filename,
		},
	}, ai.ViewCacheTTL)

	if err != nil {
		// Cache creation failed, use fallback mode (store content in Redis)
		logger.Warn("Gemini cache creation failed, using uncached mode",
			zap.Error(err),
			zap.String("fileUID", fileUID.String()))

		// Store in Redis as uncached mode
		now := time.Now()
		expireTime := now.Add(ai.ViewCacheTTL)
		metadata := &repository.CacheMetadata{
			CacheName:            "", // Empty for uncached mode
			Model:                "",
			FileUIDs:             fileUIDs,
			FileCount:            1,
			CreateTime:           now,
			ExpireTime:           expireTime,
			CachedContextEnabled: false,
			FileContents: []repository.FileContentRef{
				{
					FileUID:  fileUID,
					Content:  content,
					FileType: fileType.String(),
					Filename: filename,
				},
			},
		}

		if err := s.repository.SetCacheMetadata(ctx, kbUID, fileUIDs, metadata, ai.ViewCacheTTL); err != nil {
			logger.Warn("Failed to store uncached metadata in Redis",
				zap.Error(err),
				zap.String("fileUID", fileUID.String()))
		}

		return &FileCacheResult{
			CacheName:            "",
			Model:                "",
			CreateTime:           now,
			ExpireTime:           expireTime,
			CachedContextEnabled: false,
			FileContents:         metadata.FileContents,
		}, nil
	}

	// Successfully created Gemini cache, store metadata in Redis
	logger.Info("Successfully created Gemini cache",
		zap.String("cacheName", cacheResult.CacheName),
		zap.String("fileUID", fileUID.String()))

	metadata := &repository.CacheMetadata{
		CacheName:            cacheResult.CacheName,
		Model:                cacheResult.Model,
		FileUIDs:             fileUIDs,
		FileCount:            1,
		CreateTime:           cacheResult.CreateTime,
		ExpireTime:           cacheResult.ExpireTime,
		CachedContextEnabled: true,
		FileContents:         nil, // No file contents in cached mode
	}

	if err := s.repository.SetCacheMetadata(ctx, kbUID, fileUIDs, metadata, ai.ViewCacheTTL); err != nil {
		logger.Warn("Failed to store cache metadata in Redis",
			zap.Error(err),
			zap.String("cacheName", cacheResult.CacheName))
	}

	return &FileCacheResult{
		CacheName:            cacheResult.CacheName,
		Model:                cacheResult.Model,
		CreateTime:           cacheResult.CreateTime,
		ExpireTime:           cacheResult.ExpireTime,
		CachedContextEnabled: true,
		FileContents:         nil,
	}, nil
}

// RenewFileCache renews the TTL for both Gemini cache and Redis cache metadata
func (s *service) RenewFileCache(
	ctx context.Context,
	kbUID types.KBUIDType,
	fileUID types.FileUIDType,
	cacheName string,
) (*FileCacheResult, error) {
	logger, _ := logx.GetZapLogger(ctx)

	fileUIDs := []types.FileUIDType{fileUID}

	// If no cache name, this is uncached mode - just renew Redis TTL
	if cacheName == "" {
		newExpireTime := time.Now().Add(ai.ViewCacheTTL)
		if err := s.repository.RenewCacheMetadataTTL(ctx, kbUID, fileUIDs, ai.ViewCacheTTL, newExpireTime); err != nil {
			logger.Warn("Failed to renew uncached metadata TTL in Redis",
				zap.Error(err),
				zap.String("fileUID", fileUID.String()))
		}

		// Get updated metadata to return
		metadata, _ := s.repository.GetCacheMetadata(ctx, kbUID, fileUIDs)
		if metadata != nil {
			return &FileCacheResult{
				CacheName:            "",
				Model:                metadata.Model,
				CreateTime:           metadata.CreateTime,
				ExpireTime:           metadata.ExpireTime,
				CachedContextEnabled: false,
				FileContents:         metadata.FileContents,
			}, nil
		}
		return nil, fmt.Errorf("failed to retrieve renewed cache metadata")
	}

	// Cached mode - renew both Gemini cache and Redis metadata
	if s.aiClient == nil {
		logger.Warn("AI client not available for cache renewal",
			zap.String("cacheName", cacheName))
		return nil, errorsx.AddMessage(
			fmt.Errorf("AI client not available"),
			"Cache service is not available. Please contact your administrator.",
		)
	}

	// Renew Gemini cache TTL
	logger.Info("Renewing Gemini cache TTL",
		zap.String("cacheName", cacheName),
		zap.Duration("ttl", ai.ViewCacheTTL))

	ttl := ai.ViewCacheTTL
	updatedCache, err := s.aiClient.UpdateCache(ctx, cacheName, &ai.CacheUpdateOptions{
		TTL: &ttl,
	})
	if err != nil {
		logger.Warn("Failed to renew Gemini cache TTL",
			zap.Error(err),
			zap.String("cacheName", cacheName))
		// Continue to renew Redis even if Gemini renewal fails
	}

	// Renew Redis metadata TTL
	newExpireTime := time.Now().Add(ai.ViewCacheTTL)
	if updatedCache != nil {
		newExpireTime = updatedCache.ExpireTime
	}

	if err := s.repository.RenewCacheMetadataTTL(ctx, kbUID, fileUIDs, ai.ViewCacheTTL, newExpireTime); err != nil {
		logger.Warn("Failed to renew cache metadata TTL in Redis",
			zap.Error(err),
			zap.String("cacheName", cacheName))
	}

	// Get updated metadata to return
	metadata, _ := s.repository.GetCacheMetadata(ctx, kbUID, fileUIDs)
	if metadata != nil {
		return &FileCacheResult{
			CacheName:            metadata.CacheName,
			Model:                metadata.Model,
			CreateTime:           metadata.CreateTime,
			ExpireTime:           metadata.ExpireTime,
			CachedContextEnabled: true,
			FileContents:         nil,
		}, nil
	}

	// Fallback to UpdateCache result if Redis metadata is not available
	if updatedCache != nil {
		return &FileCacheResult{
			CacheName:            updatedCache.CacheName,
			Model:                updatedCache.Model,
			CreateTime:           updatedCache.CreateTime,
			ExpireTime:           updatedCache.ExpireTime,
			CachedContextEnabled: true,
			FileContents:         nil,
		}, nil
	}

	return nil, fmt.Errorf("failed to renew cache")
}
