package service

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/instill-ai/artifact-backend/config"
	"github.com/instill-ai/artifact-backend/pkg/ai"
	"github.com/instill-ai/artifact-backend/pkg/ai/gemini"
	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/types"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
	errorsx "github.com/instill-ai/x/errors"
	filetype "github.com/instill-ai/x/file"
	logx "github.com/instill-ai/x/log"
)

// FileCacheResult represents the result of getting or creating a file cache
// Uses the same structure as repository.CacheMetadata for consistency
type FileCacheResult = repository.CacheMetadata

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
		return cacheMetadata, nil
	}

	// Cache doesn't exist, need to create it

	// Check if AI client is available
	if s.aiClient == nil {
		return nil, errorsx.AddMessage(
			fmt.Errorf("AI client not available"),
			"Cache service is not available. Please contact your administrator.",
		)
	}

	// Check if file type is supported by AI processing
	if !filetype.IsFileTypeSupported(fileType) {
		return nil, errorsx.AddMessage(
			fmt.Errorf("file type not supported for caching: %s", fileType.String()),
			fmt.Sprintf("File type %s is not supported for caching. Only documents, images, audio, and video files can be cached.", fileType.String()),
		)
	}

	// Check if file has enough tokens for caching
	files, err := s.repository.GetKnowledgeBaseFilesByFileUIDs(ctx, []types.FileUIDType{fileUID})
	if err != nil {
		logger.Warn("Failed to get file metadata, proceeding with cache creation",
			zap.Error(err),
			zap.String("fileUID", fileUID.String()))
	} else if len(files) > 0 {
		// Get the actual token count for the file
		fileModels := []repository.KnowledgeBaseFileModel{files[0]}
		sources, err := s.repository.GetContentByFileUIDs(ctx, fileModels)
		if err != nil {
			logger.Warn("Failed to get content sources for token count, proceeding with cache creation",
				zap.Error(err),
				zap.String("fileUID", fileUID.String()))
		} else {
			totalTokens, err := s.repository.GetFilesTotalTokens(ctx, sources)
			if err != nil {
				logger.Warn("Failed to get token count, proceeding with cache creation",
					zap.Error(err),
					zap.String("fileUID", fileUID.String()))
			} else {
				fileTokenCount := totalTokens[fileUID]
				if fileTokenCount < gemini.MinCacheTokens {
					// Get the content converted file URL instead of caching
					return s.getContentMarkdownURL(ctx, kbUID, fileUID, filename)
				}
			}
		}
	}

	// Fetch file content from MinIO
	content, err := s.repository.GetMinIOStorage().GetFile(ctx, bucketName, objectName)
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
		zap.Duration("ttl", gemini.DefaultCacheTTL))

	cacheResult, err := s.aiClient.CreateCache(ctx, []ai.FileContent{
		{
			Content:  content,
			FileType: fileType,
			FileDisplayName: filename,
		},
	}, gemini.DefaultCacheTTL)

	if err != nil {
		// Cache creation failed, use fallback mode (store content in Redis)
		logger.Warn("Gemini cache creation failed, using uncached mode",
			zap.Error(err),
			zap.String("fileUID", fileUID.String()))

		// Store in Redis as uncached mode
		now := time.Now()
		expireTime := now.Add(gemini.DefaultCacheTTL)
		metadata := &repository.CacheMetadata{
			CacheName:            "", // Empty for uncached mode
			Model:                "",
			FileUIDs:             fileUIDs,
			FileCount:            1,
			CreateTime:           now,
			ExpireTime:           expireTime,
			CachedContextEnabled: false,
		}

		if err := s.repository.SetCacheMetadata(ctx, kbUID, fileUIDs, metadata, gemini.DefaultCacheTTL); err != nil {
			logger.Warn("Failed to store uncached metadata in Redis",
				zap.Error(err),
				zap.String("fileUID", fileUID.String()))
		}

		return &repository.CacheMetadata{
			CacheName:            "",
			Model:                "",
			FileUIDs:             fileUIDs,
			FileCount:            1,
			CreateTime:           now,
			ExpireTime:           expireTime,
			CachedContextEnabled: false,
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
	}

	if err := s.repository.SetCacheMetadata(ctx, kbUID, fileUIDs, metadata, gemini.DefaultCacheTTL); err != nil {
		logger.Warn("Failed to store cache metadata in Redis",
			zap.Error(err),
			zap.String("cacheName", cacheResult.CacheName))
	}

	return &repository.CacheMetadata{
		CacheName:            cacheResult.CacheName,
		Model:                cacheResult.Model,
		FileUIDs:             fileUIDs,
		FileCount:            1,
		CreateTime:           cacheResult.CreateTime,
		ExpireTime:           cacheResult.ExpireTime,
		CachedContextEnabled: true,
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
		newExpireTime := time.Now().Add(gemini.DefaultCacheTTL)
		if err := s.repository.RenewCacheMetadataTTL(ctx, kbUID, fileUIDs, gemini.DefaultCacheTTL, newExpireTime); err != nil {
			logger.Warn("Failed to renew uncached metadata TTL in Redis",
				zap.Error(err),
				zap.String("fileUID", fileUID.String()))
		}

		// Get updated metadata to return
		metadata, _ := s.repository.GetCacheMetadata(ctx, kbUID, fileUIDs)
		if metadata != nil {
			return metadata, nil
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
		zap.Duration("ttl", gemini.DefaultCacheTTL))

	ttl := gemini.DefaultCacheTTL
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
	newExpireTime := time.Now().Add(gemini.DefaultCacheTTL)
	if updatedCache != nil {
		newExpireTime = updatedCache.ExpireTime
	}

	if err := s.repository.RenewCacheMetadataTTL(ctx, kbUID, fileUIDs, gemini.DefaultCacheTTL, newExpireTime); err != nil {
		logger.Warn("Failed to renew cache metadata TTL in Redis",
			zap.Error(err),
			zap.String("cacheName", cacheName))
	}

	// Get updated metadata to return
	metadata, _ := s.repository.GetCacheMetadata(ctx, kbUID, fileUIDs)
	if metadata != nil {
		return metadata, nil
	}

	// Fallback to UpdateCache result if Redis metadata is not available
	if updatedCache != nil {
		return &repository.CacheMetadata{
			CacheName:            updatedCache.CacheName,
			Model:                updatedCache.Model,
			FileUIDs:             fileUIDs,
			FileCount:            1,
			CreateTime:           updatedCache.CreateTime,
			ExpireTime:           updatedCache.ExpireTime,
			CachedContextEnabled: true,
		}, nil
	}

	return nil, fmt.Errorf("failed to renew cache")
}

// getContentMarkdownURL returns the MinIO presigned URL for the content markdown file
// This is used when files are too small for caching (< 1024 tokens)
func (s *service) getContentMarkdownURL(ctx context.Context, kbUID types.KBUIDType, fileUID types.FileUIDType, filename string) (*repository.CacheMetadata, error) {
	logger, _ := logx.GetZapLogger(ctx)

	logger.Info("Attempting to get content markdown URL for small file",
		zap.String("fileUID", fileUID.String()),
		zap.String("filename", filename))

	// Get the content converted file
	convertedFile, err := s.repository.GetConvertedFileByFileUIDAndType(
		ctx,
		fileUID,
		artifactpb.ConvertedFileType_CONVERTED_FILE_TYPE_CONTENT,
	)
	if err != nil || convertedFile == nil {
		logger.Info("Content converted file not available yet (file still processing)",
			zap.Error(err),
			zap.String("fileUID", fileUID.String()))
		// Return empty result if content file doesn't exist yet
		// The file is still being processed - client should retry later
		return &repository.CacheMetadata{
			CacheName:            "",
			Model:                "",
			FileUIDs:             []types.FileUIDType{fileUID},
			FileCount:            1,
			CreateTime:           time.Now(),
			ExpireTime:           time.Now().Add(gemini.DefaultCacheTTL),
			CachedContextEnabled: false,
		}, nil
	}

	// Generate presigned URL for the content markdown file
	contentFilename := fmt.Sprintf("%s-content.md", filename)
	minioURL, err := s.repository.GetMinIOStorage().GetPresignedURLForDownload(
		ctx,
		config.Config.Minio.BucketName,
		convertedFile.Destination,
		contentFilename,
		convertedFile.ContentType,
		gemini.DefaultCacheTTL, // Same TTL as cache operations
	)
	if err != nil {
		logger.Warn("Failed to generate presigned URL for content markdown",
			zap.Error(err),
			zap.String("fileUID", fileUID.String()))
		// Return empty result if URL generation fails
		return &repository.CacheMetadata{
			CacheName:            "",
			Model:                "",
			FileUIDs:             []types.FileUIDType{fileUID},
			FileCount:            1,
			CreateTime:           time.Now(),
			ExpireTime:           time.Now().Add(gemini.DefaultCacheTTL),
			CachedContextEnabled: false,
		}, nil
	}

	// Encode the URL for API gateway access
	gatewayURL, err := EncodeBlobURL(minioURL)
	if err != nil {
		logger.Warn("Failed to encode blob URL for content markdown",
			zap.Error(err),
			zap.String("fileUID", fileUID.String()))
		// Return empty result if encoding fails
		return &repository.CacheMetadata{
			CacheName:            "",
			Model:                "",
			FileUIDs:             []types.FileUIDType{fileUID},
			FileCount:            1,
			CreateTime:           time.Now(),
			ExpireTime:           time.Now().Add(gemini.DefaultCacheTTL),
			CachedContextEnabled: false,
		}, nil
	}

	logger.Info("Returning content markdown blob URL for small file",
		zap.String("fileUID", fileUID.String()),
		zap.String("url", gatewayURL))

	// Return result with the content URL as CacheName (for compatibility with handler)
	return &repository.CacheMetadata{
		CacheName:            gatewayURL, // Use URL as CacheName for handler compatibility
		Model:                "",
		FileUIDs:             []types.FileUIDType{fileUID},
		FileCount:            1,
		CreateTime:           time.Now(),
		ExpireTime:           time.Now().Add(gemini.DefaultCacheTTL),
		CachedContextEnabled: false, // Not cached, but we have content URL
	}, nil
}
