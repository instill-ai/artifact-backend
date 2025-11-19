package vertexai

import (
	"context"
	"encoding/base64"
	"fmt"
	"path"
	"time"

	"github.com/gofrs/uuid"
	"google.golang.org/genai"

	"github.com/instill-ai/artifact-backend/pkg/ai"

	errorsx "github.com/instill-ai/x/errors"
	filetype "github.com/instill-ai/x/file"
)

// CreateCache implements ai.Client
// Creates a cached context for efficient content understanding using GCS and VertexAI
func (c *Client) CreateCache(ctx context.Context, files []ai.FileContent, ttl time.Duration) (*ai.CacheResult, error) {
	if len(files) == 0 {
		return nil, errorsx.AddMessage(errorsx.ErrInvalidArgument, "No files provided for caching")
	}

	// Use unified batch cache approach for all cases
	displayName := fmt.Sprintf("cache-%d-files", len(files))
	if len(files) == 1 {
		displayName = fmt.Sprintf("cache-%s", files[0].Filename)
	}

	output, err := c.createBatchCache(ctx, GetModel(), files, ttl, displayName)
	if err != nil {
		return nil, errorsx.AddMessage(err, "Unable to create VertexAI cache. The file will be processed without optimization.")
	}

	return convertToCacheResult(output), nil
}

// ListCaches implements ai.Client
func (c *Client) ListCaches(ctx context.Context, options *ai.CacheListOptions) (*ai.CacheListResult, error) {
	config := &genai.ListCachedContentsConfig{}

	if options != nil {
		if options.PageSize > 0 {
			config.PageSize = options.PageSize
		}
		if options.PageToken != "" {
			config.PageToken = options.PageToken
		}
	}

	page, err := c.client.Caches.List(ctx, config)
	if err != nil {
		return nil, errorsx.AddMessage(
			fmt.Errorf("failed to list caches: %w", err),
			"Unable to list cached contents. Please check your permissions and try again.",
		)
	}

	// Convert results to ai.CacheResult format
	caches := make([]ai.CacheResult, 0, len(page.Items))
	for _, item := range page.Items {
		caches = append(caches, ai.CacheResult{
			CacheName:     item.Name,
			Model:         item.Model,
			CreateTime:    item.CreateTime,
			ExpireTime:    item.ExpireTime,
			UsageMetadata: item.UsageMetadata,
		})
	}

	return &ai.CacheListResult{
		Caches:        caches,
		NextPageToken: page.NextPageToken,
	}, nil
}

// GetCache implements ai.Client
func (c *Client) GetCache(ctx context.Context, cacheName string) (*ai.CacheResult, error) {
	if cacheName == "" {
		return nil, errorsx.AddMessage(errorsx.ErrInvalidArgument, "Cache name is required")
	}

	cached, err := c.client.Caches.Get(ctx, cacheName, &genai.GetCachedContentConfig{})
	if err != nil {
		return nil, errorsx.AddMessage(
			fmt.Errorf("failed to get cache: %w", err),
			"Unable to retrieve cached content. The cache may have expired or been deleted.",
		)
	}

	return &ai.CacheResult{
		CacheName:     cached.Name,
		Model:         cached.Model,
		CreateTime:    cached.CreateTime,
		ExpireTime:    cached.ExpireTime,
		UsageMetadata: cached.UsageMetadata,
	}, nil
}

// UpdateCache implements ai.Client
func (c *Client) UpdateCache(ctx context.Context, cacheName string, options *ai.CacheUpdateOptions) (*ai.CacheResult, error) {
	if cacheName == "" {
		return nil, errorsx.AddMessage(errorsx.ErrInvalidArgument, "Cache name is required")
	}

	if options == nil || (options.TTL == nil && options.ExpireTime == nil) {
		return nil, errorsx.AddMessage(errorsx.ErrInvalidArgument, "Either TTL or ExpireTime must be specified for update")
	}

	config := &genai.UpdateCachedContentConfig{}
	if options.TTL != nil {
		config.TTL = *options.TTL
	}
	if options.ExpireTime != nil {
		config.ExpireTime = *options.ExpireTime
	}

	cached, err := c.client.Caches.Update(ctx, cacheName, config)
	if err != nil {
		return nil, errorsx.AddMessage(
			fmt.Errorf("failed to update cache: %w", err),
			"Unable to update cached content expiration. The cache may have expired or been deleted.",
		)
	}

	return &ai.CacheResult{
		CacheName:     cached.Name,
		Model:         cached.Model,
		CreateTime:    cached.CreateTime,
		ExpireTime:    cached.ExpireTime,
		UsageMetadata: cached.UsageMetadata,
	}, nil
}

// DeleteCache implements ai.Client
func (c *Client) DeleteCache(ctx context.Context, cacheName string) error {
	if cacheName == "" {
		return nil
	}

	// TODO: Also delete associated GCS files
	// We need to track GCS URIs per cache in metadata
	// For now, just delete the cache

	return c.deleteCache(ctx, cacheName)
}

// createBatchCache creates a VertexAI cache for multiple files using GCS
func (c *Client) createBatchCache(ctx context.Context, model string, files []ai.FileContent, ttl time.Duration, displayName string) (*CacheOutput, error) {
	if len(files) == 0 {
		return nil, errorsx.AddMessage(errorsx.ErrInvalidArgument, "No files provided for batch caching")
	}

	// Validate all files have content
	for _, file := range files {
		if len(file.Content) == 0 {
			return nil, errorsx.AddMessage(errorsx.ErrInvalidArgument, "The file appears to be empty. Please upload a valid file.")
		}
	}

	// Set defaults
	if model == "" {
		model = GetModel()
	}
	cacheTTL := ttl
	if cacheTTL == 0 {
		cacheTTL = GetCacheTTL()
	}

	// Prepare all file parts - upload to GCS and create FileData parts
	parts := make([]*genai.Part, 0, len(files))
	gcsURIs := make([]string, 0, len(files))

	// Upload all files to GCS first
	for _, file := range files {
		mimeType := filetype.FileTypeToMimeType(file.FileType)

		// Upload to object storage (GCS)
		// Generate unique path for file
		fileUID := uuid.Must(uuid.NewV4())
		objectPath := path.Join("vertexai-cache", fileUID.String(), file.Filename)

		// Convert to base64 for object.Storage interface
		base64Content := base64.StdEncoding.EncodeToString(file.Content)
		err := c.storage.UploadBase64File(ctx, "", objectPath, base64Content, mimeType)
		if err != nil {
			// Clean up already uploaded files on error
			for _, uri := range gcsURIs {
				_ = c.storage.DeleteFile(ctx, "", uri)
			}
			return nil, errorsx.AddMessage(
				fmt.Errorf("failed to upload file %s to object storage: %w", file.Filename, err),
				fmt.Sprintf("Unable to upload file %s for caching.", file.Filename),
			)
		}

		// Note: For VertexAI, we need the GCS bucket configured
		// The storage backend should be GCS for VertexAI to work
		// Get the bucket name from the storage configuration
		bucketName := c.storage.GetBucket()
		gsURI := fmt.Sprintf("gs://%s/%s", bucketName, objectPath)

		gcsURIs = append(gcsURIs, objectPath)

		// Create FileData part with GCS URI
		part := &genai.Part{
			FileData: &genai.FileData{
				FileURI:  gsURI,
				MIMEType: mimeType,
			},
		}
		parts = append(parts, part)
	}

	// Create cache config with GCS file references
	config := &genai.CreateCachedContentConfig{
		Contents: []*genai.Content{
			{
				Role:  genai.RoleUser,
				Parts: parts,
			},
		},
		TTL: cacheTTL,
	}

	if displayName != "" {
		config.DisplayName = displayName
	}

	// Set system instruction (reuse from CE gemini package)
	config.SystemInstruction = &genai.Content{
		Parts: []*genai.Part{
			{Text: getSystemInstruction()},
		},
	}

	// Create the cache
	cached, err := c.client.Caches.Create(ctx, model, config)
	if err != nil {
		// Clean up uploaded files on error
		for _, objPath := range gcsURIs {
			_ = c.storage.DeleteFile(ctx, "", objPath)
		}
		return nil, errorsx.AddMessage(
			fmt.Errorf("failed to create VertexAI cache: %w", err),
			"Unable to create processing cache. The files will be processed without optimization.",
		)
	}

	return &CacheOutput{
		CacheName:     cached.Name,
		Model:         cached.Model,
		CreateTime:    cached.CreateTime,
		ExpireTime:    cached.ExpireTime,
		UsageMetadata: cached.UsageMetadata,
		GCSFileURIs:   gcsURIs,
	}, nil
}

// deleteCache deletes a VertexAI cache
func (c *Client) deleteCache(ctx context.Context, cacheName string) error {
	if cacheName == "" {
		return errorsx.AddMessage(errorsx.ErrInvalidArgument, "Cache name is required")
	}

	_, err := c.client.Caches.Delete(ctx, cacheName, &genai.DeleteCachedContentConfig{})
	if err != nil {
		return errorsx.AddMessage(
			fmt.Errorf("failed to delete cache: %w", err),
			"Failed to clean up processing cache.",
		)
	}

	return nil
}

// convertToCacheResult converts CacheOutput to ai.CacheResult
func convertToCacheResult(output *CacheOutput) *ai.CacheResult {
	return &ai.CacheResult{
		CacheName:     output.CacheName,
		Model:         output.Model,
		CreateTime:    output.CreateTime,
		ExpireTime:    output.ExpireTime,
		UsageMetadata: output.UsageMetadata,
	}
}

// getSystemInstruction returns the system instruction for VertexAI
func getSystemInstruction() string {
	return `You are a helpful AI assistant that extracts and understands content from various file formats including documents, images, audio, and video. Your role is to:

1. Accurately extract all text content while preserving structure and formatting
2. Describe visual elements in images and videos clearly
3. Transcribe audio content accurately
4. Maintain the semantic meaning and context of the original content
5. Format the output in clean, well-structured Markdown

Focus on completeness and accuracy while maintaining readability.`
}

// CacheOutput represents the result of creating a VertexAI cache
type CacheOutput struct {
	CacheName     string
	Model         string
	CreateTime    time.Time
	ExpireTime    time.Time
	UsageMetadata any
	// GCSFileURIs stores the gs:// URIs for files in this cache
	// Used for cleanup when cache is deleted
	GCSFileURIs []string
}
