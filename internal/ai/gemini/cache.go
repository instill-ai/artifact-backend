package gemini

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"google.golang.org/genai"

	"github.com/instill-ai/artifact-backend/internal/ai"

	errorsx "github.com/instill-ai/x/errors"
)

// MaxInlineSize is the 20MB threshold for File API
const MaxInlineSize = 20 * 1024 * 1024

// FileUploadTimeout is the timeout for file upload and processing
const FileUploadTimeout = 5 * time.Minute

// CreateCache implements ai.Provider
// Creates a cached context for efficient content understanding of unstructured data
// Supports both single file and batch operations (pass multiple files for batch caching)
// systemInstruction specifies the AI's behavior (e.g., RAG instruction for conversion, Chat instruction for Q&A)
func (p *Provider) CreateCache(ctx context.Context, files []ai.FileContent, ttl time.Duration, systemInstruction string) (*ai.CacheResult, error) {
	if len(files) == 0 {
		return nil, errorsx.AddMessage(errorsx.ErrInvalidArgument, "No files provided for caching")
	}

	// Use unified batch cache approach for all cases (handles single and multiple files)
	displayName := fmt.Sprintf("cache-%d-files", len(files))
	if len(files) == 1 {
		displayName = fmt.Sprintf("cache-%s", files[0].Filename)
	}

	output, err := p.createBatchCache(ctx, GetModel(), files, ttl, displayName, systemInstruction)
	if err != nil {
		return nil, errorsx.AddMessage(err, "Unable to optimize file processing. The file will be processed without optimization.")
	}

	return convertToCacheResult(output), nil
}

// ListCaches implements ai.Provider
// Lists all cached contexts with pagination support
func (p *Provider) ListCaches(ctx context.Context, options *ai.CacheListOptions) (*ai.CacheListResult, error) {
	config := &genai.ListCachedContentsConfig{}

	if options != nil {
		if options.PageSize > 0 {
			config.PageSize = options.PageSize
		}
		if options.PageToken != "" {
			config.PageToken = options.PageToken
		}
	}

	page, err := p.client.Caches.List(ctx, config)
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

// GetCache implements ai.Provider
// Retrieves details of a specific cached context
func (p *Provider) GetCache(ctx context.Context, cacheName string) (*ai.CacheResult, error) {
	if cacheName == "" {
		return nil, errorsx.AddMessage(errorsx.ErrInvalidArgument, "Cache name is required")
	}

	cached, err := p.client.Caches.Get(ctx, cacheName, &genai.GetCachedContentConfig{})
	if err != nil {
		return nil, errorsx.AddMessage(
			fmt.Errorf("failed to get cache: %w", err),
			"Unable to retrieve cached content. The cache may have expired or been deleted.",
		)
	}

	return convertToCacheResult(&CacheOutput{
		CacheName:     cached.Name,
		Model:         cached.Model,
		CreateTime:    cached.CreateTime,
		ExpireTime:    cached.ExpireTime,
		UsageMetadata: cached.UsageMetadata,
	}), nil
}

// UpdateCache implements ai.Provider
// Updates the expiration of a cached context (only expiration can be updated)
func (p *Provider) UpdateCache(ctx context.Context, cacheName string, options *ai.CacheUpdateOptions) (*ai.CacheResult, error) {
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

	cached, err := p.client.Caches.Update(ctx, cacheName, config)
	if err != nil {
		return nil, errorsx.AddMessage(
			fmt.Errorf("failed to update cache: %w", err),
			"Unable to update cached content expiration. The cache may have expired or been deleted.",
		)
	}

	return convertToCacheResult(&CacheOutput{
		CacheName:     cached.Name,
		Model:         cached.Model,
		CreateTime:    cached.CreateTime,
		ExpireTime:    cached.ExpireTime,
		UsageMetadata: cached.UsageMetadata,
	}), nil
}

// DeleteCache implements ai.Provider
// Deletes a cached context by name
func (p *Provider) DeleteCache(ctx context.Context, cacheName string) error {
	if cacheName == "" {
		return nil
	}
	return p.deleteCache(ctx, cacheName)
}

// uploadAndWaitForFile uploads a file to Gemini File API and waits for it to be active
// This is shared logic used by both caching and conversion operations
func (p *Provider) uploadAndWaitForFile(ctx context.Context, data []byte, mimeType string) (*genai.Part, string, error) {
	// Upload file
	file, err := p.client.Files.Upload(ctx, bytes.NewReader(data), &genai.UploadFileConfig{
		MIMEType: mimeType,
	})
	if err != nil {
		return nil, "", errorsx.AddMessage(
			fmt.Errorf("failed to upload file: %w", err),
			"Unable to upload file to AI service. Please try again.",
		)
	}

	fileName := file.Name

	// Wait for file to become ACTIVE with timeout
	timeoutCtx, cancel := context.WithTimeout(ctx, FileUploadTimeout)
	defer cancel()

	if err := p.waitForFileActive(timeoutCtx, fileName); err != nil {
		// Clean up the uploaded file on timeout/failure
		_, _ = p.client.Files.Delete(ctx, fileName, nil)
		return nil, "", err
	}

	return &genai.Part{
		FileData: &genai.FileData{
			FileURI:  file.URI,
			MIMEType: mimeType,
		},
	}, fileName, nil
}

// waitForFileActive waits for an uploaded file to become ACTIVE state before using it
// This is shared logic used by both caching and conversion operations
func (p *Provider) waitForFileActive(ctx context.Context, fileName string) error {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	// Check immediately first (file might already be active)
	if fileInfo, err := p.client.Files.Get(ctx, fileName, nil); err == nil {
		if fileInfo.State == genai.FileStateActive {
			return nil
		}
		if fileInfo.State == genai.FileStateFailed {
			return errorsx.AddMessage(
				fmt.Errorf("file processing failed"),
				"AI service failed to process the uploaded file. The file may be corrupted or in an unsupported format.",
			)
		}
	}

	// Wait for file to become active
	for {
		select {
		case <-ctx.Done():
			return errorsx.AddMessage(
				fmt.Errorf("timeout waiting for file to become active: %w", ctx.Err()),
				"File upload timed out. The file may be too large or the service is busy. Please try again later.",
			)
		case <-ticker.C:
			fileInfo, err := p.client.Files.Get(ctx, fileName, nil)
			if err != nil {
				return errorsx.AddMessage(
					fmt.Errorf("failed to get file status: %w", err),
					"Unable to check file processing status. Please try again.",
				)
			}

			switch fileInfo.State {
			case genai.FileStateActive:
				return nil
			case genai.FileStateFailed:
				return errorsx.AddMessage(
					fmt.Errorf("file processing failed"),
					"AI service failed to process the uploaded file. The file may be corrupted or in an unsupported format.",
				)
			case genai.FileStateProcessing:
				continue
			default:
				return errorsx.AddMessage(
					fmt.Errorf("file in unexpected state: %s", fileInfo.State),
					"File processing encountered an unexpected error. Please try again.",
				)
			}
		}
	}
}

// createBatchCache creates a cache for multiple files with specified system instruction
func (p *Provider) createBatchCache(ctx context.Context, model string, files []ai.FileContent, ttl time.Duration, displayName, systemInstruction string) (*CacheOutput, error) {
	if len(files) == 0 {
		return nil, errorsx.AddMessage(errorsx.ErrInvalidArgument, "Invalid request. No files provided for batch caching.")
	}

	// Validate all files have content
	for _, file := range files {
		if len(file.Content) == 0 {
			return nil, errorsx.AddMessage(errorsx.ErrInvalidArgument, "The file appears to be empty. Please upload a valid file.")
		}
	}

	// Set defaults if needed
	if model == "" {
		model = GetModel()
	}

	cacheTTL := ttl
	if cacheTTL == 0 {
		cacheTTL = GetCacheTTL()
	}

	// Prepare all file parts for the cache
	parts := make([]*genai.Part, 0, len(files))
	uploadedFileNames := make([]string, 0, len(files))

	// Clean up uploaded files on error or completion
	defer func() {
		for _, fileName := range uploadedFileNames {
			if fileName != "" {
				_, _ = p.client.Files.Delete(ctx, fileName, nil)
			}
		}
	}()

	// Process each file
	for _, file := range files {
		// Determine if we need to use File API
		mimeType := ai.FileTypeToMIME(file.FileType)
		useFileAPI := len(file.Content) > MaxInlineSize || isVideoType(mimeType)

		var part *genai.Part
		var err error

		if useFileAPI {
			// Upload file and wait for it to become active
			var uploadedFileName string
			part, uploadedFileName, err = p.uploadAndWaitForFile(ctx, file.Content, mimeType)
			if err != nil {
				return nil, errorsx.AddMessage(
					fmt.Errorf("failed to upload file %s for batch caching: %w", file.Filename, err),
					fmt.Sprintf("Unable to upload file %s for processing optimization.", file.Filename),
				)
			}
			uploadedFileNames = append(uploadedFileNames, uploadedFileName)
		} else {
			// Use inline data for small files
			part = &genai.Part{
				InlineData: &genai.Blob{
					MIMEType: mimeType,
					Data:     file.Content,
				},
			}
		}

		parts = append(parts, part)
	}

	// Create cache config with all files
	config := &genai.CreateCachedContentConfig{
		Contents: []*genai.Content{
			{
				Role:  genai.RoleUser,
				Parts: parts,
			},
		},
		TTL: cacheTTL,
	}

	// Set display name if provided
	if displayName != "" {
		config.DisplayName = displayName
	}

	// Set system instruction based on the instruction type
	// Default to RAG instruction if not specified
	var instruction string
	switch systemInstruction {
	case ai.SystemInstructionRAG:
		instruction = GetRAGSystemInstruction()
	case ai.SystemInstructionChat:
		instruction = GetChatSystemInstruction()
	case "":
		// Default to RAG for backward compatibility
		instruction = GetRAGSystemInstruction()
	default:
		// Allow custom system instructions for flexibility
		instruction = systemInstruction
	}

	config.SystemInstruction = &genai.Content{
		Parts: []*genai.Part{
			{Text: instruction},
		},
	}

	// Create the cache
	cached, err := p.client.Caches.Create(ctx, model, config)
	if err != nil {
		return nil, errorsx.AddMessage(
			fmt.Errorf("failed to create batch cache: %w", err),
			"Unable to create processing cache for multiple files. The files will be processed without optimization.",
		)
	}

	// Build output
	output := &CacheOutput{
		CacheName:     cached.Name,
		Model:         cached.Model,
		CreateTime:    cached.CreateTime,
		ExpireTime:    cached.ExpireTime,
		UsageMetadata: cached.UsageMetadata,
	}

	return output, nil
}

// deleteCache deletes a cached content by name
func (p *Provider) deleteCache(ctx context.Context, cacheName string) error {
	if cacheName == "" {
		err := errorsx.ErrInvalidArgument
		return errorsx.AddMessage(err, "Internal error during cache cleanup.")
	}

	_, err := p.client.Caches.Delete(ctx, cacheName, &genai.DeleteCachedContentConfig{})
	if err != nil {
		return errorsx.AddMessage(
			fmt.Errorf("failed to delete cache: %w", err),
			"Failed to clean up processing cache. This may affect future operations.",
		)
	}

	return nil
}

// convertToCacheResult converts internal CacheOutput to public ai.CacheResult
func convertToCacheResult(output *CacheOutput) *ai.CacheResult {
	return &ai.CacheResult{
		CacheName:     output.CacheName,
		Model:         output.Model,
		CreateTime:    output.CreateTime,
		ExpireTime:    output.ExpireTime,
		UsageMetadata: output.UsageMetadata,
	}
}
