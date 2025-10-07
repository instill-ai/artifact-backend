package gemini

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"google.golang.org/genai"

	errorsx "github.com/instill-ai/x/errors"
)

// MaxInlineSize is the 20MB threshold for File API
const MaxInlineSize = 20 * 1024 * 1024

// FileUploadTimeout is the timeout for file upload and processing
const FileUploadTimeout = 5 * time.Minute

// CacheInput defines the input for creating a cached context
type CacheInput struct {
	// Model to use for caching
	Model string
	// Content is the raw file content
	Content []byte
	// ContentType is the MIME type
	ContentType string
	// Filename for better identification
	Filename string
	// TTL for the cache (default: 1 hour)
	TTL *time.Duration
	// DisplayName for easier identification
	DisplayName *string
	// SystemInstruction for the cache
	SystemInstruction *string
}

// CacheOutput defines the output of cache operations
type CacheOutput struct {
	// CacheName is the identifier for the cached content
	CacheName string
	// Model used for caching
	Model string
	// CreateTime when the cache was created
	CreateTime time.Time
	// ExpireTime when the cache will expire
	ExpireTime time.Time
	// TokenCount is the number of tokens cached
	TokenCount int
}

// CreateCache creates a cached content with the document/media for efficient reuse
func (p *Provider) createCache(ctx context.Context, input *CacheInput) (*CacheOutput, error) {
	if input == nil {
		err := errorsx.ErrInvalidArgument
		return nil, errorsx.AddMessage(err, "Invalid request. Please ensure the file is properly uploaded.")
	}

	if len(input.Content) == 0 {
		err := errorsx.ErrInvalidArgument
		return nil, errorsx.AddMessage(err, "The file appears to be empty. Please upload a valid file.")
	}

	// Set defaults
	model := input.Model
	if model == "" {
		model = DefaultConversionModel
	}

	cacheTTL := DefaultCacheTTL
	if input.TTL != nil {
		cacheTTL = *input.TTL
	}

	// Determine if we need to use File API
	// Use File API for: 1) files > 20MB, or 2) videos (always need File API for caching)
	useFileAPI := len(input.Content) > MaxInlineSize || isVideoType(input.ContentType)

	var part *genai.Part
	var uploadedFileName string
	var err error

	if useFileAPI {
		// Upload file and wait for it to become active
		part, uploadedFileName, err = p.uploadAndWaitForFile(ctx, input.Content, input.ContentType)
		if err != nil {
			return nil, errorsx.AddMessage(
				fmt.Errorf("failed to upload file for caching: %w", err),
				"Unable to upload file for processing optimization. The file will be processed without optimization.",
			)
		}

		// Clean up uploaded file after cache creation
		defer func() {
			if uploadedFileName != "" {
				_, _ = p.client.Files.Delete(ctx, uploadedFileName, nil)
			}
		}()
	} else {
		// Use inline data for small files
		part = &genai.Part{
			InlineData: &genai.Blob{
				MIMEType: input.ContentType,
				Data:     input.Content,
			},
		}
	}

	// Create cache config
	config := &genai.CreateCachedContentConfig{
		Contents: []*genai.Content{
			{
				Role: genai.RoleUser,
				Parts: []*genai.Part{
					part,
				},
			},
		},
		TTL: cacheTTL,
	}

	// Set display name if provided
	if input.DisplayName != nil {
		config.DisplayName = *input.DisplayName
	}

	// Set system instruction if provided
	if input.SystemInstruction != nil {
		config.SystemInstruction = &genai.Content{
			Parts: []*genai.Part{
				{Text: *input.SystemInstruction},
			},
		}
	} else {
		// Use default system instruction for multimodal content conversion
		config.SystemInstruction = &genai.Content{
			Parts: []*genai.Part{
				{Text: DefaultSystemInstruction},
			},
		}
	}

	// Create the cache
	cached, err := p.client.Caches.Create(ctx, model, config)
	if err != nil {
		return nil, errorsx.AddMessage(
			fmt.Errorf("failed to create cache: %w", err),
			"Unable to create processing cache. The file will be processed without optimization.",
		)
	}

	// Build output
	output := &CacheOutput{
		CacheName:  cached.Name,
		Model:      cached.Model,
		CreateTime: cached.CreateTime,
		ExpireTime: cached.ExpireTime,
	}

	return output, nil
}

// uploadAndWaitForFile uploads a file to Gemini File API and waits for it to be active
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

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	// Check immediately first
	if fileInfo, err := p.client.Files.Get(ctx, fileName, nil); err == nil {
		if fileInfo.State == genai.FileStateActive {
			return &genai.Part{
				FileData: &genai.FileData{
					FileURI:  file.URI,
					MIMEType: mimeType,
				},
			}, fileName, nil
		}
		if fileInfo.State == genai.FileStateFailed {
			err := fmt.Errorf("file processing failed")
			return nil, "", errorsx.AddMessage(err, "AI service failed to process the uploaded file. The file may be corrupted or in an unsupported format.")
		}
	}

	// Wait for file to become active
	for {
		select {
		case <-timeoutCtx.Done():
			// Clean up the uploaded file on timeout
			_, _ = p.client.Files.Delete(ctx, fileName, nil)
			return nil, "", errorsx.AddMessage(
				fmt.Errorf("timeout waiting for file to become active: %w", timeoutCtx.Err()),
				"File upload timed out. The file may be too large or the service is busy. Please try again later.",
			)
		case <-ticker.C:
			fileInfo, err := p.client.Files.Get(ctx, fileName, nil)
			if err != nil {
				return nil, "", errorsx.AddMessage(
					fmt.Errorf("failed to get file status: %w", err),
					"Unable to check file processing status. Please try again.",
				)
			}

			switch fileInfo.State {
			case genai.FileStateActive:
				return &genai.Part{
					FileData: &genai.FileData{
						FileURI:  file.URI,
						MIMEType: mimeType,
					},
				}, fileName, nil
			case genai.FileStateFailed:
				// Clean up the uploaded file
				_, _ = p.client.Files.Delete(ctx, fileName, nil)
				err := fmt.Errorf("file processing failed")
				return nil, "", errorsx.AddMessage(err, "AI service failed to process the uploaded file. The file may be corrupted or in an unsupported format.")
			case genai.FileStateProcessing:
				continue
			default:
				// Clean up the uploaded file
				_, _ = p.client.Files.Delete(ctx, fileName, nil)
				return nil, "", errorsx.AddMessage(
					fmt.Errorf("file in unexpected state: %s", fileInfo.State),
					"File processing encountered an unexpected error. Please try again.",
				)
			}
		}
	}
}
